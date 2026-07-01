#!/usr/bin/env python3
"""
reset_to_pending.py — Servizio Railway on-demand per il reset massivo del DB.

Di default resta in attesa (non tocca nulla) per non bruciare integration credits
quando sono scarsi. Si attiva SOLO se la env var TRIGGER_RESET=true è impostata
sul servizio Railway. Quando attivato:
  - Legge tutte le IndustrialCompany con scan_status="completed" o "unreachable"
  - Le rimette a scan_status="pending" in batch, con rate limiting (0.3s/call)
  - Il signal_engine_v6 le riprenderà in carico automaticamente con la logica nuova
Al termine (o se TRIGGER_RESET non è true) resta vivo con un healthcheck HTTP
su $PORT per non farsi killare da Railway, senza rifare il reset a ogni restart.
"""
import os, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT = int(os.environ.get("PORT", 8080))
TRIGGER = os.environ.get("TRIGGER_RESET", "false").lower() == "true"
RATE_DELAY = float(os.environ.get("RESET_RATE_DELAY", "0.3"))
DONE_FLAG = "/tmp/reset_done.flag"

status = {"state": "idle", "reset_total": 0, "reset_done": 0, "trigger": TRIGGER}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(status, default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] healthcheck :{PORT} | TRIGGER_RESET={TRIGGER}")

def run_reset():
    if os.path.exists(DONE_FLAG):
        log.info("Reset già eseguito in precedenza (flag presente). Skip per sicurezza.")
        status["state"] = "already_done"
        return
    log.info("=== RESET MASSIVO ATTIVATO (TRIGGER_RESET=true) ===")
    status["state"] = "loading"
    targets = []
    skip = 0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=id,scan_status",
                             headers=HDRS, timeout=20).json()
        except Exception as e:
            log.warning(f"errore lettura batch: {e}"); break
        if not isinstance(b, list) or not b: break
        for x in b:
            if (x.get("scan_status") or "") in ("completed", "unreachable", "blacklisted"):
                targets.append(x["id"])
        skip += 500
        if len(b) < 500: break

    status["reset_total"] = len(targets)
    log.info(f"Aziende da resettare a pending: {len(targets)}")
    status["state"] = "resetting"
    done = 0
    for cid in targets:
        try:
            r = requests.put(f"{BASE}/{cid}", json={"scan_status": "pending"}, headers=HDRS, timeout=10)
            if r.status_code in (200, 201, 204):
                done += 1
        except Exception:
            pass
        status["reset_done"] = done
        if done % 100 == 0:
            log.info(f"  progresso: {done}/{len(targets)}")
        time.sleep(RATE_DELAY)

    log.info(f"RESET COMPLETATO: {done}/{len(targets)} aziende rimesse in coda.")
    status["state"] = "done"
    with open(DONE_FLAG, "w") as f:
        f.write(str(done))

if TRIGGER:
    threading.Thread(target=run_reset, daemon=True).start()
else:
    log.info("In attesa. Imposta TRIGGER_RESET=true su questo servizio Railway e rideploya per avviare il reset.")

while True:
    time.sleep(3600)

#!/usr/bin/env python3
"""
Industrial Mass Scanner Runner v2.0
Carica IndustrialCompany da Base44 dove scanned=0 (o NULL)
e lancia la scansione industriale in parallelo.

Worker 0 di N: prende skip=0, N*BATCH, 2N*BATCH ...
Worker 1 di N: prende skip=BATCH, BATCH+N*BATCH ...
"""
import asyncio, aiohttp, os, json, logging, time, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from industrial_scanner import scan_company, stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [IND] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN    = os.environ.get("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID       = os.environ.get("B44_APP_ID",        "6a3a284ab0b87dfa27558bb6")
B44_BASE     = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW           = {"api-key": B44_TOKEN}
WORKER_ID    = int(os.environ.get("WORKER_ID",    "0"))
TOTAL_WORKERS= int(os.environ.get("TOTAL_WORKERS","4"))
CONCURRENCY  = int(os.environ.get("CONCURRENCY",  "12"))
BATCH        = int(os.environ.get("BATCH_SIZE",   "300"))
PORT         = int(os.environ.get("PORT",          "8080"))

# ── Healthcheck HTTP ──────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({**stats,
            "worker_id": WORKER_ID, "total_workers": TOTAL_WORKERS,
            "concurrency": CONCURRENCY}).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

def _start_healthcheck():
    try:
        HTTPServer(("0.0.0.0", PORT), Health).serve_forever()
    except OSError:
        pass  # porta già occupata: ignora

threading.Thread(target=_start_healthcheck, daemon=True).start()

# ── Carica batch da Base44 ────────────────────────────────────────────────────
async def load_pending(session, skip=0):
    """Carica IndustrialCompany non scansionate (scanned IS NULL o scanned=0)."""
    # Prende tutti i record — filtra lato client per scanned==0/null
    url = (f"{B44_BASE}/IndustrialCompany"
           f"?limit={BATCH}&skip={skip}"
           f"&fields=id,name,domain,country,city,industry,employee_count,scanned,annual_revenue_eur_k,description"
           f"&sort=id")
    try:
        async with session.get(url, headers=HW,
                               timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if isinstance(d, list):
                    # Filtra non-scansionati
                    pending = [c for c in d
                               if not c.get("scanned") or c.get("scanned") == 0]
                    return d, pending   # (totale_batch, pending_da_fare)
    except Exception as e:
        log.warning(f"load_pending skip={skip}: {e}")
    return [], []

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    log.info(f"=== Industrial Scanner v2.0 | Worker {WORKER_ID}/{TOTAL_WORKERS} | Conc={CONCURRENCY} ===")
    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        cycle = 0
        while True:
            cycle += 1
            total_done_this_cycle = 0

            # Ogni worker prende una fetta diversa del database
            # Worker 0: skip 0, 4*BATCH, 8*BATCH ...
            # Worker 1: skip BATCH, 5*BATCH, ...
            skip = WORKER_ID * BATCH

            log.info(f"Ciclo {cycle} | Inizio da skip={skip}")

            while True:
                all_batch, pending = await load_pending(session, skip=skip)
                if not all_batch:
                    break

                if pending:
                    log.info(f"  skip={skip} | caricati={len(all_batch)} | pending={len(pending)}")

                    async def _run(c):
                        async with sem:
                            try:
                                await scan_company(session, c)
                            except Exception as e:
                                log.warning(f"ERR {c.get('domain','?')}: {e}")
                            await asyncio.sleep(0.3)

                    await asyncio.gather(*[_run(c) for c in pending],
                                         return_exceptions=True)
                    total_done_this_cycle += len(pending)

                if len(all_batch) < BATCH:
                    break  # ultima pagina

                skip += TOTAL_WORKERS * BATCH  # salta al prossimo blocco del worker
                await asyncio.sleep(1)

            log.info(f"Ciclo {cycle} completato. Scansionate: {total_done_this_cycle} | "
                     f"Totale sessione: {stats.get('scanned',0)} | "
                     f"Segnali: {stats.get('signals',0)} | "
                     f"Opportunità: {stats.get('opportunities',0)}")

            if total_done_this_cycle == 0:
                log.info("Nessuna azienda pending. Attendo 5min e riprovo...")
                await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())

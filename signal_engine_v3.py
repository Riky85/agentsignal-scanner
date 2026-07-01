#!/usr/bin/env python3
"""
signal_engine_v3.py — Motore segnali industriali alta qualità
Fix chiave vs v2:
  - Salva scan_status="scanned" dopo ogni scan → niente ri-scansioni
  - Keyword multilingua (IT/DE/FR/EN)
  - 4 score distinti con pesi calibrati
  - Skip intelligente basato su scan_status nel DB
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", 8080))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7,fr;q=0.6"}

stats = {"scanned": 0, "unreachable": 0, "errors": 0, "cycle": 0,
         "current": "", "queue": 0, "good_signals": 0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] Healthcheck :{PORT}")

# ── KEYWORD MULTILINGUA (EN + IT + DE + FR) ──────────────────

AUTO_KW = [
    "robot","cobot","cnc","plc","scada","automation","conveyor","agv","amr",
    "machine vision","welding robot","pick and place","servo motor","hmi",
    "automated assembly","laser cutting","lean manufacturing","kaizen",
    "robot industriale","automazione","saldatura robotizzata","controllo numerico",
    "ispezione automatica","robot collaborativo","nastro trasportatore",
    "roboter","automatisierung","schweißroboter","industrieroboter",
    "cnc-bearbeitung","förderband","automatische montage","bildverarbeitung",
    "robot industriel","automatisation","soudage robotisé","convoyeur",
    "usinage cnc","bras robotique","chaîne automatisée",
]

ROBOT_KW = [
    "manual welding","manual assembly","heavy lifting","robotic cell",
    "robot integration","repetitive task","hazardous","palletizing","palletizer",
    "automotive supplier","electronics assembly","injection molding","die casting",
    "high volume production","3 shift","24/7 production",
    "saldatura manuale","assemblaggio manuale","movimentazione manuale",
    "produzione in serie","fornitore automotive","stampaggio a iniezione",
    "manuelle schweißung","manuelle montage","schweres heben",
    "serienproduktion","automobilzulieferer","spritzguss","druckguss",
    "soudage manuel","assemblage manuel","manutention manuelle",
    "production en série","fournisseur automobile",
]

MES_KW = [
    "mes","erp","oee","iiot","industry 4.0","digital twin","traceability",
    "production monitoring","opc ua","real-time data","downtime tracking",
    "batch tracking","work order","iso 9001","iso 13485","cfr part 11",
    "industria 4.0","gestione produzione","tracciabilità","monitoraggio produzione",
    "manutenzione predittiva","gemello digitale","efficienza impianto",
    "industrie 4.0","produktionsüberwachung","rückverfolgbarkeit",
    "predictive maintenance","digitaler zwilling","anlageneffizienz",
    "industrie 4.0","suivi de production","traçabilité","jumeau numérique",
    "maintenance prédictive","efficacité industrielle",
]

INTENT_KW = [
    "new plant","capacity expansion","new factory","greenfield","hiring",
    "automation engineer","robotics engineer","modernization","retrofit",
    "digital transformation","new production line","investment","we are growing",
    "nuovo stabilimento","ampliamento produttivo","assunzioni","ingegnere automazione",
    "modernizzazione","trasformazione digitale","nuova linea","investimento",
    "in espansione","siamo in crescita",
    "neues werk","erweiterung","stellenangebote","automatisierungsingenieur",
    "modernisierung","digitale transformation","neue produktionslinie","investition",
    "nouvelle usine","expansion","recrutement","ingénieur automatisation",
    "modernisation","transformation digitale","nouvelle ligne","investissement",
]

def fetch(url, timeout=7):
    try:
        r = requests.get(url, headers=UA, timeout=timeout,
                         verify=False, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 300:
            t = re.sub(r'<script[^>]*>.*?</script>', ' ', r.text, flags=re.S)
            t = re.sub(r'<style[^>]*>.*?</style>',  ' ', t,      flags=re.S)
            t = re.sub(r'<[^>]+>', ' ', t)
            return re.sub(r'\s+', ' ', t).lower()[:12000]
    except:
        pass
    return ""

def scan(rec):
    domain  = (rec.get("domain") or "").strip()
    if not domain:
        return {"scan_status": "unreachable",
                "last_scan_date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    base_url = rec.get("website_url") or f"https://www.{domain}"
    if not base_url.startswith("http"):
        base_url = f"https://www.{domain}"

    # Pagine da scansionare
    pages = [
        base_url,
        f"{base_url}/prodotti",    f"{base_url}/products",
        f"{base_url}/solutions",   f"{base_url}/soluzioni",
        f"{base_url}/technologie", f"{base_url}/technology",
        f"{base_url}/careers",     f"{base_url}/lavora-con-noi",
        f"{base_url}/jobs",        f"{base_url}/about",
    ]

    full_text = ""
    pages_fetched = 0
    for url in pages:
        t = fetch(url)
        if t:
            full_text += " " + t
            pages_fetched += 1
        if len(full_text) > 30000:
            break

    if len(full_text.strip()) < 200:
        return {"scan_status": "unreachable",
                "last_scan_date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    # ── Calcola score ─────────────────────────────────────────
    a_hits  = [k for k in AUTO_KW   if k in full_text]
    ro_hits = [k for k in ROBOT_KW  if k in full_text]
    m_hits  = [k for k in MES_KW    if k in full_text]
    i_hits  = [k for k in INTENT_KW if k in full_text]

    auto_s   = min(100, len(a_hits)  * 12)
    robot_s  = min(100, len(ro_hits) * 18)
    mes_s    = min(100, len(m_hits)  * 14)
    intent_s = min(100, len(i_hits)  * 15)

    # ── Raccomandazione ───────────────────────────────────────
    scores = {
        "Robotics & Cobot Integration":    robot_s,
        "MES / Digital Factory":           mes_s,
        "Industrial Automation Upgrade":   auto_s,
        "Proactive Outreach – High Intent": intent_s,
    }
    top      = max(scores, key=scores.get)
    best_val = scores[top]

    ev_map = {
        "Robotics & Cobot Integration":    ro_hits[:5],
        "MES / Digital Factory":           m_hits[:5],
        "Industrial Automation Upgrade":   a_hits[:5],
        "Proactive Outreach – High Intent": i_hits[:5],
    }

    if best_val < 12:
        top      = "Low Signal – Monitor"
        solution = "Insufficient industrial signals"
    else:
        solution = "Signals: " + ", ".join(ev_map[top])

    # ── Deal estimate EUR ────────────────────────────────────
    emp = int(float(rec.get("employee_count") or 0))
    if   emp > 5000: dmin, dmax = 300000, 2000000
    elif emp > 500:  dmin, dmax = 80000,  500000
    elif emp > 100:  dmin, dmax = 25000,  120000
    elif emp > 20:   dmin, dmax = 8000,   40000
    else:            dmin, dmax = 3000,   15000
    if intent_s > 40:
        dmin = int(dmin * 1.4)
        dmax = int(dmax * 1.4)

    return {
        "automation_readiness_score":  auto_s,
        "robotics_opportunity_score":  robot_s,
        "mes_opportunity_score":       mes_s,
        "buying_intent_score":         intent_s,
        "top_opportunity":             top,
        "recommended_solution":        solution,
        "estimated_deal_value_min":    dmin,
        "estimated_deal_value_max":    dmax,
        "estimated_deal_min":          dmin,
        "estimated_deal_max":          dmax,
        # FIX CHIAVE: salva scan_status e data
        "scan_status":                 "scanned",
        "last_scan_date":              time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ats_documentation":           f"pages={pages_fetched} | "
                                       f"auto={auto_s} robot={robot_s} "
                                       f"mes={mes_s} intent={intent_s}",
    }

def load_pending(limit=100):
    """Carica aziende NON ancora scansionate dal DB."""
    results = []
    skip = 0
    while len(results) < limit:
        try:
            b = requests.get(
                f"{BASE}?limit=200&skip={skip}"
                f"&fields=id,name,domain,website_url,employee_count,scan_status,country",
                headers=HDRS, timeout=20).json()
        except Exception as e:
            log.warning(f"load_pending error: {e}")
            break
        if not isinstance(b, list) or not b:
            break
        for r in b:
            if r.get("scan_status") not in ("scanned", "unreachable"):
                results.append(r)
                if len(results) >= limit:
                    break
        if len(b) < 200:
            break
        skip += 200
    return results

# ── PRIORITY ORDER: IT → DE → FR → ES → US → resto ──────────
PRIORITY = {
    "ITA","IT","ITALY",
    "DEU","DE","DD","GERMANY",
    "FRA","FR","FRANCE",
    "ESP","ES","SPAIN",
    "CHE","CH","SWITZERLAND",
    "AUT","AT","AUSTRIA",
    "NLD","NL","NETHERLANDS",
    "BEL","BE","BELGIUM",
    "USA","US","UNITED STATES",
    "GBR","GB","UK",
    "JPN","JP","JAPAN",
}

# ── MAIN LOOP ─────────────────────────────────────────────────
log.info("=== SIGNAL ENGINE V3 START ===")
log.info("Fix: scan_status='scanned' salvato dopo ogni scan")
log.info("Feature: keyword IT+DE+FR+EN, 4 score distinti")

while True:
    stats["cycle"] += 1
    log.info(f"\n{'='*50}")
    log.info(f"CICLO {stats['cycle']} — carico batch da scansionare...")

    batch = load_pending(limit=150)
    stats["queue"] = len(batch)

    if not batch:
        log.info("DB completamente scansionato. Pausa 2h poi ri-ciclo.")
        time.sleep(7200)
        continue

    # Ordina: paesi prioritari prima
    batch.sort(key=lambda x: (0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
    log.info(f"Batch: {len(batch)} aziende (priorità IT/DE/FR/US in testa)")

    for rec in batch:
        name   = (rec.get("name") or rec.get("domain") or "?")[:40]
        domain = rec.get("domain", "?")
        stats["current"] = domain

        try:
            result = scan(rec)

            r = requests.put(
                f"{BASE}/{rec['id']}",
                json=result,
                headers=HDRS,
                timeout=15)

            if r.status_code in (200, 201, 204):
                if result["scan_status"] == "scanned":
                    stats["scanned"] += 1
                    best = max(
                        result["automation_readiness_score"],
                        result["robotics_opportunity_score"],
                        result["mes_opportunity_score"],
                        result["buying_intent_score"])
                    if best >= 20:
                        stats["good_signals"] += 1
                    log.info(
                        f"  ✅ {name:40} | "
                        f"auto={result['automation_readiness_score']:3} "
                        f"robot={result['robotics_opportunity_score']:3} "
                        f"mes={result['mes_opportunity_score']:3} "
                        f"intent={result['buying_intent_score']:3} "
                        f"→ {result['top_opportunity'][:35]}")
                else:
                    stats["unreachable"] += 1
                    log.info(f"  ⚠️  {name}: unreachable")
            else:
                stats["errors"] += 1
                log.warning(f"  ❌ {domain}: HTTP {r.status_code} {r.text[:60]}")

        except Exception as e:
            stats["errors"] += 1
            log.warning(f"  ❌ {domain}: {e}")

        time.sleep(1.0)

    log.info(
        f"Ciclo {stats['cycle']} DONE — "
        f"scanned={stats['scanned']} unreachable={stats['unreachable']} "
        f"good_signals={stats['good_signals']} errors={stats['errors']}")

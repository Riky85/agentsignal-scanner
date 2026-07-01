"""
scanner_with_scoring.py — Scanner + Scoring integrato
Combina il feeder v3 (Wikidata/EDGAR) con il classifier del manufacturing_agents MVP.
Per ogni azienda già nel DB (senza scan): visita il sito, estrae testo, calcola scoring.
Aggiorna i campi: automation_readiness_score, robotics_opportunity_score,
                   mes_opportunity_score, buying_intent_score, top_opportunity, recommended_solution
"""

import os, re, time, requests, threading, json, asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, urljoin
import warnings; warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = "6a3a284ab0b87dfa27558bb6"
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", "8080"))
UA      = {"User-Agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
           "Accept-Encoding": "gzip, deflate",  # No Brotli
           "Accept": "text/html,*/*"}

# ── Healthcheck ─────────────────────────────────────────────────────────────
stats = {"scanned": 0, "scored": 0, "errors": 0, "current": ""}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps(stats).encode())
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()

# ── Scoring signals (dal classifier.py del MVP) ──────────────────────────────
AUTOMATION_SIGNALS = {
    "robot": 20, "cobot": 25, "collaborative robot": 25,
    "automazione": 18, "automation": 18, "cnc": 14, "plc": 14,
    "linea produttiva": 12, "production line": 12, "packaging": 10,
    "quality control": 8, "machine vision": 18, "vision system": 18,
    "lean manufacturing": 8, "agv": 20, "amr": 20, "conveyor": 10,
    "servo": 12, "welding robot": 22, "pick and place": 20,
}

SOFTWARE_SIGNALS = {
    "erp": 12, "mes": 18, "wms": 14, "crm": 8, "excel": 18,
    "manual process": 20, "digital transformation": 14,
    "industry 4.0": 16, "industria 4.0": 16,
    "traceability": 14, "tracciabilità": 14,
    "maintenance": 10, "manutenzione": 10, "opc ua": 18,
    "scada": 16, "iiot": 18, "iot": 12,
}

ROBOT_SIGNALS = {
    "welding": 18, "saldatura": 18, "palletizing": 18,
    "pallettizzazione": 18, "pick and place": 20,
    "assembly": 12, "assemblaggio": 12, "machining": 12,
    "metalworking": 16, "carpenteria": 16, "plastics": 14,
    "stampaggio": 14, "food processing": 12, "painting": 14,
    "verniciatura": 14, "grinding": 12, "deburring": 16,
}

BUYING_SIGNALS = {
    "hiring": 15, "we are expanding": 20, "nuova sede": 18,
    "new facility": 20, "investimento": 15, "investment": 15,
    "digital transformation": 18, "industria 4.0": 16,
    "siamo in crescita": 20, "growing": 12, "job opening": 14,
    "career": 8, "open position": 14, "automazione avanzata": 20,
    "robotics integration": 22, "rfp": 20, "tender": 18,
}

KEY_PAGE_HINTS = [
    "about", "azienda", "company", "chi-siamo", "who-we-are",
    "products", "prodotti", "services", "servizi",
    "automation", "automazione", "robot", "cnc", "machining",
    "production", "produzione", "manufacturing",
    "career", "careers", "lavora-con-noi", "jobs",
    "technology", "tecnologia", "quality",
]

def score_text(text: str) -> dict:
    t = text.lower()
    def calc(signals):
        s, hits = 0, []
        for kw, w in signals.items():
            if kw in t:
                s += w; hits.append(kw)
        return min(s, 100), hits

    auto_s,  auto_h  = calc(AUTOMATION_SIGNALS)
    soft_s,  soft_h  = calc(SOFTWARE_SIGNALS)
    robot_s, robot_h = calc(ROBOT_SIGNALS)
    buy_s,   buy_h   = calc(BUYING_SIGNALS)

    all_hits = list(set(auto_h + soft_h + robot_h + buy_h))

    # Top opportunity
    if robot_s >= auto_s and robot_s >= soft_s and robot_s >= 20:
        top_opp = "Robotics & Automation"
        solution = "Audit robotica/cobot + studio ROI per automazione operazioni ripetitive."
    elif soft_s >= auto_s and soft_s >= robot_s and soft_s >= 20:
        top_opp = "MES/ERP Digital Transformation"
        solution = "Assessment MES/ERP/WMS + digitalizzazione processi e tracciabilità."
    elif auto_s >= 20:
        top_opp = "Industrial Automation"
        solution = "Assessment automazione industriale + integrazione dati macchina/PLC/OPC UA."
    else:
        top_opp = "Lead Nurturing"
        solution = "Contenuti su automazione, efficienza produttiva e digitalizzazione."

    return {
        "automation_readiness_score": auto_s,
        "robotics_opportunity_score": robot_s,
        "mes_opportunity_score":      soft_s,
        "buying_intent_score":        buy_s,
        "top_opportunity":            top_opp,
        "recommended_solution":       solution,
        "signals":                    all_hits[:20],
    }

# ── Site Crawler ─────────────────────────────────────────────────────────────
def crawl_site(domain: str, max_pages: int = 8) -> str:
    """Crawl fino a max_pages pagine chiave, ritorna testo concatenato."""
    base_url = f"https://www.{domain}"
    seen = set()
    texts = []
    queue = [base_url]

    while queue and len(texts) < max_pages:
        url = queue.pop(0)
        if url in seen: continue
        seen.add(url)

        try:
            r = requests.get(url, timeout=9, headers=UA, allow_redirects=True, verify=False)
            if r.status_code >= 400: continue

            raw = r.text
            # Estrai testo visibile rimuovendo tag
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.S)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.S)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 100:
                texts.append(text[:5000])

            # Estrai link prioritari
            if len(texts) < max_pages:
                links = re.findall(r'href=["\']([^"\'#?\s]+)["\']', raw)
                for link in links:
                    if link.startswith('/') and not link.startswith('//'):
                        full = base_url + link
                    elif link.startswith('http') and domain in link:
                        full = link
                    else: continue
                    if full not in seen and any(h in full.lower() for h in KEY_PAGE_HINTS):
                        queue.append(full)

            time.sleep(0.5)
        except: pass

    return " ".join(texts)

# ── DB Operations ─────────────────────────────────────────────────────────────
def load_pending_batch(skip: int = 0, limit: int = 50) -> list:
    """Aziende non ancora scansionate (scan_status != 'done')."""
    try:
        b = requests.get(
            f"{BASE}?limit={limit}&skip={skip}&fields=id,name,domain,scan_status",
            headers=HDRS, timeout=20
        ).json()
        if not isinstance(b, list): return []
        return [r for r in b if r.get("scan_status") != "done" and r.get("domain")]
    except: return []

def save_scores(rec_id: str, scores: dict, current: dict):
    """Aggiorna via PUT mergiando con il record esistente."""
    try:
        payload = {**current, **scores, "scan_status": "done"}
        # Pulisci metadata
        for k in ("id","created_date","updated_date","created_by","signals"):
            payload.pop(k, None)
        # Assicura name
        if not payload.get("name"): return False
        r = requests.put(f"{BASE}/{rec_id}", json=payload, headers=HDRS, timeout=12)
        if r.status_code == 429: time.sleep(25); return False
        return r.status_code == 200
    except: return False

def get_full_record(rec_id: str) -> dict:
    try:
        r = requests.get(f"{BASE}/{rec_id}", headers=HDRS, timeout=10)
        return r.json() if r.status_code == 200 else {}
    except: return {}

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("▶ Scanner with Scoring — start", flush=True)
    skip = 0

    while True:
        batch = load_pending_batch(skip=skip, limit=30)
        if not batch:
            print(f"[scanner] Tutti scansionati. scored={stats['scored']}. Pausa 2h...", flush=True)
            time.sleep(7200)
            skip = 0
            continue

        print(f"[scanner] Batch {skip}-{skip+len(batch)}: {len(batch)} aziende da scansionare", flush=True)

        for rec in batch:
            rid    = rec["id"]
            domain = rec["domain"]
            name   = rec.get("name","?")
            stats["current"] = domain

            # Crawl sito
            text = crawl_site(domain)
            stats["scanned"] += 1

            if not text or len(text) < 50:
                stats["errors"] += 1
                # Segna comunque come done per non ri-scansionare
                full = get_full_record(rid)
                if full.get("name"):
                    requests.put(f"{BASE}/{rid}", json={**{k:v for k,v in full.items() if k not in ('id','created_date','updated_date','created_by')}, "scan_status": "done"}, headers=HDRS, timeout=10)
                print(f"  ❌ {name[:35]:35s} | {domain} — nessun contenuto")
                continue

            # Scoring
            scores = score_text(text)
            full   = get_full_record(rid)
            ok     = save_scores(rid, scores, full)

            if ok:
                stats["scored"] += 1
                a = scores["automation_readiness_score"]
                r = scores["robotics_opportunity_score"]
                m = scores["mes_opportunity_score"]
                b = scores["buying_intent_score"]
                print(f"  ✅ {name[:35]:35s} | A={a:2d} R={r:2d} M={m:2d} B={b:2d} | {scores['top_opportunity']}", flush=True)
            else:
                stats["errors"] += 1
                print(f"  ❌ PUT failed: {name[:35]}", flush=True)

            time.sleep(1)

        skip += len(batch)
        print(f"[scanner] Skip={skip} | scanned={stats['scanned']} scored={stats['scored']}", flush=True)

if __name__ == "__main__":
    main()

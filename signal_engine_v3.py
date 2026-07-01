#!/usr/bin/env python3
"""
signal_engine_v3.py — Motore segnali industriali
- scan_status="scanned" salvato dopo ogni scan (fix bug v2)
- Keyword multilingua IT+DE+FR+EN
- Check qualità ogni 50 scan (non blocca la scansione)
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

stats = {
    "scanned": 0, "unreachable": 0, "errors": 0,
    "cycle": 0, "current": "", "queue": 0,
    "good_signals": 0, "last_quality_check": "never",
    "quality": {"total_scanned": 0, "with_signal": 0, "top_opps": {}}
}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] Healthcheck :{PORT}")

# ── KEYWORD MULTILINGUA ───────────────────────────────────────
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
    "production monitoring","opc ua","real-time data","downtime",
    "batch tracking","work order","iso 9001","iso 13485",
    "industria 4.0","tracciabilità","monitoraggio produzione",
    "manutenzione predittiva","gemello digitale","efficienza impianto",
    "industrie 4.0","rückverfolgbarkeit","predictive maintenance",
    "digitaler zwilling","anlageneffizienz","produktionsüberwachung",
    "industrie 4.0","traçabilité","maintenance prédictive",
    "jumeau numérique","efficacité industrielle","suivi de production",
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
        r = requests.get(url, headers=UA, timeout=timeout, verify=False, allow_redirects=True)
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

    text = ""
    pages_ok = 0
    for url in [base_url,
                f"{base_url}/prodotti",    f"{base_url}/products",
                f"{base_url}/solutions",   f"{base_url}/soluzioni",
                f"{base_url}/technologie", f"{base_url}/technology",
                f"{base_url}/careers",     f"{base_url}/lavora-con-noi",
                f"{base_url}/jobs",        f"{base_url}/about"]:
        t = fetch(url)
        if t:
            text += " " + t
            pages_ok += 1
        if len(text) > 30000:
            break

    if len(text.strip()) < 200:
        return {"scan_status": "unreachable",
                "last_scan_date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    a_hits  = [k for k in AUTO_KW   if k in text]
    ro_hits = [k for k in ROBOT_KW  if k in text]
    m_hits  = [k for k in MES_KW    if k in text]
    i_hits  = [k for k in INTENT_KW if k in text]

    auto_s   = min(100, len(a_hits)  * 12)
    robot_s  = min(100, len(ro_hits) * 18)
    mes_s    = min(100, len(m_hits)  * 14)
    intent_s = min(100, len(i_hits)  * 15)

    scores = {
        "Robotics & Cobot Integration":     robot_s,
        "MES / Digital Factory":            mes_s,
        "Industrial Automation Upgrade":    auto_s,
        "Proactive Outreach – High Intent": intent_s,
    }
    top      = max(scores, key=scores.get)
    best_val = scores[top]

    ev_map = {
        "Robotics & Cobot Integration":     ro_hits[:5],
        "MES / Digital Factory":            m_hits[:5],
        "Industrial Automation Upgrade":    a_hits[:5],
        "Proactive Outreach – High Intent": i_hits[:5],
    }

    if best_val < 12:
        top      = "Low Signal – Monitor"
        solution = "Insufficient industrial signals"
    else:
        solution = "Signals: " + ", ".join(ev_map[top])

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
        "scan_status":                 "scanned",
        "last_scan_date":              time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ats_documentation":           f"pages={pages_ok} auto={auto_s} robot={robot_s} mes={mes_s} intent={intent_s}",
    }

def quality_check():
    """Analisi qualità su tutto il DB scansionato — non blocca."""
    log.info("  ── QUALITY CHECK ──")
    try:
        skip=0; total=0; with_sig=0; opps={}; top10=[]
        while True:
            b = requests.get(
                f"{BASE}?limit=500&skip={skip}"
                f"&fields=scan_status,top_opportunity,buying_intent_score,"
                f"robotics_opportunity_score,automation_readiness_score,mes_opportunity_score,name",
                headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            for x in b:
                if x.get("scan_status") == "scanned":
                    total += 1
                    opp = x.get("top_opportunity") or ""
                    bi  = int(x.get("buying_intent_score") or 0)
                    if opp and opp != "Low Signal – Monitor":
                        with_sig += 1
                        opps[opp] = opps.get(opp, 0) + 1
                    if bi >= 30:
                        top10.append((bi, x.get("name","?")))
            skip += 500
            if len(b) < 500: break

        top10.sort(reverse=True)
        signal_rate = round(with_sig / total * 100, 1) if total else 0

        log.info(f"  Scansionati totali: {total}")
        log.info(f"  Con segnale utile:  {with_sig} ({signal_rate}%)")
        log.info(f"  Distribuzione opportunità:")
        for k,v in sorted(opps.items(), key=lambda x:-x[1]):
            log.info(f"    {v:4}  {k}")
        if top10:
            log.info(f"  Top aziende per buying intent:")
            for score, name in top10[:5]:
                log.info(f"    intent={score}  {name}")

        stats["quality"] = {
            "total_scanned": total,
            "with_signal": with_sig,
            "signal_rate_pct": signal_rate,
            "top_opps": opps,
        }
        stats["last_quality_check"] = time.strftime("%H:%M:%S")
        log.info("  ── END QUALITY CHECK ──")
    except Exception as e:
        log.warning(f"  quality_check error: {e}")

def load_pending(limit=120):
    """Carica aziende non ancora scansionate."""
    results = []
    skip = 0
    while len(results) < limit:
        try:
            b = requests.get(
                f"{BASE}?limit=200&skip={skip}"
                f"&fields=id,name,domain,website_url,employee_count,scan_status,country",
                headers=HDRS, timeout=20).json()
        except Exception as e:
            log.warning(f"load_pending: {e}"); break
        if not isinstance(b, list) or not b: break
        for r in b:
            if r.get("scan_status") not in ("scanned", "unreachable"):
                results.append(r)
                if len(results) >= limit: break
        if len(b) < 200: break
        skip += 200
    return results

PRIORITY = {
    "ITA","IT","ITALY","DEU","DE","DD","GERMANY","FRA","FR","FRANCE",
    "ESP","ES","SPAIN","CHE","CH","SWITZERLAND","AUT","AT","AUSTRIA",
    "NLD","NL","NETHERLANDS","BEL","BE","BELGIUM","POL","PL","POLAND",
    "USA","US","UNITED STATES","GBR","GB","UK","JPN","JP","JAPAN",
}

# ── MAIN LOOP ─────────────────────────────────────────────────
log.info("=== SIGNAL ENGINE V3 START ===")
log.info("Quality check ogni 50 scan, scansione continua")

scan_count_since_check = 0

while True:
    stats["cycle"] += 1
    batch = load_pending(limit=120)
    stats["queue"] = len(batch)

    if not batch:
        log.info("DB completamente scansionato. Quality check finale + pausa 2h.")
        quality_check()
        time.sleep(7200)
        continue

    batch.sort(key=lambda x: (0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
    log.info(f"[C{stats['cycle']}] Batch {len(batch)} (priorità IT/DE/FR/US)")

    for rec in batch:
        name   = (rec.get("name") or rec.get("domain") or "?")[:38]
        domain = rec.get("domain", "?")
        stats["current"] = domain

        try:
            result = scan(rec)
            r = requests.put(f"{BASE}/{rec['id']}", json=result, headers=HDRS, timeout=15)

            if r.status_code in (200, 201, 204):
                if result["scan_status"] == "scanned":
                    stats["scanned"] += 1
                    scan_count_since_check += 1
                    best = max(result["automation_readiness_score"],
                               result["robotics_opportunity_score"],
                               result["mes_opportunity_score"],
                               result["buying_intent_score"])
                    if best >= 20:
                        stats["good_signals"] += 1
                    log.info(
                        f"  ✅ {name:40} "
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
                log.warning(f"  ❌ {domain}: HTTP {r.status_code}")

        except Exception as e:
            stats["errors"] += 1
            log.warning(f"  ❌ {domain}: {e}")

        # ── Quality check ogni 50 scan, NON blocca ───────────
        if scan_count_since_check >= 50:
            scan_count_since_check = 0
            quality_check()  # eseguito inline, ~5 secondi, poi si riprende

        time.sleep(1.0)

    log.info(
        f"[C{stats['cycle']}] DONE — "
        f"scanned={stats['scanned']} unreachable={stats['unreachable']} "
        f"good={stats['good_signals']} errors={stats['errors']}")

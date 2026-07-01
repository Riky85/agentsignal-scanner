#!/usr/bin/env python3
"""
signal_engine_v3.py — Motore segnali industriali alta qualità
Scansiona ogni azienda una sola volta (scan_status="scanned").
Calcola 4 score distinti con segnali reali dal sito:
  - automation_readiness_score (0-100)
  - robotics_opportunity_score (0-100)  
  - mes_opportunity_score (0-100)
  - buying_intent_score (0-100)
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", 8080))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Encoding": "gzip,deflate",
      "Accept-Language": "en-US,en;q=0.9,it;q=0.8"}

stats = {"scanned": 0, "errors": 0, "cycle": 0, "current": "", "queue": 0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),H).serve_forever(), daemon=True).start()
log.info(f"[OK] Healthcheck :{PORT}")

# ============================================================
# DIZIONARIO SEGNALI — 4 categorie con pesi specifici
# ============================================================

# AUTOMATION — indica maturità operativa attuale
AUTO_SIGNALS = {
    # Robotica fisica
    "robot": 25, "cobot": 30, "collaborative robot": 30,
    "agv": 28, "amr": 28, "autonomous mobile": 28,
    "pick and place": 22, "palletizer": 22, "palletizing": 22,
    "machine vision": 25, "vision system": 22, "image processing": 18,
    # CNC e lavorazioni
    "cnc": 20, "machining center": 22, "turning center": 20,
    "5-axis": 25, "5 axis": 25, "grinding": 15, "milling": 15,
    # Automazione di processo
    "plc": 18, "servo": 15, "hmi": 15, "scada": 22,
    "conveyor": 15, "automated assembly": 25, "automated welding": 25,
    "welding robot": 28, "laser cutting": 20, "laser welding": 20,
    # Qualità automatizzata
    "coordinate measuring": 22, "cmm": 22, "inline inspection": 20,
    "automated testing": 18, "automated inspection": 20,
    # Lean/Kaizen (indica cultura operativa)
    "lean manufacturing": 12, "kaizen": 10, "six sigma": 12,
    "just in time": 10, "kanban": 10,
}

# ROBOTICS — opportunità specifica di vendita robot/cobot
ROBOT_SIGNALS = {
    # Processi manuali ad alto rischio → target robot
    "manual welding": 35, "manual assembly": 30, "manual handling": 28,
    "repetitive task": 25, "ergonomic": 20, "heavy lifting": 25,
    "hazardous environment": 30, "cleanroom": 22,
    # Settori con alta adozione robotica
    "automotive": 25, "electronics assembly": 28, "semiconductor": 30,
    "pharma": 22, "food processing": 20, "packaging line": 25,
    "injection molding": 20, "die casting": 22,
    # Segnali espliciti
    "robot integration": 35, "robotic": 30, "automation partner": 25,
    "system integrator": 20, "robot cell": 35, "robotic cell": 35,
    # Capacità produttiva elevata
    "high volume": 18, "mass production": 20, "continuous production": 15,
    "3 shift": 20, "24/7 production": 25,
}

# MES — opportunità sistemi di gestione produzione
MES_SIGNALS = {
    # Gap digitale esplicito (hanno bisogno)
    "excel": 30, "paper": 25, "manual record": 28, "spreadsheet": 25,
    "traceability": 22, "batch tracking": 20, "work order": 18,
    # Sistemi già presenti (maturità digitale)
    "erp": 15, "sap": 18, "mes": 20, "manufacturing execution": 25,
    "production planning": 18, "scheduling": 15, "oee": 22,
    # IoT e connettività (pronti per MES)
    "iiot": 25, "industry 4.0": 20, "industria 4.0": 20,
    "opc ua": 25, "opcua": 25, "mqtt": 20, "digital twin": 22,
    "real-time monitoring": 20, "production monitoring": 22,
    "downtime": 18, "overall equipment": 20,
    # Qualità e compliance
    "iso 9001": 15, "iso 13485": 18, "fda": 20, "cfr part 11": 22,
    "gmp": 20, "serialization": 22, "track and trace": 22,
}

# BUYING INTENT — segnali che indicano prossimo acquisto
INTENT_SIGNALS = {
    # Espansione e investimento
    "new plant": 35, "greenfield": 35, "brownfield": 30,
    "capacity expansion": 30, "new factory": 35, "new facility": 30,
    "investment": 18, "capital investment": 25, "capex": 22,
    # Hiring tecnico (segnale forte)
    "hiring automation": 35, "hiring engineer": 25, "we are hiring": 15,
    "open position": 10, "job opening": 10, "career": 5,
    "automation engineer": 30, "robotics engineer": 30,
    "manufacturing engineer": 20, "process engineer": 18,
    # Segnali di cambiamento tecnologico
    "modernization": 25, "upgrade": 18, "retrofit": 25,
    "technology upgrade": 28, "digital transformation": 22,
    "new technology": 20, "innovation": 12,
    # Partnership e certificazioni recenti
    "certified partner": 15, "technology partner": 15,
    "recently awarded": 20, "new contract": 18,
    # Sostenibilità (budget disponibile)
    "sustainability": 12, "carbon neutral": 15, "net zero": 15,
    "energy efficiency": 15, "green manufacturing": 15,
}

def fetch_page(url, timeout=8):
    """Fetch una pagina e restituisce testo pulito (lowercase)"""
    try:
        r = requests.get(url, headers=UA, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            # Estrai testo visibile (rimuovi tag HTML)
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', r.text, flags=re.S)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.S)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text).lower()
            return text[:12000]
    except: pass
    return ""

def scan_company(rec):
    """Scansiona un'azienda e calcola i 4 score"""
    domain = rec.get("domain","")
    if not domain: return None
    
    website = rec.get("website_url") or f"https://www.{domain}"
    if not website.startswith("http"):
        website = f"https://www.{domain}"
    
    # Pagine da scansionare (priorità ordinate)
    pages = [
        website,                          # Homepage
        f"{website}/products",            # Prodotti
        f"{website}/solutions",           # Soluzioni
        f"{website}/technology",          # Tecnologia
        f"{website}/manufacturing",       # Produzione
        f"{website}/about",              # About (dimensioni, storia)
        f"{website}/careers",             # Hiring (buying intent)
        f"{website}/automation",          # Automazione esplicita
    ]
    
    full_text = ""
    pages_ok = 0
    evidence = []  # Testo estratto come prova
    
    for page_url in pages[:6]:
        t = fetch_page(page_url, timeout=7)
        if t and len(t) > 200:
            full_text += " " + t
            pages_ok += 1
        if len(full_text) > 25000: break
    
    if not full_text.strip() or len(full_text) < 300:
        return {"scan_status": "unreachable"}
    
    # ── Calcola i 4 score ──────────────────────────────────────
    auto_pts  = 0; robot_pts = 0; mes_pts = 0; intent_pts = 0
    auto_ev   = []; robot_ev  = []; mes_ev    = []; intent_ev = []
    
    for kw, pts in AUTO_SIGNALS.items():
        if kw in full_text:
            auto_pts += pts; auto_ev.append(kw)
    
    for kw, pts in ROBOT_SIGNALS.items():
        if kw in full_text:
            robot_pts += pts; robot_ev.append(kw)
    
    for kw, pts in MES_SIGNALS.items():
        if kw in full_text:
            mes_pts += pts; mes_ev.append(kw)
    
    for kw, pts in INTENT_SIGNALS.items():
        if kw in full_text:
            intent_pts += pts; intent_ev.append(kw)
    
    # Normalizza 0-100 (cap con curva logaritmica per evitare saturazione)
    def normalize(pts, cap=200):
        return min(100, int(pts / cap * 100))
    
    auto_score   = normalize(auto_pts,  cap=180)
    robot_score  = normalize(robot_pts, cap=200)
    mes_score    = normalize(mes_pts,   cap=160)
    intent_score = normalize(intent_pts, cap=220)
    
    # ── Raccomandazione principale ─────────────────────────────
    scores = {
        "robot":  robot_score,
        "mes":    mes_score,
        "auto":   auto_score,
        "intent": intent_score,
    }
    top = max(scores, key=scores.get)
    score_val = scores[top]
    
    emp = int(float(rec.get("employee_count") or 0))
    
    if score_val < 15:
        recommendation = "Low Signal"
        solution       = "Insufficient data for qualification"
    elif top == "robot" or robot_score > 40:
        recommendation = "Robotics & Cobot Integration"
        solution       = f"Manual/repetitive processes detected. Signals: {', '.join(robot_ev[:4])}"
    elif top == "mes" or mes_score > 35:
        recommendation = "MES / Digital Factory"
        solution       = f"Process digitization gap. Signals: {', '.join(mes_ev[:4])}"
    elif top == "auto" or auto_score > 40:
        recommendation = "Industrial Automation Upgrade"
        solution       = f"Automation present, upsell opportunity. Signals: {', '.join(auto_ev[:4])}"
    elif intent_score > 30:
        recommendation = "Proactive Outreach – High Intent"
        solution       = f"Strong buying signals. Signals: {', '.join(intent_ev[:4])}"
    else:
        recommendation = "General Industrial Prospect"
        solution       = "Moderate signals, monitor for changes"
    
    # ── Deal value estimate (EUR) ──────────────────────────────
    if emp > 5000:   dmin, dmax = 300000, 2000000
    elif emp > 500:  dmin, dmax = 80000,  500000
    elif emp > 100:  dmin, dmax = 25000,  120000
    elif emp > 20:   dmin, dmax = 8000,   40000
    else:            dmin, dmax = 3000,   15000
    
    # Boost se alto intent
    if intent_score > 50:
        dmin = int(dmin * 1.4); dmax = int(dmax * 1.4)
    
    # ── Salva evidenze ────────────────────────────────────────
    all_ev = []
    if auto_ev:   all_ev.append({"cat":"automation","signals":auto_ev[:6],  "score":auto_score})
    if robot_ev:  all_ev.append({"cat":"robotics",  "signals":robot_ev[:6], "score":robot_score})
    if mes_ev:    all_ev.append({"cat":"mes",        "signals":mes_ev[:6],   "score":mes_score})
    if intent_ev: all_ev.append({"cat":"intent",     "signals":intent_ev[:6],"score":intent_score})
    
    return {
        # Score
        "automation_readiness_score":  auto_score,
        "robotics_opportunity_score":  robot_score,
        "mes_opportunity_score":       mes_score,
        "buying_intent_score":         intent_score,
        # Raccomandazione
        "top_opportunity":             recommendation,
        "recommended_solution":        solution,
        # Deal estimate
        "estimated_deal_value_min":    dmin,
        "estimated_deal_value_max":    dmax,
        "estimated_deal_min":          dmin,
        "estimated_deal_max":          dmax,
        # Metadati
        "scan_status":                 "scanned",
        "last_scan_date":              time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ats_documentation":           f"pages={pages_ok} | auto={auto_score} robot={robot_score} mes={mes_score} intent={intent_score}",
        "ats_recent_changes":          json.dumps(all_ev)[:2000],
    }

def load_batch(skip=0, limit=80):
    """Carica aziende NON ancora scansionate"""
    try:
        url = (f"{BASE}?limit={limit}&skip={skip}"
               f"&fields=id,domain,website_url,name,employee_count,scan_status,industry")
        b = requests.get(url, headers=HDRS, timeout=20).json()
        if not isinstance(b, list): return []
        # Filtra solo quelle non ancora scansionate
        return [r for r in b
                if isinstance(r, dict)
                and r.get("scan_status") not in ["scanned","unreachable"]]
    except Exception as e:
        log.warning(f"load_batch: {e}"); return []

# ── MAIN LOOP ─────────────────────────────────────────────────
log.info("=== SIGNAL ENGINE V3 START ===")
log.info("Calcola: automation, robotics, MES, buying_intent")
log.info("Salva scan_status=scanned per evitare ri-scansioni")

global_skip = 0
while True:
    stats["cycle"] += 1
    batch = load_batch(skip=global_skip, limit=80)
    stats["queue"] = len(batch)
    
    if not batch:
        if global_skip == 0:
            log.info(f"[C{stats['cycle']}] DB completamente scansionato. Pausa 1h.")
            time.sleep(3600)
        else:
            global_skip = 0
            log.info(f"[C{stats['cycle']}] Batch vuoto, reset skip. scanned={stats['scanned']}")
        continue
    
    log.info(f"[C{stats['cycle']}] Batch {len(batch)} aziende (skip={global_skip})")
    
    for rec in batch:
        name   = rec.get("name","?")[:40]
        domain = rec.get("domain","?")
        stats["current"] = domain
        
        try:
            result = scan_company(rec)
            if result is None:
                # Nessun dominio → marca come unreachable
                requests.put(f"{BASE}/{rec['id']}",
                    json={"scan_status":"unreachable"}, headers=HDRS, timeout=10)
                continue
            
            r = requests.put(f"{BASE}/{rec['id']}",
                json=result, headers=HDRS, timeout=15)
            
            if r.status_code in [200, 201, 204]:
                stats["scanned"] += 1
                s = result.get("scan_status","?")
                if s == "scanned":
                    log.info(f"  ✅ {name} | auto={result['automation_readiness_score']} "
                             f"robot={result['robotics_opportunity_score']} "
                             f"mes={result['mes_opportunity_score']} "
                             f"intent={result['buying_intent_score']} "
                             f"→ {result['top_opportunity']}")
                else:
                    log.info(f"  ⚠️  {name}: {s}")
            else:
                stats["errors"] += 1
                log.warning(f"  ❌ {domain}: HTTP {r.status_code}")
                
        except Exception as e:
            stats["errors"] += 1
            log.warning(f"  ❌ {domain}: {e}")
        
        time.sleep(1.2)  # Rate limit rispettoso
    
    global_skip += len(batch)
    log.info(f"[C{stats['cycle']}] Progressso: scanned={stats['scanned']} err={stats['errors']} skip={global_skip}")

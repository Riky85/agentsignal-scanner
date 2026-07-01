#!/usr/bin/env python3
"""
signal_engine_v2.py — Motore di segnali industriali
Per ogni azienda scansiona il sito e genera segnali strutturati:
  - Segnali di bisogno (what they need)
  - Segnali di maturità (how ready they are)
  - Raccomandazione prioritaria (what to sell them)

Segnali estratti da: homepage, /prodotti, /soluzioni, /settori,
                     /qualita, /automazione, /about, /careers
"""
import os, re, time, requests, threading, json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, urljoin
import warnings; warnings.filterwarnings("ignore")

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = "6a3a284ab0b87dfa27558bb6"
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", "8080"))

UA = {"User-Agent":"Mozilla/5.0 AppleWebKit/537.36 Chrome/124 Safari/537.36",
      "Accept-Encoding":"gzip,deflate","Accept":"text/html,*/*"}

stats = {"scanned":0,"scored":0,"errors":0,"current":"","cycle":0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),H).serve_forever(),daemon=True).start()

# ============================================================
# DIZIONARIO SEGNALI — keyword → (categoria, peso, segnale)
# ============================================================
SIGNALS = {
    # AUTOMAZIONE & ROBOTICA
    "robot":            ("automation", 20, "Robot presence"),
    "cobot":            ("automation", 25, "Collaborative robot"),
    "agv":              ("automation", 22, "AGV/AMR logistics"),
    "amr":              ("automation", 22, "AGV/AMR logistics"),
    "pick and place":   ("automation", 20, "Pick & place automation"),
    "saldatura":        ("automation", 18, "Welding automation"),
    "welding":          ("automation", 18, "Welding automation"),
    "pallettizzazione": ("automation", 20, "Palletizing"),
    "palletizing":      ("automation", 20, "Palletizing"),
    "automazione":      ("automation", 15, "Automation mentioned"),
    "automation":       ("automation", 15, "Automation mentioned"),
    "cnc":              ("automation", 14, "CNC machining"),
    "plc":              ("automation", 14, "PLC control"),
    "servo":            ("automation", 12, "Servo systems"),
    "machine vision":   ("automation", 18, "Machine vision"),
    "vision system":    ("automation", 18, "Vision system"),
    "conveyor":         ("automation", 10, "Conveyor systems"),
    "lean":             ("automation",  8, "Lean manufacturing"),

    # DIGITALIZZAZIONE & SOFTWARE GAP
    "excel":            ("digital_gap", 20, "Excel-based processes"),
    "manual":           ("digital_gap", 18, "Manual processes"),
    "carta":            ("digital_gap", 15, "Paper-based processes"),
    "paper":            ("digital_gap", 15, "Paper-based processes"),
    "erp":              ("digital",     12, "ERP system"),
    "mes":              ("digital",     18, "MES system"),
    "wms":              ("digital",     14, "WMS system"),
    "scada":            ("digital",     16, "SCADA system"),
    "iiot":             ("digital",     18, "IIoT adoption"),
    "industria 4.0":    ("digital",     16, "Industry 4.0"),
    "industry 4.0":     ("digital",     16, "Industry 4.0"),
    "digital twin":     ("digital",     20, "Digital twin"),
    "traceability":     ("digital",     14, "Traceability"),
    "tracciabilità":    ("digital",     14, "Traceability"),
    "opc ua":           ("digital",     18, "OPC-UA connectivity"),

    # SEGNALI DI BISOGNO (comprano presto)
    "nuova linea":      ("buying",      25, "New production line"),
    "new line":         ("buying",      25, "New production line"),
    "ampliamento":      ("buying",      22, "Plant expansion"),
    "espansione":       ("buying",      22, "Expansion signal"),
    "investimento":     ("buying",      18, "Investment signal"),
    "investment":       ("buying",      18, "Investment signal"),
    "assunzioni":       ("buying",      15, "Hiring signal"),
    "we are hiring":    ("buying",      15, "Hiring signal"),
    "stiamo cercando":  ("buying",      15, "Hiring signal"),
    "efficienza":       ("buying",      12, "Efficiency seeking"),
    "produttività":     ("buying",      12, "Productivity seeking"),
    "ottimizzazione":   ("buying",      14, "Optimization focus"),
    "sustainability":   ("buying",      10, "Sustainability focus"),
    "sostenibilità":    ("buying",      10, "Sustainability focus"),

    # SEGNALI DI MATURITÀ
    "iso 9001":         ("quality",     15, "ISO 9001 certified"),
    "iso 14001":        ("quality",     12, "ISO 14001"),
    "iso 45001":        ("quality",     10, "ISO 45001"),
    "ce marking":       ("quality",     10, "CE certified"),
    "r&d":              ("innovation",  15, "R&D focus"),
    "ricerca e sviluppo":("innovation", 15, "R&D focus"),
    "brevetto":         ("innovation",  18, "Patent holder"),
    "patent":           ("innovation",  18, "Patent holder"),
    "export":           ("market",      12, "Export activity"),
    "worldwide":        ("market",      10, "Global market"),
    "multinazionale":   ("market",      10, "Multinational"),
}

# Raccomandazione in base ai segnali dominanti
def recommend(scores):
    auto  = scores.get("automation", 0)
    gap   = scores.get("digital_gap", 0)
    dig   = scores.get("digital", 0)
    buy   = scores.get("buying", 0)
    inno  = scores.get("innovation", 0)
    qual  = scores.get("quality", 0)
    
    if auto >= 60 and gap >= 30:
        return "AMR/AGV Integration", "High automation need with process gaps"
    if auto >= 40 and dig < 20:
        return "MES Implementation", "Automation present but no digital layer"
    if gap >= 40:
        return "Digital Transformation", "Heavy manual/paper processes"
    if dig >= 40 and auto < 20:
        return "Robotics Introduction", "Digital ready, needs physical automation"
    if buy >= 40:
        return "Proactive Outreach", "Strong buying signals detected"
    if inno >= 30:
        return "AI/ML Integration", "Innovation-driven company"
    if qual >= 30:
        return "Quality Automation", "Quality focus with automation potential"
    return "General Assessment", "Moderate signals across categories"

def fetch_text(url, timeout=8):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, verify=False,
                        allow_redirects=True)
        if r.status_code == 200:
            # Rimuovi HTML tags, prendi testo visibile
            text = re.sub(r'<[^>]+>',' ',r.text)
            text = re.sub(r'\s+',' ',text).lower()
            return text[:8000]
    except: pass
    return ""

def scan_company(rec):
    domain  = rec.get("domain","")
    website = rec.get("website_url") or rec.get("website") or f"https://www.{domain}"
    if not website.startswith("http"):
        website = f"https://{website}"
    
    # Pagine da scansionare
    pages_to_check = [
        website,
        f"{website}/prodotti", f"{website}/products",
        f"{website}/soluzioni", f"{website}/solutions",
        f"{website}/about", f"{website}/chi-siamo",
        f"{website}/qualita", f"{website}/quality",
        f"{website}/lavora-con-noi", f"{website}/careers",
        f"{website}/settori", f"{website}/industries",
    ]
    
    full_text = ""
    pages_ok = 0
    for page in pages_to_check[:6]:  # max 6 pagine
        t = fetch_text(page, timeout=6)
        if t:
            full_text += " " + t
            pages_ok += 1
        if len(full_text) > 20000: break
    
    if not full_text.strip():
        return None
    
    # Analisi segnali
    scores = {}
    detected_signals = []
    
    for keyword, (category, weight, label) in SIGNALS.items():
        if keyword in full_text:
            scores[category] = scores.get(category, 0) + weight
            detected_signals.append({"signal": label, "category": category, "weight": weight})
    
    # Calcola score compositi
    auto_score   = min(100, scores.get("automation", 0))
    dig_score    = min(100, scores.get("digital", 0) + scores.get("digital_gap", 0))
    buy_score    = min(100, scores.get("buying", 0))
    robot_score  = min(100, scores.get("automation", 0))
    mes_score    = min(100, scores.get("digital", 0))
    inno_score   = min(100, scores.get("innovation", 0) + scores.get("quality", 0))
    
    # Raccomandazione
    top_opp, reason = recommend(scores)
    
    # Deal estimate in base alla dimensione
    emp = int(rec.get("employee_count") or 0)
    if emp > 5000:   deal_min, deal_max = 200000, 1000000
    elif emp > 500:  deal_min, deal_max = 50000,  300000
    elif emp > 50:   deal_min, deal_max = 15000,  80000
    else:            deal_min, deal_max = 5000,   30000
    
    return {
        "automation_readiness_score": auto_score,
        "robotics_opportunity_score": robot_score,
        "mes_opportunity_score":      mes_score,
        "buying_intent_score":        buy_score,
        "top_opportunity":            top_opp,
        "recommended_solution":       reason,
        "estimated_deal_value_min":   deal_min,
        "estimated_deal_value_max":   deal_max,
        "scan_status":                "scanned",
        "last_scan_date":             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ats_recent_changes":         json.dumps(detected_signals[:20]),
        "ats_documentation":          f"Pages scanned: {pages_ok} | Signals: {len(detected_signals)}",
    }

def load_batch(skip=0, limit=100):
    """Carica aziende non ancora scansionate"""
    try:
        b = requests.get(
            f"{BASE}?limit={limit}&skip={skip}&fields=id,domain,website_url,website,name,employee_count,scan_status",
            headers=HDRS, timeout=20).json()
        return [r for r in b if isinstance(r, dict) and not r.get("scan_status")]
    except: return []

# Main loop
print("[signal_engine_v2] Avvio", flush=True)
global_skip = 0

while True:
    stats["cycle"] += 1
    batch = load_batch(skip=global_skip, limit=50)
    
    if not batch:
        global_skip = 0
        print(f"[C{stats['cycle']}] Ciclo completo — ricomincio. scanned={stats['scanned']}", flush=True)
        time.sleep(300)
        continue
    
    print(f"[C{stats['cycle']}] Scan {len(batch)} aziende (skip={global_skip})", flush=True)
    
    for rec in batch:
        stats["current"] = rec.get("domain","?")
        try:
            result = scan_company(rec)
            if result:
                r = requests.put(f"{BASE}/{rec['id']}",
                    json=result, headers=HDRS, timeout=15)
                if r.status_code in [200,204]:
                    stats["scored"] += 1
                stats["scanned"] += 1
        except Exception as e:
            stats["errors"] += 1
            print(f"[err] {rec.get('domain','?')} — {e}", flush=True)
        time.sleep(1.5)
    
    global_skip += len(batch)
    print(f"[C{stats['cycle']}] scored={stats['scored']} err={stats['errors']}", flush=True)

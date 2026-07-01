#!/usr/bin/env python3
"""
signal_engine_v5.py — Motore di scansione definitivo.

Design:
- Parallelismo reale: ThreadPoolExecutor a livello di azienda (15 worker default)
  + fetch multi-pagina concorrente per azienda (6 thread interni) => throughput alto.
- Ogni azienda è isolata in un try/except: un errore non ferma mai il batch.
- Scrive scan_status/scanned/last_scan_date/top_opportunity/pipeline_notes
  (campi ora confermati funzionanti sullo schema IndustrialCompany).
- Crea record IndustrialSignal reali con evidenza testuale per il pannello "Why Now".
- Soglia di creazione segnale calibrata a 8 (keyword-matching su testo reale ha
  hit-rate naturalmente basso: meglio mostrare segnali deboli motivati che nulla).
- Healthcheck HTTP server su $PORT per non farsi killare da Railway durante le pause.
- Self-healing: loop esterno con try/except globale, mai si ferma per un'eccezione singola.
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE     = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
SIG_BASE = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialSignal"
HDRS = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT = int(os.environ.get("PORT", 8080))
WORKERS = int(os.environ.get("SCAN_WORKERS", 15))
SIGNAL_THRESHOLD = 8

UA = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,"good":0,
         "signals_created":0,"queue":0,"last_qc":"never","qc":{},"status":"starting"}
lock = threading.Lock()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats, default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] healthcheck su :{PORT} | worker paralleli={WORKERS} | soglia segnale={SIGNAL_THRESHOLD}")

AUTO_KW=[
    "industrial robot","collaborative robot","cobot","robotic arm","robot cell",
    "welding robot","robot welder","robot palletizer","robot gripper",
    "autonomous mobile robot","automated guided vehicle","agv system","amr system",
    "cnc machining","cnc machine","cnc turning","cnc milling","cnc lathe",
    "machining center","turning center","5-axis machining","grinding machine",
    "plc programming","scada system","hmi interface","servo drive","servo system",
    "machine vision system","vision inspection","automated inspection",
    "conveyor system","automated assembly line","pick and place","palletizing system",
    "laser cutting machine","laser welding machine","plasma cutting",
    "robot industriale","braccio robotico","cella robotizzata",
    "saldatura robotizzata","macchina cnc","lavorazione cnc","controllo numerico computerizzato",
    "nastro trasportatore automatico","linea automatizzata","sistema di visione",
    "industrieroboter","schweißroboter","roboterarm","cnc-fräsmaschine",
    "cnc-drehmaschine","fördersystem","automatische montagelinie","bildverarbeitungssystem",
    "robot industriel","bras robotique","cellule robotisée","machine cnc",
    "usinage cnc","centre d'usinage","convoyeur automatique","système de vision industrielle",
    "automation","automazione","automatisierung",
]
ROBOT_KW=[
    "manual welding","manual assembly","manual handling","manual loading",
    "heavy lifting","ergonomic risk","repetitive motion","repetitive assembly",
    "hazardous environment","hot environment","dusty environment",
    "tier 1 supplier","tier 2 supplier","automotive supplier","auto parts",
    "electronics assembly","pcb assembly","semiconductor assembly",
    "injection molding","die casting","metal stamping","sheet metal stamping",
    "forging plant","foundry","casting plant",
    "high volume production","mass production","continuous production",
    "3 shift operation","three shift","24/7 operation","lights out manufacturing",
    "robot integration","robotic integration","system integration","turnkey automation",
    "saldatura manuale","assemblaggio manuale","movimentazione manuale","sollevamento carichi",
    "produzione di massa","fornitore automotive","fonderia","stampaggio a iniezione",
    "manuelle schweißung","manuelle montage","schwerlasthandhabung",
    "automobilzulieferer","großserienfertigung","druckguss","spritzguss","gießerei",
    "soudage manuel","assemblage manuel","manutention manuelle",
    "fournisseur automotive","fonderie","emboutissage","production de masse",
    "manufacturing plant","production facility","stabilimento produttivo",
]
MES_KW=[
    "manufacturing execution system","mes system","erp system","sap manufacturing",
    "oee monitoring","overall equipment effectiveness","scada system","dcs system",
    "opc ua","opcua protocol","mqtt protocol","digital twin","digital factory","smart factory",
    "iiot platform","industrial iot","industry 4.0 platform",
    "predictive maintenance","condition monitoring","vibration monitoring",
    "production scheduling software","paper-based","paper records","manual records",
    "production traceability","lot traceability","batch traceability",
    "serialization system","track and trace system","quality management system",
    "iso 9001 certified","iatf 16949","iso 13485 certified","fda 21 cfr","gmp compliant",
    "sistema mes","sistema erp produzione","industria 4.0","manutenzione predittiva",
    "gemello digitale","fabbrica digitale","tracciabilità di produzione","monitoraggio oee",
    "fertigungsmanagementsystem","industrie 4.0","digitale fabrik","digitaler zwilling",
    "zustandsüberwachung","système mes","maintenance prédictive","usine numérique",
    "jumeau numérique","traçabilité de production","quality management","iso 9001",
]
INTENT_KW=[
    "new manufacturing plant","new production facility","greenfield plant",
    "brownfield expansion","capacity expansion","production capacity increase",
    "new factory opening","plant expansion","new assembly line","new production line",
    "automation engineer","robotics engineer","manufacturing engineer",
    "process engineer","cnc programmer","robot programmer","plc programmer","scada engineer",
    "lean manufacturing engineer","industrial engineer",
    "capital expenditure","capex investment","technology investment",
    "equipment investment","machinery investment","automation investment",
    "digital transformation manufacturing","industry 4.0 implementation",
    "lean transformation","manufacturing modernization","machine retrofit","equipment upgrade",
    "production line upgrade","legacy system replacement","plc upgrade","scada upgrade",
    "nuovo stabilimento produttivo","ampliamento capacità produttiva",
    "nuova linea di produzione","ingegnere automazione","ingegnere produzione",
    "programmatore cnc","trasformazione digitale produzione","modernizzazione impianti",
    "investimento tecnologico","revamping impianto",
    "neues produktionswerk","kapazitätserweiterung","neue produktionslinie",
    "automatisierungsingenieur","fertigungsingenieur","cnc-programmierer",
    "digitale transformation fertigung","maschinenmodernisierung","retrofit maschinen",
    "nouvelle usine de production","expansion capacité","ingénieur automatisation",
    "programmeur cnc","modernisation des équipements","investissement automatisation",
    "hiring","careers","join our team","we are hiring","stiamo assumendo","offerte di lavoro",
]
BLACKLIST = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|'
    r'real estate agent|immobilienmakler|insurance broker|'
    r'restaurant|ristorante|hotel|albergo|'
    r'software development company|web agency|digital marketing agency|'
    r'university|hospital|school|charity|non.?profit|ngo|onlus)\b', re.I)

CATEGORY_MAP = {
    "Robotics & Cobot":"robotics",
    "MES/Digital Factory":"mes_scada",
    "Automation Upgrade":"cnc_machine_tending",
    "High Buying Intent":"growth_buying_intent",
}

def fetch(url, timeout=6):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 300:
            t = re.sub(r'<script[^>]*>.*?</script>', ' ', r.text, flags=re.S)
            t = re.sub(r'<style[^>]*>.*?</style>', '', t, flags=re.S)
            t = re.sub(r'<[^>]+>', ' ', t)
            return re.sub(r'\s+', ' ', t).lower()[:12000]
    except Exception:
        pass
    return ""

def scan_text(rec):
    domain = (rec.get("domain") or "").strip()
    if not domain: return ""
    base_url = rec.get("website_url") or f"https://www.{domain}"
    if not base_url.startswith("http"): base_url = f"https://www.{domain}"
    urls = [base_url, f"{base_url}/products", f"{base_url}/solutions",
            f"{base_url}/technology", f"{base_url}/careers", f"{base_url}/about"]
    text = ""
    with ThreadPoolExecutor(max_workers=6) as ex:
        for t in ex.map(fetch, urls):
            if t: text += " " + t
            if len(text) > 28000: break
    return text

def compute_scores(text):
    a_h=[k for k in AUTO_KW   if k in text]
    ro_h=[k for k in ROBOT_KW  if k in text]
    m_h=[k for k in MES_KW    if k in text]
    i_h=[k for k in INTENT_KW if k in text]
    auto_s =min(100,len(a_h)*8)
    robot_s=min(100,len(ro_h)*12)
    mes_s  =min(100,len(m_h)*10)
    int_s  =min(100,len(i_h)*15)
    scores={"Robotics & Cobot":robot_s,"MES/Digital Factory":mes_s,
            "Automation Upgrade":auto_s,"High Buying Intent":int_s}
    top=max(scores,key=scores.get); best=scores[top]
    if best<8: top="Low Signal"
    ev={"Robotics & Cobot":ro_h[:6],"MES/Digital Factory":m_h[:6],
        "Automation Upgrade":a_h[:6],"High Buying Intent":i_h[:6],"Low Signal":[]}
    solution=("Signals: "+", ".join(ev[top])) if ev[top] else "No specific signals detected"
    return {"_top":top,"_solution":solution,"_evidence":ev,
            "automation_readiness_score":auto_s,"robotics_opportunity_score":robot_s,
            "mes_opportunity_score":mes_s,"buying_intent_score":int_s,
            "amr_agv_opportunity_score":round((robot_s+auto_s)/2),
            "_scores_by_cat":scores}

def deal_range(emp, int_s):
    if   emp>5000: dmin,dmax=300000,2000000
    elif emp>500:  dmin,dmax=80000,500000
    elif emp>100:  dmin,dmax=25000,120000
    elif emp>20:   dmin,dmax=8000,40000
    else:          dmin,dmax=3000,15000
    if int_s>40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)
    return dmin,dmax

def create_signals(rec, result, source_url):
    created = 0
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    for cat, score in result["_scores_by_cat"].items():
        if score < SIGNAL_THRESHOLD: continue
        kws = result["_evidence"].get(cat, [])
        if not kws: continue
        payload = {
            "company_id": rec["id"], "company_name": rec.get("name",""),
            "company_domain": rec.get("domain",""),
            "signal_category": CATEGORY_MAP.get(cat, "growth_buying_intent"),
            "signal_type": kws[0][:60],
            "source_url": source_url,
            "evidence_text": f"Rilevate {len(kws)} evidenze tecniche: {', '.join(kws)}"[:500],
            "confidence_score": min(95, int(score)),
            "detected_at": now, "last_verified_at": now,
        }
        try:
            r = requests.post(SIG_BASE, json=payload, headers=HDRS, timeout=10)
            if r.status_code in (200,201): created += 1
        except Exception:
            pass
    return created

def process_company(rec):
    name = (rec.get("name") or rec.get("domain") or "?")[:38]
    domain = rec.get("domain","?")
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    try:
        text = scan_text(rec)
        if len(text.strip()) < 200:
            requests.put(f"{BASE}/{rec['id']}", json={
                "scan_status":"unreachable","scanned":True,"last_scan_date":now,
                "buying_intent_score":0,"automation_readiness_score":0,
                "robotics_opportunity_score":0,"mes_opportunity_score":0,
            }, headers=HDRS, timeout=10)
            with lock: stats["unreachable"] += 1
            return
        if BLACKLIST.search(text[:3000]):
            requests.put(f"{BASE}/{rec['id']}", json={
                "scan_status":"blacklisted","scanned":True,"last_scan_date":now,
            }, headers=HDRS, timeout=10)
            with lock: stats["unreachable"] += 1
            return
        result = compute_scores(text)
        emp = int(float(rec.get("employee_count") or 0))
        dmin, dmax = deal_range(emp, result["buying_intent_score"])
        base_url = rec.get("website_url") or f"https://www.{domain}"
        payload = {
            "automation_readiness_score": result["automation_readiness_score"],
            "robotics_opportunity_score":  result["robotics_opportunity_score"],
            "mes_opportunity_score":       result["mes_opportunity_score"],
            "buying_intent_score":         result["buying_intent_score"],
            "amr_agv_opportunity_score":   result["amr_agv_opportunity_score"],
            "estimated_deal_value_min": dmin, "estimated_deal_value_max": dmax,
            "estimated_deal_min": dmin, "estimated_deal_max": dmax,
            "scan_status": "completed", "scanned": True, "last_scan_date": now,
            "top_opportunity": result["_top"],
            "pipeline_notes": result["_solution"][:500],
            "recommended_solution": result["_solution"][:200],
        }
        r = requests.put(f"{BASE}/{rec['id']}", json=payload, headers=HDRS, timeout=15)
        if r.status_code in (200,201,204):
            n_sig = create_signals(rec, result, base_url)
            with lock:
                stats["scanned"] += 1
                stats["signals_created"] += n_sig
                best = max(result["automation_readiness_score"], result["robotics_opportunity_score"],
                           result["mes_opportunity_score"], result["buying_intent_score"])
                if best >= SIGNAL_THRESHOLD: stats["good"] += 1
            log.info(f"  ✅ {name:38} A={result['automation_readiness_score']:3.0f} "
                     f"R={result['robotics_opportunity_score']:3.0f} "
                     f"M={result['mes_opportunity_score']:3.0f} "
                     f"I={result['buying_intent_score']:3.0f} sig={n_sig} → {result['_top'][:28]}")
        else:
            with lock: stats["errors"] += 1
            log.warning(f"  ❌ {domain}: HTTP {r.status_code}")
    except Exception as e:
        with lock: stats["errors"] += 1
        log.warning(f"  ❌ {domain}: {str(e)[:120]}")

def quality_check():
    log.info("── QC ──")
    try:
        skip=0; total=0; good=0; zero=0; pending=0
        while True:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=buying_intent_score,"
                             f"robotics_opportunity_score,automation_readiness_score,"
                             f"mes_opportunity_score,scan_status", headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            for x in b:
                if (x.get("scan_status") or "") == "pending": pending += 1; continue
                total += 1
                best = max(x.get("buying_intent_score") or 0, x.get("robotics_opportunity_score") or 0,
                           x.get("automation_readiness_score") or 0, x.get("mes_opportunity_score") or 0)
                if best == 0: zero += 1
                elif best >= SIGNAL_THRESHOLD: good += 1
            skip += 500
            if len(b) < 500: break
        rate = round(good/total*100,1) if total else 0
        log.info(f"  Total:{total+pending} | Scansionati:{total} | Pending:{pending} | Con segnale: {good} ({rate}%) | Zero: {zero}")
        with lock:
            stats["qc"] = {"total":total,"pending":pending,"good":good,"zero":zero,"rate":rate}
            stats["last_qc"] = time.strftime("%H:%M:%S")
        log.info("── END QC ──")
    except Exception as e:
        log.warning(f"qc error: {e}")

def load_pending(limit=300):
    results = []; skip = 0
    while len(results) < limit:
        try:
            b = requests.get(f"{BASE}?limit=200&skip={skip}"
                             f"&fields=id,name,domain,website_url,employee_count,country,scan_status",
                             headers=HDRS, timeout=20).json()
        except Exception as e:
            log.warning(f"load error: {e}"); break
        if not isinstance(b,list) or not b: break
        for r in b:
            if (r.get("scan_status") or "") == "pending":
                results.append(r)
                if len(results) >= limit: break
        if len(b) < 200: break
        skip += 200
    return results

PRIORITY = {"IT","DE","FR","ES","CH","AT","NL","BE","PL","SE","FI","US","GB","JP"}

log.info("=== SIGNAL ENGINE v5 — DEFINITIVO ===")
log.info(f"Parallelismo: {WORKERS} worker | soglia segnale: {SIGNAL_THRESHOLD}")

while True:
    try:
        stats["cycle"] += 1
        batch = load_pending(limit=300)
        stats["queue"] = len(batch)
        if not batch:
            log.info("Nessuna azienda pending. QC + pausa 30 min.")
            stats["status"] = "idle"
            quality_check()
            time.sleep(1800)
            continue
        batch.sort(key=lambda x: (0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
        stats["status"] = "scanning"
        log.info(f"[C{stats['cycle']}] Batch {len(batch)} aziende — {WORKERS} worker paralleli")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(process_company, rec) for rec in batch]
            for f in as_completed(futs):
                pass
        elapsed = time.time() - t0
        log.info(f"[C{stats['cycle']}] fine batch in {elapsed:.0f}s — "
                 f"scanned={stats['scanned']} good={stats['good']} "
                 f"signals={stats['signals_created']} err={stats['errors']}")
        if stats["cycle"] % 3 == 0:
            quality_check()
    except Exception as e:
        log.error(f"ERRORE LOOP PRINCIPALE (continuo comunque): {e}")
        time.sleep(30)

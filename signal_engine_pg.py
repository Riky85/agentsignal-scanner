#!/usr/bin/env python3
"""
signal_engine_pg.py — Motore di Digital Manufacturing Intelligence v6, versione Postgres.

Stessa logica di detection di signal_engine_v6.py (5 domande per azienda: tipo, processi,
segnali di automazione, soluzione, why-now) ma legge/scrive tutto su Postgres (Railway)
invece di chiamare Base44 direttamente per ogni record. Zero chiamate Base44 da qui:
la sync verso Base44 e' un job separato (sync_to_base44.py).

Vantaggi vs v6: da 5-7 chiamate HTTP Base44 per azienda scansionata a ZERO (tutto locale,
gratis, istantaneo). dedup_pass() e quality_check() diventano query SQL singole invece di
GET paginati + DELETE uno a uno.
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

PG_DSN = os.environ.get("DATABASE_URL") or os.environ.get(
    "PG_DSN",
    "postgresql://agent:AgentSignal2026!@postgres-db.railway.internal:5432/agentsignal"
)
def get_conn():
    return psycopg2.connect(PG_DSN, connect_timeout=15, cursor_factory=RealDictCursor)

PORT = int(os.environ.get("PORT", 8080))
WORKERS = int(os.environ.get("SCAN_WORKERS", 12))
SIGNAL_THRESHOLD = 8

UA = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,"good":0,
         "signals_created":0,"tech_created":0,"jobs_created":0,"opps_created":0,
         "queue":0,"last_qc":"never","qc":{},"status":"starting"}
lock = threading.Lock()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats, default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] healthcheck :{PORT} | worker={WORKERS} | soglia={SIGNAL_THRESHOLD} | Postgres-only")

# ─────────────────────────── INDUSTRY CLASSIFICATION ───────────────────────────
INDUSTRY_KW = {
    "Packaging": ["packaging machinery","packaging line","packaging solutions","filling machine","labeling machine","imballaggio","confezionamento"],
    "Automotive": ["automotive supplier","tier 1 supplier","tier 2 supplier","auto parts","car manufacturer","automotive industry","componentistica automotive"],
    "Electronics": ["pcb assembly","electronics manufacturing","semiconductor","electronic components","circuit board","elettronica"],
    "Food & Beverage": ["food processing","beverage production","food manufacturer","food industry","alimentare","bevande"],
    "Pharma": ["pharmaceutical manufacturing","pharma industry","gmp compliant","drug manufacturing","farmaceutico"],
    "Metalworking": ["metalworking","sheet metal","metal fabrication","stamping","forging","lavorazioni meccaniche","carpenteria metallica"],
    "Plastics": ["plastic injection","plastics manufacturing","polymer processing","injection molding","materie plastiche"],
    "Logistics": ["logistics provider","warehouse operations","distribution center","freight","intralogistics","logistica"],
    "Machinery": ["machine builder","industrial machinery","machine manufacturer","macchinari industriali","costruzione macchine"],
    "Aerospace": ["aerospace manufacturer","aviation industry","aircraft components","aerospace supplier","aeronautica"],
    "Medical Devices": ["medical device manufacturer","medical equipment","healthcare devices","dispositivi medici"],
    "Chemicals": ["chemical manufacturer","chemical processing","specialty chemicals","industria chimica"],
    "Furniture": ["furniture manufacturer","furniture production","arredamento","mobili"],
    "Textile": ["textile manufacturer","textile production","fabric mill","tessile"],
    "Construction Materials": ["construction materials","building materials","cement production","materiali da costruzione"],
    "Industrial Components": ["industrial components","mechanical components","precision components","componenti industriali"],
}

# ─────────────────────────── PROCESS SIGNALS ───────────────────────────
PROCESS_KW = {
    "production": ["production line","assembly line","manufacturing","machining","cnc","turning","milling",
                   "welding","injection molding","packaging","palletizing","filling","labeling","sorting",
                   "picking","material handling","end of line","quality control","inspection","testing","traceability"],
    "logistics": ["warehouse","intralogistics","internal transport","forklift","picking","packing","shipping",
                  "distribution center","warehouse expansion","automated warehouse"],
}

# ─────────────────────────── 5 OPPORTUNITY CATEGORIES ───────────────────────────
ROBOTICS_KW = ["manual handling","repetitive tasks","heavy lifting","palletizing","depalletizing",
               "machine tending","pick and place","assembly","welding","packaging line",
               "end-of-line packaging","operator shortage","labor shortage"]
ROBOTICS_SOLUTIONS = {"default":"Collaborative Robot Cell","palletizing":"Palletizing Robot",
                      "machine tending":"Machine Tending Cell","welding":"Robotic Arm & Cell Automation"}

AMR_AGV_KW = ["warehouse expansion","internal logistics","material handling","forklift operators",
              "logistics operators","transport carts","warehouse automation","distribution center",
              "picking operations","high-volume warehouse"]
AMR_SOLUTIONS = {"default":"AMR Fleet","agv":"AGV Deployment","warehouse":"Warehouse Automation",
                 "material handling":"Material Handling Automation"}

MES_KW = [" mes ","mes system","mes software","scada","oee","downtime","production monitoring","traceability","shop floor",
          "digital factory","industry 4.0","smart factory","plc","hmi","opc ua","siemens",
          "rockwell","schneider","wincc","ignition","production planning","performance monitoring"]
# nota: "mes" nudo rimosso — matchava dentro "times","comes","homes","themes","resumes","sometimes" (falso positivo grave)
MES_SOLUTIONS = {"default":"MES / OEE Monitoring","scada":"SCADA Upgrade","plc":"PLC / HMI Retrofit",
                 "iot":"Industrial IoT Platform"}

VISION_KW = ["quality inspection","visual inspection","defect detection","camera inspection",
             "metrology","non-conformity","ocr","barcode verification","traceability",
             "inspection line","quality automation"]
VISION_SOLUTIONS = {"default":"Machine Vision Inspection","quality":"Quality Automation",
                    "ai":"AI Vision System","traceability":"Traceability System"}

MAINT_KW = ["maintenance technician","downtime","preventive maintenance","predictive maintenance",
            "condition monitoring","vibration monitoring","equipment failure","spare parts",
            "maintenance engineer"]
MAINT_SOLUTIONS = {"default":"Predictive Maintenance","monitoring":"Maintenance Monitoring",
                   "sensors":"Industrial IoT Sensors"}

OPP_CATEGORIES = {
    "robotics":  {"kw":ROBOTICS_KW, "field":"robotics_opportunity_score", "cat":"robotics", "sol":ROBOTICS_SOLUTIONS, "weight":12},
    "amr_agv":   {"kw":AMR_AGV_KW,  "field":"amr_agv_opportunity_score", "cat":"amr_agv", "sol":AMR_SOLUTIONS, "weight":12},
    "mes_scada": {"kw":MES_KW,      "field":"mes_opportunity_score", "cat":"mes_scada", "sol":MES_SOLUTIONS, "weight":8},
    "vision":    {"kw":VISION_KW,   "field":"machine_vision_opportunity_score", "cat":"machine_vision", "sol":VISION_SOLUTIONS, "weight":12},
    "maintenance":{"kw":MAINT_KW,   "field":"maintenance_opportunity_score", "cat":"maintenance", "sol":MAINT_SOLUTIONS, "weight":12},
}

# Nota: "hiring"/"careers"/"join our team" rimossi deliberatamente — sono link standard
# presenti su quasi ogni sito aziendale e diluivano il buying intent score con rumore.
# L'hiring specifico (job titles reali) resta tracciato separatamente in detect_jobs().
INTENT_KW = ["new manufacturing plant","new production facility","greenfield plant","brownfield expansion",
             "capacity expansion","production capacity increase","new factory opening","plant expansion",
             "new assembly line","new production line","capital expenditure","capex investment",
             "technology investment","equipment investment","machinery investment","automation investment",
             "digital transformation","industry 4.0 implementation","lean transformation",
             "manufacturing modernization","machine retrofit","equipment upgrade","production line upgrade",
             "acquisition","new plant","new machinery","sustainability investment","operational efficiency"]

# ─────────────────────────── TECHNOLOGY VENDORS ───────────────────────────
TECH_VENDORS = {
    "plc_automation": ["siemens","rockwell","allen-bradley","allen bradley","schneider electric","omron",
                       "beckhoff","mitsubishi electric","b&r automation","phoenix contact"],
    "scada_hmi": ["wincc","ignition scada","wonderware","factorytalk","aveva","ifix"],
    "mes_erp": ["sap erp","sap hana","sap s/4hana","sap business one","running on sap","sap consultant",
                "oracle erp","microsoft dynamics","infor","epicor"," mes "],  # "sap" nudo rimosso: falso positivo su "ASAP"/"disappear"
    "cad_plm": ["solidworks","autocad","siemens nx","ptc creo","catia","autodesk","teamcenter"],
    "robotics": ["universal robots","abb robot","fanuc","kuka","yaskawa","omron robot","mobile industrial robots"," mir ","onrobot","robotiq"],
}

# ─────────────────────────── JOB TITLES ───────────────────────────
JOB_TITLES = ["automation engineer","plc programmer","robotics engineer","manufacturing engineer",
              "production engineer","process engineer","maintenance technician","industrial electrician",
              "cnc operator","warehouse operator","logistics manager","quality control technician",
              "mes specialist","scada engineer","controls engineer","plant manager","operations manager"]

BLACKLIST = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|immobilienmakler|'
    r'insurance broker|restaurant|ristorante|hotel|albergo|software development company|'
    r'web agency|digital marketing agency|university|hospital|school|charity|non.?profit|ngo|onlus)\b', re.I)

def fetch(url, timeout=6):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 300:
            raw = r.text
            t = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.S)
            t = re.sub(r'<style[^>]*>.*?</style>', '', t, flags=re.S)
            t = re.sub(r'<[^>]+>', ' ', t)
            return re.sub(r'\s+', ' ', t).lower()[:15000]
    except Exception:
        pass
    return ""

def gather_pages(base_url):
    """Ritorna dict {url: text} per homepage + pagine chiave (prodotti, careers, news)."""
    urls = {
        "home": base_url,
        "products": f"{base_url}/products",
        "solutions": f"{base_url}/solutions",
        "careers": f"{base_url}/careers",
        "jobs": f"{base_url}/jobs",
        "about": f"{base_url}/about",
        "news": f"{base_url}/news",
        "press": f"{base_url}/press",
    }
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch, u): k for k, u in urls.items()}
        for f in as_completed(futs):
            k = futs[f]
            t = f.result()
            if t: out[k] = t
    return out

def classify_industry(all_text):
    scores = {cat: sum(1 for k in kws if k in all_text) for cat, kws in INDUSTRY_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Industrial Components"

def detect_processes(all_text):
    found = {}
    for cat, kws in PROCESS_KW.items():
        hits = [k for k in kws if k in all_text]
        if hits: found[cat] = hits
    return found

def detect_opportunities(all_text):
    """Ritorna dict categoria -> {score, evidence[], solution}"""
    result = {}
    for key, cfg in OPP_CATEGORIES.items():
        hits = [k for k in cfg["kw"] if k in all_text]
        score = min(100, len(hits) * cfg["weight"])
        sol = cfg["sol"]["default"]
        for tag, s in cfg["sol"].items():
            if tag != "default" and any(tag in h for h in hits):
                sol = s; break
        result[key] = {"score": score, "evidence": hits[:6], "solution": sol, "field": cfg["field"], "cat": cfg["cat"]}
    return result

def detect_technologies(all_text):
    found = []
    for cat, vendors in TECH_VENDORS.items():
        for v in vendors:
            if v.strip() in all_text:
                found.append({"name": v.strip().title(), "category": cat})
    return found

def detect_jobs(careers_text, source_url):
    found = []
    if not careers_text: return found
    for title in JOB_TITLES:
        if title in careers_text:
            idx = careers_text.find(title)
            snippet = careers_text[max(0,idx-100):idx+300]
            kws = [k for k in INTENT_KW if k in snippet] + [title]
            found.append({"title": title.title(), "snippet": snippet[:400], "keywords": list(set(kws))})
    return found[:10]

def compute_buying_intent(all_text):
    hits = [k for k in INTENT_KW if k in all_text]
    return min(100, len(hits) * 10), hits[:8]

def compute_fit_score(employee_count, industry_cat, opp_scores):
    emp_fit = 60
    if employee_count and employee_count > 20: emp_fit = 80
    if employee_count and employee_count > 200: emp_fit = 95
    industry_fit = 90 if industry_cat != "Other" else 50
    signal_fit = min(100, max(opp_scores.values(), default=0))
    return round(emp_fit*0.3 + industry_fit*0.3 + signal_fit*0.4)

def deal_range(emp, top_score):
    if   emp and emp > 5000: dmin,dmax = 300000,2000000
    elif emp and emp > 500:  dmin,dmax = 80000,500000
    elif emp and emp > 100:  dmin,dmax = 25000,120000
    elif emp and emp > 20:   dmin,dmax = 8000,40000
    else:                    dmin,dmax = 3000,15000
    if top_score > 40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)
    return dmin, dmax

def compute_confidence(pages_ok, total_evidence):
    base = min(60, pages_ok * 10)
    ev = min(40, total_evidence * 4)
    return min(100, base + ev)

def fetch(url, timeout=6):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 300:
            raw = r.text
            t = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.S)
            t = re.sub(r'<style[^>]*>.*?</style>', '', t, flags=re.S)
            t = re.sub(r'<[^>]+>', ' ', t)
            return re.sub(r'\s+', ' ', t).lower()[:15000]
    except Exception:
        pass
    return ""

def gather_pages(base_url):
    """Ritorna dict {url: text} per homepage + pagine chiave (prodotti, careers, news)."""
    urls = {
        "home": base_url,
        "products": f"{base_url}/products",
        "solutions": f"{base_url}/solutions",
        "careers": f"{base_url}/careers",
        "jobs": f"{base_url}/jobs",
        "about": f"{base_url}/about",
        "news": f"{base_url}/news",
        "press": f"{base_url}/press",
    }
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch, u): k for k, u in urls.items()}
        for f in as_completed(futs):
            k = futs[f]
            t = f.result()
            if t: out[k] = t
    return out

def classify_industry(all_text):
    scores = {cat: sum(1 for k in kws if k in all_text) for cat, kws in INDUSTRY_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Industrial Components"

def detect_processes(all_text):
    found = {}
    for cat, kws in PROCESS_KW.items():
        hits = [k for k in kws if k in all_text]
        if hits: found[cat] = hits
    return found

def detect_opportunities(all_text):
    """Ritorna dict categoria -> {score, evidence[], solution}"""
    result = {}
    for key, cfg in OPP_CATEGORIES.items():
        hits = [k for k in cfg["kw"] if k in all_text]
        score = min(100, len(hits) * cfg["weight"])
        sol = cfg["sol"]["default"]
        for tag, s in cfg["sol"].items():
            if tag != "default" and any(tag in h for h in hits):
                sol = s; break
        result[key] = {"score": score, "evidence": hits[:6], "solution": sol, "field": cfg["field"], "cat": cfg["cat"]}
    return result

def detect_technologies(all_text):
    found = []
    for cat, vendors in TECH_VENDORS.items():
        for v in vendors:
            if v.strip() in all_text:
                found.append({"name": v.strip().title(), "category": cat})
    return found

def detect_jobs(careers_text, source_url):
    found = []
    if not careers_text: return found
    for title in JOB_TITLES:
        if title in careers_text:
            idx = careers_text.find(title)
            snippet = careers_text[max(0,idx-100):idx+300]
            kws = [k for k in INTENT_KW if k in snippet] + [title]
            found.append({"title": title.title(), "snippet": snippet[:400], "keywords": list(set(kws))})
    return found[:10]

def compute_buying_intent(all_text):
    hits = [k for k in INTENT_KW if k in all_text]
    return min(100, len(hits) * 10), hits[:8]

def compute_fit_score(employee_count, industry_cat, opp_scores):
    emp_fit = 60
    if employee_count and employee_count > 20: emp_fit = 80
    if employee_count and employee_count > 200: emp_fit = 95
    industry_fit = 90 if industry_cat != "Other" else 50
    signal_fit = min(100, max(opp_scores.values(), default=0))
    return round(emp_fit*0.3 + industry_fit*0.3 + signal_fit*0.4)

def deal_range(emp, top_score):
    if   emp and emp > 5000: dmin,dmax = 300000,2000000
    elif emp and emp > 500:  dmin,dmax = 80000,500000
    elif emp and emp > 100:  dmin,dmax = 25000,120000
    elif emp and emp > 20:   dmin,dmax = 8000,40000
    else:                    dmin,dmax = 3000,15000
    if top_score > 40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)
    return dmin, dmax

def compute_confidence(pages_ok, total_evidence):
    base = min(60, pages_ok * 10)
    ev = min(40, total_evidence * 4)
    return min(100, base + ev)


BLACKLIST = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|immobilienmakler|'
    r'insurance broker|restaurant|ristorante|hotel|albergo|software development company|'
    r'web agency|digital marketing agency|university|hospital|school|charity|non.?profit|ngo|onlus)\b', re.I)

def process_company(rec, conn):
    name = (rec.get("name") or rec.get("domain") or "?")[:40]
    domain = rec.get("domain","?")
    cid = rec["id"]
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()
    try:
        base_url = rec.get("website_url") or f"https://www.{domain}"
        if not base_url.startswith("http"): base_url = f"https://www.{domain}"
        pages = gather_pages(base_url)
        all_text = " ".join(pages.values())

        if len(all_text.strip()) < 200:
            cur.execute("""UPDATE industrial_company SET scan_status='unreachable', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            return
        if BLACKLIST.search(all_text[:3000]):
            cur.execute("""UPDATE industrial_company SET scan_status='blacklisted', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            return

        industry_cat = classify_industry(all_text)
        processes = detect_processes(all_text)
        opps = detect_opportunities(all_text)
        techs = detect_technologies(all_text)
        jobs = detect_jobs(pages.get("careers","") + pages.get("jobs",""), f"{base_url}/careers")
        buying_intent, intent_hits = compute_buying_intent(all_text)

        emp = int(float(rec.get("employee_count") or 0)) or None
        opp_scores_only = {k: v["score"] for k,v in opps.items()}
        fit_score = compute_fit_score(emp, industry_cat, opp_scores_only)
        top_key = max(opp_scores_only, key=opp_scores_only.get)
        top_score = opp_scores_only[top_key]
        top_solution = opps[top_key]["solution"] if top_score >= SIGNAL_THRESHOLD else "No specific opportunity detected"
        top_label = {"robotics":"Robotics & Cobot","amr_agv":"AMR / AGV","mes_scada":"MES/SCADA/OEE",
                     "vision":"Machine Vision","maintenance":"Predictive Maintenance"}.get(top_key, top_key)
        dmin, dmax = deal_range(emp, top_score)
        total_evidence = sum(len(v["evidence"]) for v in opps.values()) + len(intent_hits)
        confidence = compute_confidence(len(pages), total_evidence)

        why_now_parts = []
        for k, v in opps.items():
            if v["score"] >= SIGNAL_THRESHOLD and v["evidence"]:
                why_now_parts.append(f"{v['cat']}: {', '.join(v['evidence'][:3])}")
        if intent_hits:
            why_now_parts.append(f"buying intent: {', '.join(intent_hits[:3])}")
        if jobs:
            why_now_parts.append(f"hiring: {', '.join(j['title'] for j in jobs[:3])}")
        why_now_summary = " | ".join(why_now_parts)[:600] if why_now_parts else "No strong signals detected"

        cur.execute("""
            UPDATE industrial_company SET
                automation_readiness_score=%s, robotics_opportunity_score=%s, amr_agv_opportunity_score=%s,
                mes_opportunity_score=%s, machine_vision_opportunity_score=%s, maintenance_opportunity_score=%s,
                buying_intent_score=%s, fit_score=%s, confidence_score=%s, industry_category=%s,
                estimated_deal_value_min=%s, estimated_deal_value_max=%s,
                scan_status='completed', scanned=TRUE, last_scan_date=%s,
                top_opportunity=%s, recommended_solution=%s, pipeline_notes=%s,
                dirty=TRUE, updated_at=now()
            WHERE id=%s
        """, (
            max(opp_scores_only.get("robotics",0), opp_scores_only.get("mes_scada",0)),
            opp_scores_only.get("robotics",0), opp_scores_only.get("amr_agv",0),
            opp_scores_only.get("mes_scada",0), opp_scores_only.get("vision",0),
            opp_scores_only.get("maintenance",0), buying_intent, fit_score, confidence,
            industry_cat, dmin, dmax, now, top_label, top_solution, why_now_summary, cid
        ))
        conn.commit()

        n_sig = 0
        for k, v in opps.items():
            if v["score"] < SIGNAL_THRESHOLD or not v["evidence"]: continue
            cur.execute("""INSERT INTO industrial_signal
                (company_id, company_name, company_domain, signal_category, signal_type,
                 source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, v["cat"], v["evidence"][0][:60], base_url,
                f"Rilevate {len(v['evidence'])} evidenze: {', '.join(v['evidence'])}"[:500],
                min(95, int(v["score"])), now, now))
            n_sig += 1

        n_tech = 0
        seen_tech = set()
        for t in techs:
            if t["name"] in seen_tech: continue
            seen_tech.add(t["name"])
            cur.execute("""INSERT INTO industrial_technology
                (company_id, company_name, company_domain, technology_name, category,
                 confidence_score, evidence_text, source_url, detected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, t["name"], t["category"], 80,
                f"Menzione di '{t['name']}' rilevata sul sito", base_url, now))
            n_tech += 1

        n_jobs = 0
        for j in jobs:
            cur.execute("""INSERT INTO industrial_job_signal
                (company_id, company_domain, job_title, job_description, source_url,
                 extracted_keywords, detected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (
                cid, domain, j["title"], j["snippet"], f"{base_url}/careers", j["keywords"], now))
            n_jobs += 1

        n_opp = 0
        if top_score >= SIGNAL_THRESHOLD:
            cur.execute("""INSERT INTO industrial_opportunity
                (company_id, company_name, company_domain, opportunity_type, recommended_solution,
                 opportunity_score, buying_intent_score, estimated_deal_value_min, estimated_deal_value_max,
                 reason_summary, signals_count, top_signals)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, top_label, top_solution, fit_score, buying_intent,
                dmin, dmax, why_now_summary,
                sum(1 for v in opps.values() if v["score"] >= SIGNAL_THRESHOLD),
                [f"{v['cat']}:{v['score']}" for v in opps.values() if v["score"] >= SIGNAL_THRESHOLD][:5]))
            n_opp = 1
        conn.commit()

        with lock:
            stats["scanned"] += 1
            stats["signals_created"] += n_sig
            stats["tech_created"] += n_tech
            stats["jobs_created"] += n_jobs
            stats["opps_created"] += n_opp
            if top_score >= SIGNAL_THRESHOLD: stats["good"] += 1
        log.info(f"  ✅ {name:38} {industry_cat[:18]:18} fit={fit_score:3d} bi={buying_intent:3d} "
                 f"top={top_label[:20]:20} sig={n_sig} tech={n_tech} jobs={n_jobs} opp={n_opp}")
    except Exception as e:
        conn.rollback()
        with lock: stats["errors"] += 1
        log.warning(f"  ❌ {domain}: {str(e)[:150]}")
    finally:
        cur.close()

def _norm_name(n):
    n = (n or "").lower().strip()
    n = re.sub(r'\s*\([^)]*\)\s*$', '', n)
    n = re.sub(r'\b(inc|inc\.|ltd|ltd\.|llc|gmbh|s\.p\.a\.|spa|s\.r\.l\.|srl|corp|corporation|co\.|company|ag|sa|nv|bv)\b', '', n)
    n = re.sub(r'[^a-z0-9]+', '', n)
    return n

def dedup_pass(conn):
    """Dedup su Postgres: tutto in SQL, istantaneo, zero chiamate esterne."""
    try:
        cur = conn.cursor()
        # Livello 1: stesso dominio esatto -> tiene il piu' vecchio (created_at minore)
        cur.execute("""
            DELETE FROM industrial_company a USING industrial_company b
            WHERE a.domain = b.domain AND a.id > b.id
        """)
        removed_domain = cur.rowcount
        conn.commit()

        # Livello 2: stesso nome normalizzato + stesso paese, dominio diverso
        cur.execute("SELECT id, name, country FROM industrial_company")
        recs = cur.fetchall()
        from collections import defaultdict
        by_name_country = defaultdict(list)
        for r in recs:
            key = (_norm_name(r["name"]), (r["country"] or "").upper())
            if key[0]: by_name_country[key].append(r["id"])
        to_delete = []
        for key, ids in by_name_country.items():
            if len(ids) <= 1: continue
            ids.sort()
            to_delete.extend(ids[1:])
        removed_name = 0
        if to_delete:
            cur.execute("DELETE FROM industrial_company WHERE id = ANY(%s)", (to_delete,))
            removed_name = cur.rowcount
        conn.commit()

        total = removed_domain + removed_name
        if total:
            log.info(f"  🧹 Dedup: rimossi {total} record duplicati ({removed_domain} dominio, {removed_name} nome/paese)")
        else:
            log.info("  🧹 Dedup: nessun doppione trovato")
        with lock: stats["last_dedup"] = {"time": time.strftime("%H:%M:%S"), "removed": total}
        cur.close()
    except Exception as e:
        conn.rollback()
        log.warning(f"dedup error: {e}")

def quality_check(conn):
    log.info("── QC ──")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                count(*) FILTER (WHERE scan_status = 'pending') AS pending,
                count(*) FILTER (WHERE scan_status != 'pending') AS total,
                count(*) FILTER (WHERE scan_status != 'pending' AND coalesce(fit_score,0) >= 60) AS good
            FROM industrial_company
        """)
        row = cur.fetchone()
        total, pending, good = row["total"] or 0, row["pending"] or 0, row["good"] or 0
        rate = round(good/total*100,1) if total else 0
        log.info(f"  Total:{total+pending} | Scansionati:{total} | Pending:{pending} | Fit>=60: {good} ({rate}%)")
        with lock:
            stats["qc"] = {"total":total,"pending":pending,"good":good,"rate":rate}
            stats["last_qc"] = time.strftime("%H:%M:%S")
        cur.close()
        dedup_pass(conn)
    except Exception as e:
        conn.rollback()
        log.warning(f"qc error: {e}")

def load_pending(conn, limit=200):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, domain, website_url, employee_count, country, scan_status
        FROM industrial_company WHERE scan_status='pending'
        ORDER BY (country = ANY(%s)) DESC, id ASC
        LIMIT %s
    """, (list(PRIORITY), limit))
    rows = cur.fetchall()
    cur.close()
    return rows

PRIORITY = {"IT","DE","FR","ES","CH","AT","NL","BE","PL","SE","FI","US","GB","JP"}

log.info("=== SIGNAL ENGINE PG — Digital Manufacturing Intelligence (Postgres-only) ===")
log.info("5 domande per azienda: tipo, processi, segnali, soluzione, why-now")

while True:
    try:
        stats["cycle"] += 1
        conn = get_conn()
        batch = load_pending(conn, limit=200)
        stats["queue"] = len(batch)
        if not batch:
            log.info("Nessuna azienda pending. QC + pausa 30 min.")
            stats["status"] = "idle"
            quality_check(conn)
            conn.close()
            time.sleep(1800)
            continue
        stats["status"] = "scanning"
        log.info(f"[C{stats['cycle']}] Batch {len(batch)} — {WORKERS} worker paralleli")
        t0 = time.time()
        # ogni worker apre la propria connessione Postgres (leggera, locale)
        def _work(rec):
            c = get_conn()
            try:
                process_company(rec, c)
            finally:
                c.close()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(_work, rec) for rec in batch]
            for f in as_completed(futs):
                pass
        elapsed = time.time() - t0
        log.info(f"[C{stats['cycle']}] fine in {elapsed:.0f}s — scanned={stats['scanned']} good={stats['good']} "
                 f"signals={stats['signals_created']} tech={stats['tech_created']} "
                 f"jobs={stats['jobs_created']} opp={stats['opps_created']} err={stats['errors']}")
        if stats["cycle"] % 3 == 0:
            quality_check(conn)
        conn.close()
    except Exception as e:
        log.error(f"ERRORE LOOP PRINCIPALE (continuo): {e}")
        time.sleep(30)

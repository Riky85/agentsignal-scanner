#!/usr/bin/env python3
"""
signal_engine_v6.py — Motore definitivo di Digital Manufacturing Intelligence.

Per ogni azienda risponde a 5 domande:
 1. Che tipo di azienda è?           -> industry_category (classificazione)
 2. Che processi ha?                  -> process signals (production/logistics keyword hits)
 3. Quali segnali di automazione?     -> 5 categorie opportunità con evidenza reale
 4. Che soluzione vendergli?          -> recommended_solution + opportunity record
 5. Perché adesso?                    -> why_now_summary con evidenze datate e sourced

Scrive su 5 tabelle Base44: IndustrialCompany, IndustrialSignal, IndustrialTechnology,
IndustrialJobPosting, IndustrialOpportunity. Ogni segnale ha sempre: score numerico,
evidence testuale, source_url, detected_at — mai booleani nudi.

Parallelo (ThreadPoolExecutor), self-healing, healthcheck HTTP su $PORT.
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE      = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
SIG_BASE  = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialSignal"
TECH_BASE = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialTechnology"
JOB_BASE  = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialJobPosting"
OPP_BASE  = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialOpportunity"
HDRS = {"api-key": API_KEY, "Content-Type": "application/json"}
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
log.info(f"[OK] healthcheck :{PORT} | worker={WORKERS} | soglia={SIGNAL_THRESHOLD}")

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

def post_safe(url, payload, timeout=10, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(url, json=payload, headers=HDRS, timeout=timeout)
            if r.status_code in (200,201): return True
            if r.status_code == 429:
                time.sleep(2 * (attempt+1)); continue
            return False
        except Exception:
            time.sleep(1)
    return False

def put_safe(url, payload, timeout=15, retries=4):
    for attempt in range(retries):
        try:
            r = requests.put(url, json=payload, headers=HDRS, timeout=timeout)
            if r.status_code in (200,201,204): return r
            if r.status_code == 429:
                time.sleep(3 * (attempt+1)); continue
            return r
        except Exception as e:
            time.sleep(1)
    return r

def process_company(rec):
    name = (rec.get("name") or rec.get("domain") or "?")[:40]
    domain = rec.get("domain","?")
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    try:
        base_url = rec.get("website_url") or f"https://www.{domain}"
        if not base_url.startswith("http"): base_url = f"https://www.{domain}"
        pages = gather_pages(base_url)
        all_text = " ".join(pages.values())

        if len(all_text.strip()) < 200:
            requests.put(f"{BASE}/{rec['id']}", json={
                "scan_status":"unreachable","scanned":True,"last_scan_date":now,
            }, headers=HDRS, timeout=10)
            with lock: stats["unreachable"] += 1
            return
        if BLACKLIST.search(all_text[:3000]):
            requests.put(f"{BASE}/{rec['id']}", json={
                "scan_status":"blacklisted","scanned":True,"last_scan_date":now,
            }, headers=HDRS, timeout=10)
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

        payload = {
            "automation_readiness_score": max(opp_scores_only.get("robotics",0), opp_scores_only.get("mes_scada",0)),
            "robotics_opportunity_score": opp_scores_only.get("robotics",0),
            "amr_agv_opportunity_score":  opp_scores_only.get("amr_agv",0),
            "mes_opportunity_score":      opp_scores_only.get("mes_scada",0),
            "machine_vision_opportunity_score": opp_scores_only.get("vision",0),
            "maintenance_opportunity_score": opp_scores_only.get("maintenance",0),
            "buying_intent_score": buying_intent,
            "fit_score": fit_score,
            "confidence_score": confidence,
            "industry_category": industry_cat,
            "estimated_deal_value_min": dmin, "estimated_deal_value_max": dmax,
            "estimated_deal_min": dmin, "estimated_deal_max": dmax,
            "scan_status": "completed", "scanned": True, "last_scan_date": now,
            "top_opportunity": top_label,
            "recommended_solution": top_solution,
            "pipeline_notes": why_now_summary,
        }
        r = put_safe(f"{BASE}/{rec['id']}", payload)
        if r.status_code not in (200,201,204):
            with lock: stats["errors"] += 1
            log.warning(f"  ❌ {domain}: HTTP {r.status_code} su company update")
            return

        n_sig = 0
        for k, v in opps.items():
            if v["score"] < SIGNAL_THRESHOLD or not v["evidence"]: continue
            ok = post_safe(SIG_BASE, {
                "company_id": rec["id"], "company_name": rec.get("name",""), "company_domain": domain,
                "signal_category": v["cat"], "signal_type": v["evidence"][0][:60],
                "source_url": base_url,
                "evidence_text": f"Rilevate {len(v['evidence'])} evidenze: {', '.join(v['evidence'])}"[:500],
                "confidence_score": min(95, int(v["score"])), "detected_at": now, "last_verified_at": now,
            })
            if ok: n_sig += 1

        n_tech = 0
        seen_tech = set()
        for t in techs:
            if t["name"] in seen_tech: continue
            seen_tech.add(t["name"])
            ok = post_safe(TECH_BASE, {
                "company_id": rec["id"], "company_name": rec.get("name",""), "company_domain": domain,
                "technology_name": t["name"], "category": t["category"],
                "confidence_score": 80, "evidence_text": f"Menzione di '{t['name']}' rilevata sul sito",
                "source_url": base_url, "detected_at": now,
            })
            if ok: n_tech += 1

        n_jobs = 0
        for j in jobs:
            ok = post_safe(JOB_BASE, {
                "company_id": rec["id"], "company_name": rec.get("name",""), "company_domain": domain,
                "job_title": j["title"], "job_description": j["snippet"],
                "source_url": f"{base_url}/careers", "detected_at": now,
                "extracted_keywords": j["keywords"],
            })
            if ok: n_jobs += 1

        n_opp = 0
        if top_score >= SIGNAL_THRESHOLD:
            ok = post_safe(OPP_BASE, {
                "company_id": rec["id"], "company_name": rec.get("name",""), "company_domain": domain,
                "opportunity_type": top_label, "recommended_solution": top_solution,
                "opportunity_score": fit_score, "buying_intent_score": buying_intent,
                "estimated_deal_value_min": dmin, "estimated_deal_value_max": dmax,
                "reason_summary": why_now_summary,
                "signals_count": sum(1 for v in opps.values() if v["score"] >= SIGNAL_THRESHOLD),
                "top_signals": [f"{v['cat']}:{v['score']}" for v in opps.values() if v["score"] >= SIGNAL_THRESHOLD][:5],
            })
            if ok: n_opp = 1

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
        with lock: stats["errors"] += 1
        log.warning(f"  ❌ {domain}: {str(e)[:150]}")

def _norm_name(n):
    n = (n or "").lower().strip()
    n = re.sub(r'\s*\([^)]*\)\s*$', '', n)   # rimuove "(Japan)", "(Germany)" ecc. in coda
    n = re.sub(r'\b(inc|inc\.|ltd|ltd\.|llc|gmbh|s\.p\.a\.|spa|s\.r\.l\.|srl|corp|corporation|co\.|company|ag|sa|nv|bv)\b', '', n)
    n = re.sub(r'[^a-z0-9]+', '', n)
    return n

def dedup_pass():
    """Dedup leggero: un solo GET paginato, poi 2 livelli — website esatto E nome normalizzato+paese."""
    try:
        skip=0; recs=[]
        while True:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=id,name,domain,country,created_date",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            recs.extend(b); skip += 500
            if len(b) < 500: break

        from collections import defaultdict
        to_delete = {}

        # Livello 1: stesso dominio esatto (regola primaria da istruzioni utente)
        by_domain = defaultdict(list)
        for r in recs:
            d = (r.get("domain") or "").lower().strip().replace("www.","")
            if d: by_domain[d].append(r)
        for d, v in by_domain.items():
            if len(v) <= 1: continue
            v.sort(key=lambda x: x.get("created_date") or "")
            for extra in v[1:]:
                to_delete[extra["id"]] = extra

        # Livello 2: stesso nome normalizzato + stesso paese ma dominio diverso
        # (es. EagleBurgmann eagleburgmann.jp vs eagleburgmann.com — stessa azienda, 2 record)
        by_name_country = defaultdict(list)
        for r in recs:
            if r["id"] in to_delete: continue
            key = (_norm_name(r.get("name")), (r.get("country") or "").upper())
            if key[0]: by_name_country[key].append(r)
        for key, v in by_name_country.items():
            if len(v) <= 1: continue
            v.sort(key=lambda x: x.get("created_date") or "")
            for extra in v[1:]:
                to_delete[extra["id"]] = extra

        removed = 0
        for cid in to_delete:
            try:
                rr = requests.delete(f"{BASE}/{cid}", headers=HDRS, timeout=8)
                if rr.status_code in (200,204): removed += 1
            except Exception:
                pass
            time.sleep(0.1)

        if removed:
            log.info(f"  🧹 Dedup: rimossi {removed} record duplicati (dominio+nome/paese)")
        else:
            log.info("  🧹 Dedup: nessun doppione trovato")
        with lock: stats["last_dedup"] = {"time": time.strftime("%H:%M:%S"), "removed": removed}
    except Exception as e:
        log.warning(f"dedup error: {e}")

def quality_check():
    log.info("── QC ──")
    try:
        skip=0; total=0; good=0; pending=0
        while True:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=fit_score,scan_status",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            for x in b:
                if (x.get("scan_status") or "") == "pending": pending += 1; continue
                total += 1
                if (x.get("fit_score") or 0) >= 60: good += 1
            skip += 500
            if len(b) < 500: break
        rate = round(good/total*100,1) if total else 0
        log.info(f"  Total:{total+pending} | Scansionati:{total} | Pending:{pending} | Fit>=60: {good} ({rate}%)")
        with lock:
            stats["qc"] = {"total":total,"pending":pending,"good":good,"rate":rate}
            stats["last_qc"] = time.strftime("%H:%M:%S")
        dedup_pass()
    except Exception as e:
        log.warning(f"qc error: {e}")

def load_pending(limit=200):
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

log.info("=== SIGNAL ENGINE v6 — Digital Manufacturing Intelligence ===")
log.info("5 domande per azienda: tipo, processi, segnali, soluzione, why-now")

while True:
    try:
        stats["cycle"] += 1
        batch = load_pending(limit=200)
        stats["queue"] = len(batch)
        if not batch:
            log.info("Nessuna azienda pending. QC + pausa 30 min.")
            stats["status"] = "idle"
            quality_check()
            time.sleep(1800)
            continue
        batch.sort(key=lambda x: (0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
        stats["status"] = "scanning"
        log.info(f"[C{stats['cycle']}] Batch {len(batch)} — {WORKERS} worker paralleli")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(process_company, rec) for rec in batch]
            for f in as_completed(futs):
                pass
        elapsed = time.time() - t0
        log.info(f"[C{stats['cycle']}] fine in {elapsed:.0f}s — scanned={stats['scanned']} good={stats['good']} "
                 f"signals={stats['signals_created']} tech={stats['tech_created']} "
                 f"jobs={stats['jobs_created']} opp={stats['opps_created']} err={stats['errors']}")
        if stats["cycle"] % 3 == 0:
            quality_check()
    except Exception as e:
        log.error(f"ERRORE LOOP PRINCIPALE (continuo): {e}")
        time.sleep(30)

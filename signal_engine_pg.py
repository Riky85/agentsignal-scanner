#!/usr/bin/env python3
"""
signal_engine_pg.py — Digital Manufacturing Intelligence engine, Postgres-only, English output.

For every company answers 5 questions:
 1. What type of company is it?        -> industry_category
 2. What processes does it run?         -> process signals
 3. What automation opportunities exist? -> 5 opportunity categories with real evidence
 4. What should we sell them?           -> solution tags (multiple, comma-separated)
 5. Why now?                            -> short why-now tags (comma-separated, e.g. "Hiring PLC", "Warehouse Expansion")

Every signal/technology carries: evidence text, the EXACT page URL it was found on (not just homepage),
a numeric confidence score, and a timestamp. Also extracts contact info (email, phone, LinkedIn) from
contact/about pages. Writes everything to Postgres (zero Base44 calls — sync is a separate batch job).
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
WORKERS = int(os.environ.get("SCAN_WORKERS", 16))
SIGNAL_THRESHOLD = 8

UA = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,"good":0,
         "signals_created":0,"tech_created":0,"jobs_created":0,"opps_created":0,
         "queue":0,"last_qc":"never","qc":{},"status":"starting"}
lock = threading.Lock()

# ─────────────────────────── SELF-WATCHDOG ───────────────────────────
# Root cause of past silent hangs varies (DB lock pileups, HTTP requests that never
# time out on certain hosts, etc). Rather than chase every possible cause, this
# watchdog forces a hard process exit if NO company has finished processing for
# WATCHDOG_TIMEOUT seconds. The bash supervisor loop around this script (see
# Dockerfile/railway startCommand: "while true; do python3 signal_engine_pg.py; ...")
# immediately relaunches it, so the engine self-heals within minutes instead of
# hanging silently for hours.
_last_progress = {"t": time.time()}
WATCHDOG_TIMEOUT = int(os.environ.get("WATCHDOG_TIMEOUT", 240))  # 4 minutes

def _touch_progress():
    with lock:
        _last_progress["t"] = time.time()

def _watchdog_loop():
    while True:
        time.sleep(20)
        stalled_for = time.time() - _last_progress["t"]
        if stalled_for > WATCHDOG_TIMEOUT:
            log.error(f"WATCHDOG: no progress for {stalled_for:.0f}s (limit {WATCHDOG_TIMEOUT}s) — forcing hard restart")
            os._exit(1)

threading.Thread(target=_watchdog_loop, daemon=True).start()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats, default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        if self.path != "/scan-now":
            self.send_response(404); self.end_headers(); return
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
            result = scan_now(
                (payload.get("name") or "").strip(),
                (payload.get("website") or "").strip(),
                (payload.get("industry") or "").strip(),
                (payload.get("country") or "").strip(),
            )
            status = 200 if "error" not in result else 400
        except Exception as e:
            result, status = {"error": str(e)}, 500
        b = json.dumps(result, default=str).encode()
        self.send_response(status); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] healthcheck+scan-now :{PORT} | workers={WORKERS} | threshold={SIGNAL_THRESHOLD} | Postgres-only")

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
    "Steel & Metals": ["steel producer","steel manufacturer","iron and steel","metal producer","stahlindustrie","acciaieria"],
    "Energy": ["energy production","power generation","renewable energy","energy company","energieerzeugung"],
    "Aerospace & Defense": ["aerospace industry","aircraft manufacturer","defense contractor","aviation supplier"],
    "Water & Utilities": ["water treatment","water utility","wastewater treatment","water supply company"],
    "Glass & Ceramics": ["glass manufacturer","glass production","ceramics manufacturer","glasindustrie"],
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
ROBOTICS_SOLUTIONS = {"default":"Process automation retrofit","palletizing":"Palletizing robot cell",
                      "machine tending":"Machine tending robot cell","welding":"Robotic welding cell"}

AMR_AGV_KW = ["warehouse expansion","internal logistics","material handling","forklift operators",
              "logistics operators","transport carts","warehouse automation","distribution center",
              "picking operations","high-volume warehouse"]
AMR_SOLUTIONS = {"default":"AMR/AGV fleet deployment","agv":"AGV fleet deployment","warehouse":"Warehouse automation",
                 "material handling":"Material handling automation"}

MES_KW = [" mes ","mes system","mes software","scada","oee","downtime","production monitoring","traceability","shop floor",
          "digital factory","industry 4.0","smart factory","plc","hmi","opc ua","siemens",
          "rockwell","schneider","wincc","ignition","production planning","performance monitoring"]
# note: bare "mes" removed — matched inside "times","comes","homes","themes","resumes" (false positive)
MES_SOLUTIONS = {"default":"MES/SCADA digitalization","scada":"SCADA upgrade","plc":"PLC/HMI retrofit",
                 "iot":"Industrial IoT platform"}

VISION_KW = ["quality inspection","visual inspection","defect detection","camera inspection",
             "metrology","non-conformity","ocr","barcode verification","traceability",
             "inspection line","quality automation"]
VISION_SOLUTIONS = {"default":"Machine vision quality inspection","quality":"Quality automation",
                    "ai":"AI vision system","traceability":"Traceability system"}

MAINT_KW = ["maintenance technician","downtime","preventive maintenance","predictive maintenance",
            "condition monitoring","vibration monitoring","equipment failure","spare parts",
            "maintenance engineer"]
MAINT_SOLUTIONS = {"default":"Predictive maintenance program","monitoring":"Maintenance monitoring platform",
                   "sensors":"Industrial IoT sensors"}

OPP_CATEGORIES = {
    "robotics":  {"kw":ROBOTICS_KW, "field":"robotics_opportunity_score", "cat":"robotics", "sol":ROBOTICS_SOLUTIONS, "weight":12},
    "amr_agv":   {"kw":AMR_AGV_KW,  "field":"amr_agv_opportunity_score", "cat":"amr_agv", "sol":AMR_SOLUTIONS, "weight":12},
    "mes_scada": {"kw":MES_KW,      "field":"mes_opportunity_score", "cat":"mes_scada", "sol":MES_SOLUTIONS, "weight":8},
    "vision":    {"kw":VISION_KW,   "field":"machine_vision_opportunity_score", "cat":"machine_vision", "sol":VISION_SOLUTIONS, "weight":12},
    "maintenance":{"kw":MAINT_KW,   "field":"maintenance_opportunity_score", "cat":"maintenance", "sol":MAINT_SOLUTIONS, "weight":12},
}
# Display order for solution tags (matches the UI convention used before)
SOLUTION_ORDER = ["amr_agv", "mes_scada", "vision", "robotics", "maintenance"]

# note: "hiring"/"careers"/"join our team" deliberately excluded — standard links on almost every
# corporate site, they diluted buying intent with noise. Specific hiring is tracked via detect_jobs().
INTENT_KW = ["new manufacturing plant","new production facility","greenfield plant","brownfield expansion",
             "capacity expansion","production capacity increase","new factory opening","plant expansion",
             "new assembly line","new production line","capital expenditure","capex investment",
             "technology investment","equipment investment","machinery investment","automation investment",
             "digital transformation","industry 4.0 implementation","lean transformation",
             "manufacturing modernization","machine retrofit","equipment upgrade","production line upgrade",
             "acquisition","new plant","new machinery","sustainability investment","operational efficiency",
             # more natural phrasing real corporate sites actually use (less formal/press-release-y)
             "we are expanding","we're expanding","expanding our production","expanding our facility",
             "expanding our team","growing rapidly","rapid growth","state-of-the-art facility",
             "state of the art facility","newly built facility","newly opened facility","recently opened",
             "grand opening","new headquarters","relocating to a new","moved to a new facility",
             "million investment","million euro investment","multi-million investment","investing heavily",
             "significant investment","doubling capacity","doubling production","tripling capacity",
             "scaling up production","ramping up production","increased output","expansion project",
             "opens new facility","opened a new facility","invests in new","recently acquired","merger",
             # German (DE/AT/CH sites are very often local-language only)
             "neue produktionslinie","kapazitätserweiterung","werkserweiterung","neues werk","investition in automatisierung",
             "digitalisierung","industrie 4.0","neubau produktion","standorterweiterung","modernisierung der produktion",
             "wir expandieren","wir bauen aus","erweitern unsere produktion","neue niederlassung","übernahme",
             # Italian (IT sites)
             "nuovo stabilimento","ampliamento produttivo","nuova linea di produzione","investimento in automazione",
             "digitalizzazione","industria 4.0","ampliamento capacità produttiva","nuovo impianto",
             "stiamo espandendo","espansione della produzione","nuova sede","acquisizione recente",
             # French (FR/BE/CH sites)
             "nouvelle ligne de production","extension de capacité","nouvelle usine","investissement automatisation",
             "transformation digitale","industrie 4.0","modernisation de la production",
             "nous investissons","nouvelle usine ouverte","nouveau siège"]

# Growth/expansion signals -> short "Why Now" tags (title case, 2-3 words, English)
GROWTH_TAG_MAP = [
    (["new production facility","new manufacturing plant","greenfield plant","new factory opening","new plant",
      "neues werk","neubau produktion","nuovo stabilimento","nuovo impianto","nouvelle usine",
      "state-of-the-art facility","state of the art facility","newly built facility","newly opened facility",
      "recently opened","grand opening","opens new facility","opened a new facility","neue niederlassung",
      "nuova sede"], "New Facility"),
    (["new assembly line","new production line","neue produktionslinie","nuova linea di produzione",
      "nouvelle ligne de production"], "New Production Line"),
    (["capacity expansion","production capacity increase","plant expansion","brownfield expansion",
      "kapazitätserweiterung","werkserweiterung","standorterweiterung","ampliamento produttivo",
      "ampliamento capacità produttiva","extension de capacité","we are expanding","we're expanding",
      "expanding our production","expanding our facility","growing rapidly","rapid growth",
      "doubling capacity","doubling production","tripling capacity","scaling up production",
      "ramping up production","increased output","expansion project","wir expandieren","wir bauen aus",
      "erweitern unsere produktion","stiamo espandendo","espansione della produzione","nous investissons"], "Plant Expansion"),
    (["warehouse expansion","warehouse automation"], "Warehouse Expansion"),
    (["digital transformation","industry 4.0 implementation","smart factory","digitalisierung","industrie 4.0",
      "digitalizzazione","industria 4.0","transformation digitale"], "Digital Transformation"),
    (["automation investment","equipment investment","machinery investment","technology investment",
      "capital expenditure","capex investment","investition in automatisierung","investimento in automazione",
      "investissement automatisation","million investment","million euro investment","multi-million investment",
      "investing heavily","significant investment"], "Automation Investment"),
    (["acquisition","recently acquired","merger","übernahme","acquisizione recente"], "Recent Acquisition"),
    (["machine retrofit","equipment upgrade","production line upgrade","modernisierung der produktion"], "Equipment Upgrade"),
    (["sustainability investment"], "Sustainability Investment"),
    (["lean transformation","manufacturing modernization"], "Manufacturing Modernization"),
    (["new machinery","invests in new"], "New Machinery"),
    (["new headquarters","relocating to a new","moved to a new facility","nouveau siège"], "Relocation/HQ Move"),
]

# ─────────────────────────── TECHNOLOGY VENDORS ───────────────────────────
TECH_VENDORS = {
    "plc_automation": ["siemens","rockwell","allen-bradley","allen bradley","schneider electric","omron",
                       "beckhoff","mitsubishi electric","b&r automation","phoenix contact","abb automation",
                       "yokogawa","emerson automation","honeywell process","bosch rexroth","festo","sew eurodrive",
                       "wago","rittal","pilz safety","turck","ifm electronic","pepperl+fuchs","keyence"],
    "scada_hmi": ["wincc","ignition scada","wonderware","factorytalk","aveva","ifix","citect scada","movicon","zenon scada"],
    "mes_erp": ["sap erp","sap hana","sap s/4hana","sap business one","running on sap","sap consultant",
                "oracle erp","microsoft dynamics","infor","epicor"," mes ","siemens opcenter","critical manufacturing",
                "dassault delmia","plex systems","qad erp","netsuite erp","iqms","proalpha"],
    "cad_plm": ["solidworks","autocad","siemens nx","ptc creo","catia","autodesk","teamcenter","windchill plm"],
    "robotics": ["universal robots","abb robot","fanuc","kuka","yaskawa","omron robot","mobile industrial robots",
                 " mir ","onrobot","robotiq","stäubli robotics","staubli robotics","denso robotics","epson robots"],
    "iiot_platform": ["ptc thingworx","c3 ai","litmus edge","azure iot","aws iot","predix ge","cumulocity"],
}

# ─────────────────────────── JOB TITLES ───────────────────────────
JOB_TITLES = ["automation engineer","plc programmer","robotics engineer","manufacturing engineer",
              "production engineer","process engineer","maintenance technician","industrial electrician",
              "cnc operator","warehouse operator","logistics manager","quality control technician",
              "mes specialist","scada engineer","controls engineer","plant manager","operations manager",
              "automation technician","controls technician","robotics technician","iot engineer",
              "digitalization manager","industry 4.0 manager","lean manufacturing engineer","continuous improvement engineer",
              # broader roles that appear far more often on real careers pages than the specialist titles above
              "machine operator","cnc machinist","assembly line worker","forklift operator","warehouse associate",
              "welder","field service technician","supply chain manager","procurement manager","shift supervisor",
              "quality engineer","supply chain planner","production supervisor","maintenance engineer","project engineer"]

# Short "Hiring X" why-now tags per job title
JOB_TAG_MAP = {
    "automation engineer":"Hiring Automation Engineer", "plc programmer":"Hiring PLC",
    "robotics engineer":"Hiring Robotics Engineer", "manufacturing engineer":"Hiring Manufacturing Engineer",
    "production engineer":"Hiring Production Engineer", "process engineer":"Hiring Process Engineer",
    "maintenance technician":"Hiring Technician", "industrial electrician":"Hiring Electrician",
    "cnc operator":"Hiring CNC Operator", "warehouse operator":"Hiring Warehouse Staff",
    "logistics manager":"Hiring Logistics Manager", "quality control technician":"Hiring QC Technician",
    "mes specialist":"Hiring MES Specialist", "scada engineer":"Hiring SCADA Engineer",
    "controls engineer":"Hiring Controls Engineer", "plant manager":"Hiring Plant Manager",
    "operations manager":"Hiring Operations Manager", "automation technician":"Hiring Automation Technician",
    "controls technician":"Hiring Controls Technician", "robotics technician":"Hiring Robotics Technician",
    "iot engineer":"Hiring IoT Engineer", "digitalization manager":"Hiring Digitalization Manager",
    "industry 4.0 manager":"Hiring Industry 4.0 Manager", "lean manufacturing engineer":"Hiring Lean Engineer",
    "continuous improvement engineer":"Hiring CI Engineer",
}

TOP_LABELS = {"robotics":"Robotics & Cobot","amr_agv":"AMR / AGV","mes_scada":"MES/SCADA/OEE",
              "vision":"Machine Vision","maintenance":"Predictive Maintenance"}

BLACKLIST = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|immobilienmakler|'
    r'insurance broker|restaurant|ristorante|hotel|albergo|software development company|'
    r'web agency|digital marketing agency|university|hospital|school|charity|non.?profit|ngo|onlus)\b', re.I)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(?:\+\d{1,3}[\s.\-]?)?\(?\d{2,4}\)?[\s.\-]{0,2}\d{3,4}[\s.\-]{0,2}\d{3,5}(?:[\s.\-]?\d{2,4})?')
LINKEDIN_RE = re.compile(r'https?://[a-z]{0,3}\.?linkedin\.com/company/[a-zA-Z0-9\-_%]+', re.I)
EMAIL_JUNK = ("sentry.io","wixpress.com","example.com","godaddy.com","cloudflare.com","schema.org",
              "w3.org","google.com","googletagmanager.com","gstatic.com","facebook.com","twitter.com",
              "x.com","wordpress.com","gravatar.com","sentry-next.wixpress.com",".png",".jpg",".jpeg",
              ".gif",".svg",".webp",".ico",".avif",".woff",".woff2",".ttf",".eot",".svg",
              "yourdomain.com","domain.com")
EMAIL_ASSET_RE = re.compile(r'@\d+x\b', re.I)  # retina asset suffixes like name@2x.webp, not real emails
def _is_valid_email(cand):
    cand = cand.strip().strip(".,;:")
    if any(j in cand.lower() for j in EMAIL_JUNK): return False
    if EMAIL_ASSET_RE.search(cand): return False
    domain_part = cand.split("@")[-1]
    tld = domain_part.split(".")[-1] if "." in domain_part else ""
    if not (2 <= len(tld) <= 10 and tld.isalpha()): return False
    return True

# ─────────────────────────── FETCH ───────────────────────────
from requests.adapters import HTTPAdapter, Retry

def make_session():
    """One Session per company: all ~9 page fetches to the same host reuse the same TCP/TLS
    connection (keep-alive) instead of a fresh handshake each time — faster and lighter on
    both ends. Built-in retry adapter absorbs transient 502/503/504 without extra code."""
    s = requests.Session()
    s.headers.update(UA)
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[502, 503, 504],
                   allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch(url, session=None, timeout=6):
    """Returns {'text': cleaned lowercase text for keyword matching, 'raw': html with tags kept
    (minus script/style) for extracting hrefs like mailto:/tel:/linkedin links), 'final_url': resolved URL after redirects."""
    try:
        req = session or requests
        r = req.get(url, timeout=timeout, verify=False, allow_redirects=True,
                    headers=None if session else UA)
        if r.status_code == 200 and len(r.content) > 300:
            raw = r.text
            raw_clean = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.S)
            raw_clean = re.sub(r'<style[^>]*>.*?</style>', '', raw_clean, flags=re.S)
            text = re.sub(r'<[^>]+>', ' ', raw_clean)
            text = re.sub(r'\s+', ' ', text).lower()[:15000]
            return {"text": text, "raw": raw_clean[:40000], "final_url": r.url}
    except Exception:
        pass
    return {"text": "", "raw": "", "final_url": ""}

def _root_domain(host):
    host = (host or "").lower().replace("www.", "").split(":")[0]
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host

def domain_mismatch(original_domain, final_url):
    """True if the fetched page ended up on a completely different domain
    (squatted/parked/redirected-to-unrelated-business). Prevents scanning/extracting
    contacts from the WRONG company."""
    if not final_url: return False
    try:
        final_host = final_url.split("//")[-1].split("/")[0]
    except Exception:
        return False
    return _root_domain(original_domain) != _root_domain(final_host)


def gather_pages(base_url):
    """Returns dict {page_name: {'text':.., 'raw':..}} for homepage + key pages incl. contact.
    All pages share ONE session (connection reuse to the same host = faster + fewer handshakes)."""
    urls = {
        "home": base_url,
        "products": f"{base_url}/products",
        "solutions": f"{base_url}/solutions",
        "contact": f"{base_url}/contact",
        "careers": f"{base_url}/careers",
        "jobs": f"{base_url}/jobs",
        "about": f"{base_url}/about",
        "news": f"{base_url}/news",
        "press": f"{base_url}/press",
    }
    out = {}
    session = make_session()
    try:
        with ThreadPoolExecutor(max_workers=9) as ex:
            futs = {ex.submit(fetch, u, session): k for k, u in urls.items()}
            for f in as_completed(futs):
                k = futs[f]
                r = f.result()
                if r["text"]: out[k] = r
    finally:
        session.close()
    return out, urls

# ─────────────────────────── DETECTION (page-aware) ───────────────────────────
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

def detect_opportunities(pages, page_urls, base_url):
    """Returns dict category -> {score, evidence[], evidence_urls{kw:url}, solution, field, cat}.
    Each evidence keyword is linked to the FIRST specific page it was found on, not just homepage."""
    result = {}
    for key, cfg in OPP_CATEGORIES.items():
        hits, hit_urls = [], {}
        for kw in cfg["kw"]:
            for pname, pdata in pages.items():
                if kw in pdata["text"]:
                    hits.append(kw)
                    hit_urls[kw] = page_urls.get(pname, base_url)
                    break
        score = min(100, len(hits) * cfg["weight"])
        sol = cfg["sol"]["default"]
        for tag, s in cfg["sol"].items():
            if tag != "default" and any(tag in h for h in hits):
                sol = s; break
        result[key] = {"score": score, "evidence": hits[:6], "evidence_urls": hit_urls,
                        "solution": sol, "field": cfg["field"], "cat": cfg["cat"]}
    return result

def detect_technologies(pages, page_urls, base_url):
    found = []
    seen = set()
    for cat, vendors in TECH_VENDORS.items():
        for v in vendors:
            vv = v.strip()
            if not vv or vv in seen: continue
            for pname, pdata in pages.items():
                if vv in pdata["text"]:
                    found.append({"name": vv.title(), "category": cat, "url": page_urls.get(pname, base_url)})
                    seen.add(vv)
                    break
    return found

GENERIC_HIRING_MARKERS = ["open position","open positions","current openings","current opening",
    "job opening","job openings","vacancy","vacancies","we are hiring","we're hiring","join our team",
    "join our growing team","apply now","apply today","view all jobs","browse jobs","open roles","current vacancies",
    # DE/IT/FR equivalents so non-English careers pages are not silently ignored
    "offene stellen","wir stellen ein","jobangebote","posizioni aperte","stiamo assumendo","offerte di lavoro",
    "postes ouverts","nous recrutons","offres d'emploi"]

def detect_jobs(pages, page_urls, base_url):
    found = []
    careers_url = page_urls.get("careers", f"{base_url}/careers")
    careers_text = ""
    for pname in ("careers","jobs"):
        if pname in pages:
            careers_text += pages[pname]["text"]
            careers_url = page_urls.get(pname, careers_url)
    if not careers_text: return found
    for title in JOB_TITLES:
        if title in careers_text:
            idx = careers_text.find(title)
            snippet = careers_text[max(0,idx-100):idx+300]
            kws = [k for k in INTENT_KW if k in snippet] + [title]
            found.append({"title": title.title(), "snippet": snippet[:400],
                          "keywords": list(set(kws)), "url": careers_url})
    # Fallback: no specific title matched, but the careers page clearly has real, current
    # hiring content (not just a bare nav link) -> weaker but still real "Active Hiring" signal.
    if not found and len(careers_text.strip()) > 400:
        if any(m in careers_text for m in GENERIC_HIRING_MARKERS):
            found.append({"title": "Active Hiring", "snippet": careers_text[:400],
                          "keywords": [], "url": careers_url})
    return found[:10]

def compute_buying_intent(pages, page_urls, base_url):
    hits, hit_urls = [], {}
    for k in INTENT_KW:
        for pname, pdata in pages.items():
            if k in pdata["text"]:
                hits.append(k)
                hit_urls[k] = page_urls.get(pname, base_url)
                break
    return min(100, len(hits) * 10), hits[:8], hit_urls

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

def extract_contact_info(pages, domain):
    """Best-effort email / phone / LinkedIn extraction, prioritizing contact > about > home."""
    priority = ["contact","about","home","products","solutions","news","press","careers","jobs"]
    email, phone, linkedin = None, None, None

    for pname in priority:
        if pname not in pages: continue
        raw = pages[pname]["raw"]
        m = re.findall(r'mailto:([^"\'>\s?]+)', raw, re.I)
        for cand in m:
            if "@" in cand and _is_valid_email(cand):
                email = cand.strip(); break
        if not email:
            m = EMAIL_RE.findall(raw)
            for cand in m:
                if _is_valid_email(cand):
                    email = cand.strip(); break
        if email: break

    for pname in priority:
        if pname not in pages: continue
        raw = pages[pname]["raw"]
        # tel: links are the most reliable — sanity check digit count only
        m = re.findall(r'tel:([+\d\s().\-]+)', raw, re.I)
        for cand in m:
            digits = re.sub(r'\D','',cand)
            if 7 <= len(digits) <= 15:
                phone = cand.strip(); break
        if phone: break
        # fallback: plain text match, only accept if it LOOKS like a formatted phone
        # (has a separator or leading +) — rejects raw digit blobs from tracking/IDs
        for m2 in PHONE_RE.findall(pages[pname]["text"]):
            cand = m2.strip()
            digits = re.sub(r'\D','',cand)
            has_separator = bool(re.search(r'[\s().\-]', cand)) or cand.startswith("+")
            if has_separator and 7 <= len(digits) <= 15:
                phone = cand; break
        if phone: break

    for pname in ("home","about","contact"):
        if pname not in pages: continue
        m = LINKEDIN_RE.findall(pages[pname]["raw"])
        if m:
            linkedin = m[0].split("?")[0]; break

    return email, phone, linkedin

MIN_EVIDENCE_FOR_SOLUTION = 2  # a single ambiguous keyword match is not enough to confidently recommend a solution

def build_solution_tags(opps, threshold):
    """Only tag a solution if BOTH the score clears the threshold AND there are
    at least MIN_EVIDENCE_FOR_SOLUTION distinct keyword matches backing it up
    (one lone keyword hit is too weak to make a confident recommendation)."""
    tags = []
    for key in SOLUTION_ORDER:
        v = opps.get(key, {})
        if v.get("score", 0) >= threshold and len(v.get("evidence", [])) >= MIN_EVIDENCE_FOR_SOLUTION:
            tags.append(OPP_CATEGORIES[key]["sol"]["default"])
    return ", ".join(tags)

CURRENT_YEAR = 2026
YEAR_RE = re.compile(r"\b(20[0-2][0-9])\b")

def _is_recent_enough(pages, keyword):
    """Recency guard: a growth/expansion mention next to an OLD year (e.g. an old press
    release still live on the site saying 'we expanded in 2018') should NOT trigger a
    false sense of urgency today. If no year is mentioned near the match, treat it as
    evergreen copy and keep it (most marketing pages don't timestamp themselves)."""
    for pdata in pages.values():
        text = pdata["text"]
        idx = text.find(keyword)
        if idx == -1:
            continue
        window = text[max(0, idx-200):idx+200]
        years = [int(y) for y in YEAR_RE.findall(window)]
        if not years:
            return True
        return max(years) >= CURRENT_YEAR - 1  # keep only if a 2025/2026 mention is nearby
    return True

def build_why_now_tags(opps, intent_hits, jobs, threshold, pages=None, max_tags=6):
    tags = []
    growth_pool = set(intent_hits) | set(opps.get("amr_agv", {}).get("evidence", []))
    # Recency filter: drop growth/expansion mentions that are actually stale old news
    # (e.g. a 2017 press release about "plant expansion" still sitting on the site).
    if pages:
        growth_pool = {k for k in growth_pool if _is_recent_enough(pages, k)}
    for kws, tag in GROWTH_TAG_MAP:
        if any(k in growth_pool for k in kws) and tag not in tags:
            tags.append(tag)
    for j in jobs:
        t = j["title"] if j["title"] == "Active Hiring" else JOB_TAG_MAP.get(j["title"].lower(), f"Hiring {j['title']}")
        if t not in tags:
            tags.append(t)
    if opps.get("vision", {}).get("score", 0) >= threshold and "Quality Automation" not in tags:
        tags.append("Quality Automation")
    if opps.get("maintenance", {}).get("score", 0) >= threshold and "Downtime Reduction" not in tags:
        tags.append("Downtime Reduction")
    # Cross-signal boost: hiring for a role that matches a technical opportunity category
    # already detected (e.g. hiring "robotics engineer" AND robotics signals found on-site)
    # is a much stronger, correlated buying signal than either alone -> surface it explicitly.
    hiring_titles = " ".join(j["title"].lower() for j in jobs)
    role_to_opp = {"robotics": "robotics", "plc": "mes_scada", "automation": "mes_scada",
                   "controls": "mes_scada", "mes": "mes_scada", "scada": "mes_scada",
                   "maintenance": "maintenance", "quality": "vision", "iot": "mes_scada",
                   "digitalization": "mes_scada", "industry 4.0": "mes_scada"}
    for role_kw, opp_key in role_to_opp.items():
        if role_kw in hiring_titles and opps.get(opp_key, {}).get("score", 0) >= threshold:
            if "Hiring Matches Tech Need" not in tags:
                tags.append("Hiring Matches Tech Need")
            break
    return ", ".join(tags[:max_tags])

# ─────────────────────────── MAIN PROCESSING ───────────────────────────
def process_company(rec, conn):
    name = (rec.get("name") or rec.get("domain") or "?")[:40]
    domain = rec.get("domain","?")
    cid = rec["id"]
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()
    try:
        base_url = rec.get("website_url") or f"https://www.{domain}"
        if not base_url.startswith("http"): base_url = f"https://www.{domain}"
        pages, page_urls = gather_pages(base_url)
        all_text = " ".join(p["text"] for p in pages.values())

        if len(all_text.strip()) < 200:
            cur.execute("""UPDATE industrial_company SET scan_status='unreachable', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            _touch_progress()
            return

        # Domain squatted/parked/redirected to an unrelated business (e.g. expired domain
        # now hosting a completely different company) -> NEVER extract signals/contacts
        # from the wrong company. Check against the homepage's resolved final URL.
        home_final = pages.get("home", {}).get("final_url", "")
        if domain_mismatch(domain, home_final):
            cur.execute("""UPDATE industrial_company SET scan_status='domain_mismatch', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            log.info(f"  SKIP {name:38} domain mismatch: {domain} -> {home_final[:60]}")
            return
        if BLACKLIST.search(all_text[:3000]):
            cur.execute("""UPDATE industrial_company SET scan_status='blacklisted', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            _touch_progress()
            return

        industry_cat = classify_industry(all_text)
        opps = detect_opportunities(pages, page_urls, base_url)
        techs = detect_technologies(pages, page_urls, base_url)
        jobs = detect_jobs(pages, page_urls, base_url)
        buying_intent, intent_hits, intent_urls = compute_buying_intent(pages, page_urls, base_url)
        email, phone, linkedin_url = extract_contact_info(pages, domain)

        emp = int(float(rec.get("employee_count") or 0)) or None
        opp_scores_only = {k: v["score"] for k,v in opps.items()}
        fit_score = compute_fit_score(emp, industry_cat, opp_scores_only)
        top_key = max(opp_scores_only, key=opp_scores_only.get)
        top_score = opp_scores_only[top_key]
        # BUG FIX (2026-07-03): max() on an all-zero dict just returns the first key
        # ("robotics") regardless of any real evidence, mislabeling every zero-signal
        # company as "Robotics & Cobot". Only assign a top_opportunity label/deal range
        # when the winning category actually cleared the signal threshold.
        if top_score >= SIGNAL_THRESHOLD:
            top_label = TOP_LABELS.get(top_key, top_key)
            dmin, dmax = deal_range(emp, top_score)
        else:
            top_label = None
            dmin, dmax = None, None
        total_evidence = sum(len(v["evidence"]) for v in opps.values()) + len(intent_hits)
        confidence = compute_confidence(len(pages), total_evidence)

        solution_tags = build_solution_tags(opps, SIGNAL_THRESHOLD)
        why_now_tags = build_why_now_tags(opps, intent_hits, jobs, SIGNAL_THRESHOLD, pages=pages)
        reason_parts = []
        for k, v in opps.items():
            if v["score"] >= SIGNAL_THRESHOLD and v["evidence"]:
                reason_parts.append(f"{v['cat']}: {', '.join(v['evidence'][:3])}")
        if intent_hits:
            reason_parts.append(f"buying intent: {', '.join(intent_hits[:3])}")
        if jobs:
            reason_parts.append(f"hiring: {', '.join(j['title'] for j in jobs[:3])}")
        reason_summary = " | ".join(reason_parts)[:600] if reason_parts else "No strong signals detected"

        cur.execute("""
            UPDATE industrial_company SET
                automation_readiness_score=%s, robotics_opportunity_score=%s, amr_agv_opportunity_score=%s,
                mes_opportunity_score=%s, machine_vision_opportunity_score=%s, maintenance_opportunity_score=%s,
                buying_intent_score=%s, fit_score=%s, confidence_score=%s, industry_category=%s,
                estimated_deal_value_min=%s, estimated_deal_value_max=%s,
                scan_status='completed', scanned=TRUE, last_scan_date=%s,
                top_opportunity=%s, recommended_solution=%s, pipeline_notes=%s,
                email=%s, phone=%s, linkedin_url=%s,
                dirty=TRUE, updated_at=now()
            WHERE id=%s
        """, (
            max(opp_scores_only.get("robotics",0), opp_scores_only.get("mes_scada",0)),
            opp_scores_only.get("robotics",0), opp_scores_only.get("amr_agv",0),
            opp_scores_only.get("mes_scada",0), opp_scores_only.get("vision",0),
            opp_scores_only.get("maintenance",0), buying_intent, fit_score, confidence,
            industry_cat, dmin, dmax, now, top_label, solution_tags, why_now_tags,
            email, phone, linkedin_url, cid
        ))
        conn.commit()

        n_sig = 0
        for k, v in opps.items():
            if v["score"] < SIGNAL_THRESHOLD or not v["evidence"]: continue
            first_kw = v["evidence"][0]
            src_url = v["evidence_urls"].get(first_kw, base_url)
            cur.execute("""INSERT INTO industrial_signal
                (company_id, company_name, company_domain, signal_category, signal_type,
                 source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, v["cat"], first_kw[:60], src_url,
                f"Detected {len(v['evidence'])} matches: {', '.join(v['evidence'])}"[:500],
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
                f"Mention of '{t['name']}' found on site", t["url"], now))
            n_tech += 1

        n_jobs = 0
        for j in jobs:
            cur.execute("""INSERT INTO industrial_job_signal
                (company_id, company_domain, job_title, job_description, source_url,
                 extracted_keywords, detected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (
                cid, domain, j["title"], j["snippet"], j["url"], j["keywords"], now))
            n_jobs += 1

        n_opp = 0
        if top_score >= SIGNAL_THRESHOLD:
            cur.execute("""INSERT INTO industrial_opportunity
                (company_id, company_name, company_domain, opportunity_type, recommended_solution,
                 opportunity_score, buying_intent_score, estimated_deal_value_min, estimated_deal_value_max,
                 reason_summary, signals_count, top_signals)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, top_label, solution_tags or opps[top_key]["solution"],
                fit_score, buying_intent, dmin, dmax, reason_summary,
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
        log.info(f"  OK {name:38} {industry_cat[:18]:18} fit={fit_score:3d} bi={buying_intent:3d} "
                 f"sol=[{solution_tags[:40]:40}] why=[{why_now_tags[:40]:40}] "
                 f"sig={n_sig} tech={n_tech} jobs={n_jobs} opp={n_opp} email={'Y' if email else 'N'} phone={'Y' if phone else 'N'}")
        _touch_progress()
    except Exception as e:
        conn.rollback()
        with lock: stats["errors"] += 1
        log.warning(f"  ERR {domain}: {str(e)[:150]}")
        _touch_progress()
    finally:
        cur.close()

# ─────────────────────────── ON-DEMAND SCAN (manual "Add Company") ───────────────────────────
PLATFORM_BLOCKLIST_SCAN = ["facebook.com","google.com","linkedin.com","instagram.com","amazon.com",
    "shopify.com","twitter.com","x.com","youtube.com","wikipedia.org"]

def scan_now(name, website, industry_hint="", country_hint=""):
    """Runs the SAME real detection pipeline used by the background scanner, synchronously,
    for a single manually-submitted company. Live HTTP validation happens first (same rule as
    bulk import). Upserts into the master Postgres table (dedup by domain) and returns the
    fully enriched result so the caller (e.g. the app's 'Add Company' button) can display it
    immediately instead of waiting for the next batch sync."""
    if not name or not website:
        return {"error": "name and website are required"}
    if not website.startswith("http"):
        website = "https://" + website
    try:
        host = website.split("//")[-1].split("/")[0].lower()
    except Exception:
        return {"error": "invalid website URL"}
    domain = host[4:] if host.startswith("www.") else host
    if any(b in domain for b in PLATFORM_BLOCKLIST_SCAN):
        return {"error": "social/platform URLs are not accepted, please provide the company's own website"}

    # Live HTTP validation BEFORE any insert (same standing rule as the bulk importer)
    try:
        r = requests.get(website, headers=UA, timeout=10, allow_redirects=True)
        if r.status_code >= 400:
            return {"error": f"website not reachable (HTTP {r.status_code})"}
    except Exception as e:
        return {"error": f"website not reachable: {str(e)[:150]}"}

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM industrial_company WHERE domain=%s", (domain,))
        existing = cur.fetchone()
        if existing:
            cid = existing["id"]
            cur.execute("""UPDATE industrial_company SET name=%s, website_url=%s,
                            industry=COALESCE(NULLIF(%s,''), industry), country=COALESCE(NULLIF(%s,''), country),
                            scan_status='processing', updated_at=now() WHERE id=%s""",
                        (name, website, industry_hint, country_hint, cid))
        else:
            cur.execute("""INSERT INTO industrial_company
                (name, domain, website_url, industry, country, source, scan_status, scanned, dirty, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,'manual_add','processing',FALSE,TRUE, now(), now())
                RETURNING id""",
                (name, domain, website, industry_hint or None, country_hint or None))
            cid = cur.fetchone()["id"]
        conn.commit()
    finally:
        cur.close()

    rec = {"id": cid, "name": name, "domain": domain, "website_url": website, "employee_count": None}
    process_company(rec, conn)  # runs the exact same detection + signal/tech/opportunity inserts as the background scanner

    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM industrial_company WHERE id=%s", (cid,))
        company = dict(cur.fetchone())
        cur.execute("""SELECT signal_category, signal_type, evidence_text, source_url, confidence_score
                       FROM industrial_signal WHERE company_id=%s ORDER BY detected_at DESC LIMIT 20""", (cid,))
        signals = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
    return {"company": company, "signals": signals}

def _norm_name(n):
    import unicodedata
    n = (n or "").lower().strip()
    n = re.sub(r'\s*\([^)]*\)\s*$', '', n)
    n = re.sub(r'\b(inc|inc\.|ltd|ltd\.|llc|gmbh|s\.p\.a\.|spa|s\.r\.l\.|srl|corp|corporation|co\.|company|ag|sa|nv|bv)\b', '', n)
    # normalizza accenti/umlaut: Güdel -> Gudel, così matcha eventuali varianti senza dieresi
    n = unicodedata.normalize('NFKD', n).encode('ascii', 'ignore').decode('ascii')
    n = re.sub(r'[^a-z0-9]+', '', n)
    return n

def dedup_pass(conn):
    """Dedup on Postgres: pure SQL, instant, zero external calls."""
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM industrial_company a USING industrial_company b
            WHERE a.domain = b.domain AND a.id > b.id
        """)
        removed_domain = cur.rowcount
        conn.commit()

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
            log.info(f"  Dedup: removed {total} duplicate records ({removed_domain} domain, {removed_name} name/country)")
        else:
            log.info("  Dedup: no duplicates found")
        with lock: stats["last_dedup"] = {"time": time.strftime("%H:%M:%S"), "removed": total}
        cur.close()
    except Exception as e:
        conn.rollback()
        log.warning(f"dedup error: {e}")

def retry_stale_unreachable(conn, days=10, batch_cap=300):
    """Websites marked 'unreachable' might have been down temporarily (maintenance, blip,
    rate limiting) rather than genuinely dead. Retry a capped batch of the oldest ones
    periodically instead of losing them forever."""
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE industrial_company SET scan_status='pending'
            WHERE id IN (
                SELECT id FROM industrial_company
                WHERE scan_status='unreachable' AND last_scan_date < now() - (%s || ' days')::interval
                ORDER BY last_scan_date ASC LIMIT %s
            )
        """, (days, batch_cap))
        n = cur.rowcount
        conn.commit()
        if n:
            log.info(f"  Retry unreachable: {n} companies older than {days}d requeued for a second attempt")
        cur.close()
    except Exception as e:
        conn.rollback()
        log.warning(f"retry_stale_unreachable error: {e}")

def quality_check(conn):
    log.info("-- QC --")
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
        log.info(f"  Total:{total+pending} | Scanned:{total} | Pending:{pending} | Fit>=60: {good} ({rate}%)")
        with lock:
            stats["qc"] = {"total":total,"pending":pending,"good":good,"rate":rate}
            stats["last_qc"] = time.strftime("%H:%M:%S")
        cur.close()
        dedup_pass(conn)
        retry_stale_unreachable(conn)
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

log.info("=== SIGNAL ENGINE PG — Digital Manufacturing Intelligence (Postgres-only, English output) ===")
log.info("5 questions per company: type, processes, signals, solution, why-now")

while True:
    try:
        stats["cycle"] += 1
        conn = get_conn()
        batch = load_pending(conn, limit=200)
        stats["queue"] = len(batch)
        if not batch:
            log.info("No pending companies. QC + 30 min pause.")
            stats["status"] = "idle"
            quality_check(conn)
            conn.close()
            time.sleep(1800)
            continue
        stats["status"] = "scanning"
        log.info(f"[C{stats['cycle']}] Batch {len(batch)} — {WORKERS} parallel workers")
        t0 = time.time()
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
        log.info(f"[C{stats['cycle']}] done in {elapsed:.0f}s — scanned={stats['scanned']} good={stats['good']} "
                 f"signals={stats['signals_created']} tech={stats['tech_created']} "
                 f"jobs={stats['jobs_created']} opp={stats['opps_created']} err={stats['errors']}")
        if stats["cycle"] % 3 == 0:
            quality_check(conn)
        conn.close()
    except Exception as e:
        log.error(f"MAIN LOOP ERROR (continuing): {e}")
        time.sleep(30)

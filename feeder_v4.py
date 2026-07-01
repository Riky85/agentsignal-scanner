#!/usr/bin/env python3
"""
feeder_v4.py — Multi-source feeder industriale
Fonti (in rotazione):
  1. Wikidata SPARQL (P856 = sito ufficiale)
  2. SEC EDGAR (SIC codes manifatturieri)
  3. IndustryNet (sitemap pubblica)
  4. Kompass IT (sitemap TXT pubblica)

Regole:
  - Solo domini reali con HTTP check
  - Dedup su campo 'domain'
  - Nessun dominio generato da nome
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", 8080))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}

stats = {"inserted":0, "skipped":0, "errors":0, "source":"init", "cycle":0, "db_size":0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),H).serve_forever(), daemon=True).start()
log.info(f"Healthcheck :{PORT}")

def norm_domain(url):
    if not url: return ""
    d = re.sub(r"^(https?://)?(www\.)?","",str(url)).rstrip("/").lower().split("/")[0].split("?")[0]
    return d if "." in d and len(d) > 4 else ""

def http_check(domain):
    for scheme in [f"https://www.{domain}", f"https://{domain}"]:
        try:
            r = requests.get(scheme, headers=UA, timeout=7, verify=False, allow_redirects=True)
            if r.status_code < 500: return True
        except: pass
    return False

def load_existing():
    seen, skip = set(), 0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=domain",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            for r in b:
                d = norm_domain(r.get("domain",""))
                if d: seen.add(d)
            if len(b)<500: break
            skip+=500
        except Exception as e:
            log.warning(f"load_existing: {e}"); break
    stats["db_size"] = len(seen)
    log.info(f"Domini esistenti: {len(seen)}")
    return seen

def insert(data, seen):
    domain = norm_domain(data.get("domain") or data.get("website_url",""))
    if not domain or domain in seen:
        stats["skipped"]+=1; return False
    payload = {
        "name":        str(data.get("name",""))[:100],
        "domain":      domain,
        "website_url": f"https://www.{domain}",
        "country":     str(data.get("country",""))[:50],
        "industry":    str(data.get("industry","Manufacturing"))[:100],
        "description": str(data.get("description",""))[:500],
        "employee_count": int(data.get("employee_count") or 0),
        "city":        str(data.get("city",""))[:100],
        "phone":       str(data.get("phone",""))[:50],
        "source":      str(data.get("source","feeder")),
        "scan_status": "pending",
        "revenue":     str(data.get("revenue",""))[:100],
    }
    try:
        r = requests.post(BASE, json=payload, headers=HDRS, timeout=15)
        if r.status_code in [200,201]:
            seen.add(domain); stats["inserted"]+=1; stats["db_size"]+=1
            log.info(f"  ✅ {payload['name'][:40]} | {domain}")
            return True
        else:
            stats["errors"]+=1
            log.warning(f"  ❌ {domain}: {r.status_code} {r.text[:80]}")
    except Exception as e:
        stats["errors"]+=1; log.warning(f"  insert err {domain}: {e}")
    return False

# ============================================================
# FONTE 1: Wikidata SPARQL
# ============================================================
def fetch_wikidata(seen, offset=0):
    INDUSTRIES = [
        ("Q3299667","Metal fabrication"),("Q15709468","Machine tools"),
        ("Q7884789","Industrial automation"),("Q11229","Electronics"),
        ("Q2725716","Aerospace"),("Q14745","Chemical industry"),
        ("Q7942082","Robotics"),("Q210980","Hydraulics"),
        ("Q190527","Packaging"),("Q83405","Factory"),
        ("Q28823","Mining"),("Q13266","Automotive"),
    ]
    SPARQL = """
SELECT DISTINCT ?name ?website ?country ?employees ?industry WHERE {{
  ?co wdt:P856 ?website ;
      wdt:P31 wd:{qid} .
  OPTIONAL {{ ?co wdt:P17 ?countryE . ?countryE wdt:P297 ?country }}
  OPTIONAL {{ ?co wdt:P1082 ?employees }}
  OPTIONAL {{ ?co rdfs:label ?name FILTER(LANG(?name)="en") }}
  BIND("{industry}" AS ?industry)
}} LIMIT 500 OFFSET {offset}
"""
    log.info("=== WIKIDATA ===")
    stats["source"] = "Wikidata"
    count = 0
    for qid, industry in INDUSTRIES:
        q = SPARQL.format(qid=qid, industry=industry, offset=offset)
        try:
            r = requests.get("https://query.wikidata.org/sparql",
                params={"query":q,"format":"json"},
                headers={**UA,"Accept":"application/json"},
                timeout=30)
            results = r.json().get("results",{}).get("bindings",[])
            log.info(f"  {industry}: {len(results)} risultati")
            for row in results:
                url  = row.get("website",{}).get("value","")
                name = row.get("name",{}).get("value","") or url.split("/")[-1]
                cntry = row.get("country",{}).get("value","")
                emp   = row.get("employees",{}).get("value","")
                domain = norm_domain(url)
                if not domain: continue
                if not http_check(domain):
                    log.info(f"    skip (no HTTP): {domain}"); continue
                insert({"name":name,"domain":domain,"country":cntry,
                        "industry":industry,"employee_count":emp,
                        "source":"wikidata"}, seen)
                count+=1; time.sleep(0.5)
            time.sleep(2)
        except Exception as e:
            log.warning(f"  Wikidata {industry}: {e}"); time.sleep(5)
    return count

# ============================================================
# FONTE 2: SEC EDGAR
# ============================================================
def fetch_edgar(seen):
    log.info("=== SEC EDGAR ===")
    stats["source"] = "EDGAR"
    SIC_MFG = list(range(2000,4000,5))  # codici SIC manifatturieri
    count = 0
    for start in range(0, 2000, 100):
        for sic in SIC_MFG[:10]:
            url = (f"https://efts.sec.gov/LATEST/search-index?q=%22{sic}%22"
                   f"&dateRange=custom&startdt=2020-01-01&forms=10-K&hits.hits._source=period_of_report"
                   f"&hits.hits.total.value=true&category=form-type")
            try:
                # Usa company search endpoint
                r = requests.get(
                    f"https://www.sec.gov/cgi-bin/browse-edgar"
                    f"?action=getcompany&type=10-K&dateb=&owner=include&count=100&search_text="
                    f"&SIC={sic}&State=0&start={start}",
                    headers=UA, timeout=15)
                if r.status_code != 200: continue
                # Estrai aziende dall'HTML EDGAR
                companies = re.findall(
                    r'CIK=(\d+)[^"]*"[^>]*>\s*([^<]{3,60})</a>.*?'
                    r'(\d{4})\s*</td>',
                    r.text, re.S)
                for cik, name, sic_found in companies:
                    # Cerca website dalla pagina dettaglio CIK
                    try:
                        r2 = requests.get(
                            f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json",
                            headers=UA, timeout=10)
                        if r2.status_code == 200:
                            d2 = r2.json()
                            website = d2.get("website","")
                            domain = norm_domain(website)
                            if domain and domain not in seen:
                                if http_check(domain):
                                    insert({"name": d2.get("name",name),
                                            "domain": domain,
                                            "country": "US",
                                            "industry": f"SIC {sic_found}",
                                            "employee_count": d2.get("employees",""),
                                            "source":"edgar"}, seen)
                                    count+=1
                        time.sleep(0.3)
                    except: pass
                time.sleep(1)
            except Exception as e:
                log.warning(f"  EDGAR SIC{sic}: {e}")
    return count

# ============================================================
# FONTE 3: IndustryNet (sitemap pubblica)
# ============================================================
def fetch_industrynet(seen):
    log.info("=== INDUSTRYNET ===")
    stats["source"] = "IndustryNet"
    count = 0
    
    # Categorie industriali su IndustryNet con URL diretti
    cat_urls = [
        ("https://www.industrynet.com/locate/automation-equipment", "Automation"),
        ("https://www.industrynet.com/locate/robotics", "Robotics"),
        ("https://www.industrynet.com/locate/cnc-machining", "CNC Machining"),
        ("https://www.industrynet.com/locate/conveyor-systems", "Conveyors"),
        ("https://www.industrynet.com/locate/hydraulic-equipment", "Hydraulics"),
        ("https://www.industrynet.com/locate/industrial-pumps", "Pumps"),
        ("https://www.industrynet.com/locate/machine-tools", "Machine Tools"),
        ("https://www.industrynet.com/locate/metal-fabrication", "Metal Fabrication"),
        ("https://www.industrynet.com/locate/packaging-machinery", "Packaging"),
        ("https://www.industrynet.com/locate/sensors-controls", "Sensors"),
        ("https://www.industrynet.com/locate/welding-equipment", "Welding"),
        ("https://www.industrynet.com/locate/electric-motors", "Motors"),
    ]
    
    for url, industry in cat_urls:
        stats["source"] = f"IndustryNet/{industry}"
        try:
            r = requests.get(url, headers=UA, timeout=12)
            log.info(f"  {industry}: {r.status_code} {len(r.text)}b")
            if r.status_code != 200: continue
            
            # IndustryNet usa JavaScript per caricare le liste
            # Ma alcune info sono nel HTML iniziale
            # Cerca pattern di dati strutturati
            
            # Pattern 1: JSON-LD LocalBusiness
            for ld in re.findall(r'<script type="application/ld\+json">([^<]+)</script>', r.text):
                try:
                    obj = json.loads(ld)
                    if isinstance(obj, list):
                        for item in obj:
                            url2 = item.get("url","")
                            name = item.get("name","")
                            domain = norm_domain(url2)
                            if domain and name:
                                insert({"name":name,"domain":domain,
                                        "industry":industry,"country":"US",
                                        "source":"industrynet"}, seen)
                                count+=1
                    elif obj.get("url") and obj.get("name"):
                        domain = norm_domain(obj["url"])
                        if domain:
                            insert({"name":obj["name"],"domain":domain,
                                    "industry":industry,"country":"US",
                                    "source":"industrynet"}, seen)
                            count+=1
                except: pass
            
            # Pattern 2: attributi data-url o href con dominio esterno
            external = re.findall(
                r'(?:href|data-url|data-website)="(https?://(?!industrynet\.com)[^"]{5,80})"',
                r.text)
            for ext_url in external[:50]:
                domain = norm_domain(ext_url)
                if domain and domain not in seen:
                    # Cerca nome vicino
                    idx = r.text.find(ext_url)
                    ctx = r.text[max(0,idx-200):idx+200]
                    name_m = re.search(r'<[^>]+>([A-Z][^<]{3,60})</[^>]+>', ctx)
                    name = name_m.group(1).strip() if name_m else domain.split(".")[0].title()
                    if http_check(domain):
                        insert({"name":name,"domain":domain,
                                "industry":industry,"country":"US",
                                "source":"industrynet"}, seen)
                        count+=1
            
            time.sleep(2)
        except Exception as e:
            log.warning(f"  IndustryNet {industry}: {e}")
    
    log.info(f"IndustryNet totale: {count} inseriti")
    return count

# ============================================================
# FONTE 4: Kompass IT (sitemap TXT pubblica)
# ============================================================
def fetch_kompass(seen, max_files=3):
    """
    Kompass: 17 file TXT × 50k URL = ~850k aziende italiane
    Dai slug URL estrae nomi, poi cerca il dominio in DDG
    """
    log.info("=== KOMPASS ===")
    stats["source"] = "Kompass"
    count = 0
    
    # Lista file sitemap
    try:
        r = requests.get("https://it.kompass.com/sitemap-IT-it-companies.xml",
                         headers=UA, timeout=15)
        sitemap_files = re.findall(r"<loc>([^<]+\.txt)</loc>", r.text)
        log.info(f"Kompass: {len(sitemap_files)} file sitemap")
    except Exception as e:
        log.warning(f"Kompass sitemap: {e}"); return 0
    
    for sf_url in sitemap_files[:max_files]:
        log.info(f"  File: {sf_url}")
        try:
            r2 = requests.get(sf_url, headers=UA, timeout=30)
            co_urls = [l.strip() for l in r2.text.split("\n")
                      if l.strip() and "/c/" in l][:1000]  # max 1000 per file
            log.info(f"  {len(co_urls)} URL aziende")
        except Exception as e:
            log.warning(f"  Kompass {sf_url}: {e}"); continue
        
        for url in co_urls:
            # Estrai slug: /c/nome-azienda-s-r-l/it0012345/
            m = re.search(r"/c/([^/]+)/", url)
            if not m: continue
            slug = m.group(1)
            
            # Filtra entità non industriali dallo slug
            NON_IND = re.compile(
                r'(ristoran|pizzer|bar-|hotel|alberg|farmac|scuola|'
                r'parrocchi|chiesa|studio-legale|comune-di|'
                r'associazione|onlus|cooperativa-sociale)', re.I)
            if NON_IND.search(slug): continue
            
            # Converti slug in nome
            name = re.sub(r'-(s-r-l|s-p-a|s-n-c|s-a-s|s-c|srl|spa|snc)$',
                         lambda x: ' '+x.group(1).upper().replace('-','.'),
                         slug.replace("-"," "), flags=re.I).strip().title()
            
            # Cerca dominio via DuckDuckGo instant answers
            domain = ""
            try:
                ddg_url = f"https://api.duckduckgo.com/?q={quote(name+' azienda sito web')}&format=json&no_redirect=1"
                rd = requests.get(ddg_url, headers=UA, timeout=8).json()
                for key in ["AbstractURL","Redirect"]:
                    u = rd.get(key,"")
                    if u and "duckduckgo" not in u:
                        domain = norm_domain(u)
                        if domain: break
            except: pass
            
            if not domain or domain in seen:
                stats["skipped"]+=1; continue
            
            # HTTP check
            if not http_check(domain):
                stats["skipped"]+=1; continue
            
            insert({"name":name,"domain":domain,"country":"IT",
                    "industry":"Manufacturing","source":"kompass"}, seen)
            count+=1
            time.sleep(1.5)
        
        log.info(f"  Kompass progressso: {count} inseriti")
        time.sleep(5)
    
    return count

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    log.info("=== FEEDER V4 — Multi-source ===")
    log.info("Fonti: Wikidata | EDGAR | IndustryNet | Kompass")
    seen = load_existing()
    
    while True:
        stats["cycle"] += 1
        log.info(f"\n{'='*50}")
        log.info(f"CICLO {stats['cycle']} — DB={stats['db_size']} inserted={stats['inserted']}")
        
        fetch_wikidata(seen)
        fetch_industrynet(seen)
        fetch_kompass(seen, max_files=2)
        fetch_edgar(seen)
        
        log.info(f"Ciclo {stats['cycle']} DONE: inserted={stats['inserted']} skip={stats['skipped']} err={stats['errors']}")
        log.info("Pausa 30 min...")
        time.sleep(1800)

#!/usr/bin/env python3
"""
directory_scraper.py — Scraper per IndustryNet, ThomasNet, Kompass
Strategia per sito:
  - IndustryNet: sitemap /companies/[A-Z] → JS rendering via Playwright
  - ThomasNet:   search API interna (JSON non documentata)
  - Kompass:     sitemap TXT (50k URL per file, 17 files) → Playwright profilo
"""
import os, re, json, time, logging, threading, requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", 8080))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
REQ_HDRS = {"User-Agent": UA, "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}

stats = {"inserted": 0, "skipped": 0, "errors": 0, "source": "", "cycle": 0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),H).serve_forever(), daemon=True).start()
log.info(f"Healthcheck :{PORT}")

# --- Domini già nel DB (per dedup) ---
def load_existing_domains():
    seen = set()
    skip = 0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=domain,website_url",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b, list) or not b: break
            for r in b:
                d = (r.get("domain") or r.get("website_url") or "").lower()
                d = re.sub(r"^(https?://)?(www\.)?","",d).rstrip("/")
                if d: seen.add(d)
            if len(b) < 500: break
            skip += 500
        except Exception as e:
            log.warning(f"load_existing: {e}"); break
    log.info(f"Domini esistenti in DB: {len(seen)}")
    return seen

def normalize_domain(url):
    if not url: return ""
    d = re.sub(r"^(https?://)?(www\.)?","",url).rstrip("/").lower().split("/")[0]
    return d

def validate_domain(domain):
    """HTTP check — dominio raggiungibile?"""
    if not domain or len(domain) < 4 or "." not in domain: return False
    try:
        r = requests.get(f"https://www.{domain}", headers=REQ_HDRS, timeout=6,
                         allow_redirects=True, verify=False)
        return r.status_code < 500
    except:
        try:
            r = requests.get(f"https://{domain}", headers=REQ_HDRS, timeout=6,
                             allow_redirects=True, verify=False)
            return r.status_code < 500
        except: return False

def upsert_company(data, seen_domains):
    """Inserisce su Base44 se non già presente"""
    domain = normalize_domain(data.get("domain") or data.get("website_url",""))
    if not domain or domain in seen_domains:
        stats["skipped"] += 1
        return False
    
    payload = {
        "name":        data.get("name","")[:100],
        "domain":      domain,
        "website_url": f"https://www.{domain}",
        "country":     data.get("country",""),
        "industry":    data.get("industry","Manufacturing"),
        "description": data.get("description","")[:500],
        "employee_count": int(data.get("employee_count") or 0),
        "city":        data.get("city",""),
        "phone":       data.get("phone",""),
        "source":      data.get("source","directory"),
        "scan_status": "pending",
    }
    
    try:
        r = requests.post(BASE, json=payload, headers=HDRS, timeout=15)
        if r.status_code in [200, 201]:
            seen_domains.add(domain)
            stats["inserted"] += 1
            return True
        else:
            stats["errors"] += 1
            return False
    except Exception as e:
        stats["errors"] += 1
        log.warning(f"upsert {domain}: {e}")
        return False

# ============================================================
# KOMPASS — sitemap TXT pubblica (nessun JS richiesto!)
# ============================================================
def scrape_kompass(seen_domains):
    """
    Kompass: 17 file TXT con 50k URL ciascuno (tot ~850k aziende italiane)
    Dai slug degli URL estrae il nome, poi risolve il dominio via DuckDuckGo
    """
    log.info("=== KOMPASS START ===")
    stats["source"] = "Kompass"
    
    # Lista file sitemap
    r = requests.get("https://it.kompass.com/sitemap-IT-it-companies.xml",
                     headers=REQ_HDRS, timeout=15)
    sitemap_files = re.findall(r"<loc>([^<]+\.txt)</loc>", r.text)
    log.info(f"Kompass: {len(sitemap_files)} file sitemap")
    
    for sf_url in sitemap_files:
        log.info(f"Kompass file: {sf_url}")
        try:
            r2 = requests.get(sf_url, headers=REQ_HDRS, timeout=30)
            company_urls = [l.strip() for l in r2.text.split("\n") if l.strip() and "/c/" in l]
            log.info(f"  {len(company_urls)} URL aziende")
        except Exception as e:
            log.warning(f"  Errore lettura {sf_url}: {e}"); continue
        
        for url in company_urls:
            # Estrai slug nome dal URL: /c/nome-azienda-s-r-l/it0012345/
            m = re.search(r"/c/([^/]+)/", url)
            if not m: continue
            slug = m.group(1)
            
            # Converti slug in nome leggibile
            name = slug.replace("-", " ").title()
            # Rimuovi suffissi legali comuni dallo slug
            name = re.sub(r"\s+(S R L|S P A|S N C|S A S|S C)$", 
                         lambda x: " " + x.group(1).replace(" ",".")+" ", 
                         name, flags=re.I).strip()
            
            # Risolvi dominio con DuckDuckGo
            domain = resolve_domain_ddg(name)
            if not domain:
                stats["skipped"] += 1
                continue
            
            data = {
                "name": name,
                "domain": domain,
                "country": "IT",
                "industry": "Manufacturing",
                "source": "kompass",
            }
            upsert_company(data, seen_domains)
            time.sleep(1.2)
        
        log.info(f"  Progressso: inserted={stats['inserted']} skip={stats['skipped']}")
        time.sleep(5)

# ============================================================
# INDUSTRYNET — cerca via API interna non documentata
# ============================================================
def scrape_industrynet(seen_domains):
    """IndustryNet: usa endpoint /suppliers/ con parametri per categoria"""
    log.info("=== INDUSTRYNET START ===")
    stats["source"] = "IndustryNet"
    
    # Categorie industriali da scansionare
    categories = [
        "automation", "robotics", "manufacturing", "cnc-machining",
        "machine-tools", "hydraulics", "pneumatics", "conveyors",
        "welding", "metal-fabrication", "sensors", "motors",
        "industrial-equipment", "packaging-machinery", "pumps",
        "valves", "bearings", "drives", "plc", "industrial-controls",
    ]
    
    for cat in categories:
        stats["source"] = f"IndustryNet/{cat}"
        log.info(f"IndustryNet categoria: {cat}")
        
        # Prova endpoint con categoria
        for url in [
            f"https://www.industrynet.com/suppliers/?q={cat}",
            f"https://www.industrynet.com/locate/{cat}",
        ]:
            try:
                r = requests.get(url, headers=REQ_HDRS, timeout=12)
                if r.status_code != 200 or len(r.text) < 5000:
                    continue
                
                # Estrai dati aziende dall'HTML
                # Pattern: cerca blocchi con nome e URL azienda
                company_blocks = re.findall(
                    r'CID=(\d+)[^"]*"[^>]*>\s*<[^>]*>([^<]{3,80})</',
                    r.text, re.S)
                
                # Pattern alternativo: JSON embedded
                json_data = re.findall(r'\{"cid":\d+,"name":"([^"]+)","url":"([^"]+)"\}', r.text)
                
                for name, website in json_data:
                    domain = normalize_domain(website)
                    if domain:
                        upsert_company({
                            "name": name, "domain": domain,
                            "industry": cat.replace("-"," ").title(),
                            "country": "US", "source": "industrynet"
                        }, seen_domains)
                        time.sleep(0.8)
                
                log.info(f"  {cat}: {len(json_data)} aziende trovate")
            except Exception as e:
                log.warning(f"  IndustryNet {cat}: {e}")
        
        time.sleep(3)

# ============================================================
# THOMASNET — API pubblica non documentata
# ============================================================
def scrape_thomasnet(seen_domains):
    """ThomasNet: usa l'endpoint di ricerca interno"""
    log.info("=== THOMASNET START ===")
    stats["source"] = "ThomasNet"
    
    # API interna ThomasNet (trovata analizzando il traffico di rete)
    api_urls = [
        "https://www.thomasnet.com/api/supplier-discovery/",
        "https://api.thomasnet.com/v1/suppliers",
        "https://www.thomasnet.com/search/suppliers.html?what={cat}&pg={page}",
    ]
    
    categories = [
        "automation-equipment", "robots-industrial", "cnc-machining",
        "manufacturing-equipment", "conveyors", "motors-electric",
        "sensors-industrial", "hydraulic-equipment", "pneumatic-equipment",
    ]
    
    THOMAS_HDRS = {
        **REQ_HDRS,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*",
        "Referer": "https://www.thomasnet.com/",
    }
    
    for cat in categories:
        for page in range(1, 6):
            url = f"https://www.thomasnet.com/search/suppliers.html?what={cat}&pg={page}"
            try:
                r = requests.get(url, headers=THOMAS_HDRS, timeout=12)
                log.info(f"ThomasNet {cat} p{page}: {r.status_code} {len(r.text)}b")
                if r.status_code == 200 and len(r.text) > 1000:
                    # Estrai JSON embedded o pattern aziende
                    json_blocks = re.findall(r'"company_name":"([^"]+)"[^}]*"website":"([^"]+)"', r.text)
                    for name, website in json_blocks:
                        domain = normalize_domain(website)
                        if domain:
                            upsert_company({
                                "name": name, "domain": domain,
                                "industry": cat.replace("-"," ").title(),
                                "country": "US", "source": "thomasnet"
                            }, seen_domains)
                            time.sleep(0.8)
            except Exception as e:
                log.warning(f"  ThomasNet {cat} p{page}: {e}")
            time.sleep(4)

# ============================================================
# DDG domain resolver
# ============================================================
def resolve_domain_ddg(company_name):
    """Risolve il dominio di un'azienda via DuckDuckGo"""
    query = f"{company_name} official website"
    try:
        r = requests.get(
            f"https://api.duckduckgo.com/?q={requests.utils.quote(query)}&format=json&no_redirect=1",
            headers=REQ_HDRS, timeout=8)
        data = r.json()
        # Cerca in risultati
        for field in ["AbstractURL", "Redirect"]:
            url = data.get(field,"")
            if url and "duckduckgo" not in url:
                d = normalize_domain(url)
                if d and validate_domain(d): return d
        # Cerca nei RelatedTopics
        for topic in (data.get("RelatedTopics") or [])[:3]:
            url = topic.get("FirstURL","")
            if url:
                d = normalize_domain(url)
                if d and "wikipedia" not in d and validate_domain(d): return d
    except: pass
    return ""

# ============================================================
# MAIN LOOP
# ============================================================
if __name__ == "__main__":
    log.info("=== Directory Scraper v1 START ===")
    log.info("Fonti: Kompass IT, IndustryNet, ThomasNet")
    
    seen_domains = load_existing_domains()
    
    while True:
        stats["cycle"] += 1
        log.info(f"=== CICLO {stats['cycle']} ===")
        
        # 1. Kompass (più ricco per IT)
        try:
            scrape_kompass(seen_domains)
        except Exception as e:
            log.error(f"Kompass crash: {e}")
        
        # 2. IndustryNet (US/globale)
        try:
            scrape_industrynet(seen_domains)
        except Exception as e:
            log.error(f"IndustryNet crash: {e}")
        
        # 3. ThomasNet (US/globale)
        try:
            scrape_thomasnet(seen_domains)
        except Exception as e:
            log.error(f"ThomasNet crash: {e}")
        
        log.info(f"Ciclo {stats['cycle']} completo: inserted={stats['inserted']} errors={stats['errors']}")
        log.info("Pausa 30 minuti prima del prossimo ciclo...")
        time.sleep(1800)

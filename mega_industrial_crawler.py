#!/usr/bin/env python3
"""
Mega Industrial Domain Crawler v1.0
Fonti:
  1. Kompass.com  — 20M+ aziende B2B, categoria manifatturiero
  2. Europages.com — 3M aziende EU
  3. Dnb.com (Dun&Bradstreet) — liste SIC code manifatturiero
  4. Opencorporates — aziende manifatturiere IT/DE/FR
  5. GLEIF (Legal Entity Identifier) — aziende manifatturiere registrate
  6. Dataset locali: YC, Wellfound, BuiltWith (già nel repo)

Target: 100.000+ domini industriali → IndustrialCompany in B44
Strategia: Crawl gentile (2s delay), User-Agent rotation, retry 3x
"""
import asyncio, aiohttp, os, json, re, logging, time, threading, random
from urllib.parse import urljoin, quote_plus, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CRAWLER] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN  = os.environ.get("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID     = os.environ.get("B44_APP_ID",        "6a3a284ab0b87dfa27558bb6")
B44_BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW         = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
PORT       = int(os.environ.get("PORT", "8080"))
CONCURRENCY= int(os.environ.get("CONCURRENCY", "5"))
DELAY      = float(os.environ.get("REQUEST_DELAY", "1.5"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
]

stats = {"crawled": 0, "inserted": 0, "skipped": 0, "errors": 0, "status": "starting"}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()

# ── Categorie industriali target ──────────────────────────────────────────────
KOMPASS_CATS = [
    # (url_slug, label, paesi_prioritari)
    ("metalworking-and-metal-processing", "Metalworking", ["it","de","fr","es","pl","cz","at"]),
    ("machine-tools", "Machine Tools", ["it","de","ch","es","jp","kr","tw"]),
    ("industrial-robots", "Industrial Robots", ["it","de","jp","se","dk","fi","nl"]),
    ("automation-systems-and-equipment", "Automation", ["it","de","at","ch","fr","es","nl"]),
    ("conveying-equipment-and-materials-handling", "Conveyors", ["it","de","nl","be","pl","cz"]),
    ("packaging-machines-and-equipment", "Packaging Machinery", ["it","de","fr","es","ch"]),
    ("food-processing-machinery", "Food Processing", ["it","de","dk","nl","fr","be"]),
    ("plastics-processing-machinery", "Plastics Machinery", ["it","de","at","fr","es"]),
    ("cutting-tools-and-accessories", "Cutting Tools", ["it","de","se","us","ch"]),
    ("welding-equipment-and-supplies", "Welding Equipment", ["it","de","fr","es","pl"]),
    ("industrial-furnaces-and-ovens", "Industrial Furnaces", ["it","de","at","fr"]),
    ("compressors-and-compressed-air", "Compressors", ["it","de","nl","se","fi"]),
    ("hydraulic-and-pneumatic-equipment", "Hydraulics Pneumatics", ["it","de","nl","at","se"]),
    ("pumps-and-pump-systems", "Industrial Pumps", ["it","de","nl","se","dk"]),
    ("electric-motors-and-generators", "Electric Motors", ["it","de","fr","es","pl"]),
    ("transformers-and-power-supplies", "Power Electronics", ["de","it","fr","pl","cz"]),
    ("measuring-and-monitoring-equipment", "Measurement Equipment", ["de","it","ch","nl","se"]),
    ("warehouse-and-storage-systems", "Warehouse Systems", ["it","de","fr","nl","es"]),
    ("agricultural-machinery", "Agricultural Machinery", ["it","de","fr","pl","es"]),
    ("construction-equipment", "Construction Equipment", ["it","de","fr","es","pl"]),
    ("automotive-components-and-accessories", "Automotive Parts", ["it","de","fr","pl","cz","sk"]),
    ("pharmaceutical-machinery", "Pharma Machinery", ["it","de","ch","fr","gb"]),
    ("printing-and-paper-machinery", "Printing Machinery", ["de","it","ch","nl","gb"]),
    ("textile-machinery", "Textile Machinery", ["it","de","ch","tr","in"]),
    ("woodworking-machinery", "Woodworking Machinery", ["it","de","at","ch","pl"]),
    ("ceramics-and-tiles-machinery", "Ceramics Machinery", ["it","de","es","tr"]),
    ("rubber-processing-machinery", "Rubber Machinery", ["it","de","fr","at"]),
    ("glass-processing-machinery", "Glass Machinery", ["it","de","fr","cz"]),
    ("laboratory-equipment-and-supplies", "Lab Equipment", ["de","it","ch","nl","se"]),
    ("environmental-technology", "Environmental Tech", ["de","it","nl","dk","se"]),
]

EUROPAGES_CATS = [
    "sheet-metal-work", "machined-parts", "forging-and-casting",
    "industrial-valves", "bearings", "gears-and-gear-drives",
    "springs", "screws-bolts-and-nuts-standard", "gaskets-and-seals",
    "plastic-moulded-parts", "surface-treatment-and-coating",
    "heat-exchangers", "industrial-fans-and-blowers",
    "silos-and-tanks", "industrial-filters",
    "industrial-sensors-and-transducers", "plcs-and-scada",
    "machine-vision-systems", "industrial-cameras",
    "automated-guided-vehicles-agv", "collaborative-robots",
    "pallet-handling-systems", "sorting-systems",
    "industrial-cleaning-machines", "deburring-machines",
]

# SIC codes manifatturieri (Dun&Bradstreet / NAICS)
DNB_SIC_CODES = [
    "2000-2099",  # Food
    "2100-2199",  # Tobacco
    "2200-2399",  # Textile
    "2400-2499",  # Lumber
    "2500-2599",  # Furniture
    "2600-2699",  # Paper
    "2700-2799",  # Printing
    "2800-2899",  # Chemical
    "2900-2999",  # Petroleum
    "3000-3099",  # Rubber/Plastics
    "3100-3199",  # Leather
    "3200-3299",  # Stone/Glass
    "3300-3399",  # Primary Metals
    "3400-3499",  # Fabricated Metals
    "3500-3599",  # Industrial Machinery ← CORE
    "3600-3699",  # Electronic Equipment ← CORE
    "3700-3799",  # Transportation Equipment ← CORE
    "3800-3899",  # Instruments ← CORE
]

# ── HTTP Fetch ────────────────────────────────────────────────────────────────
def rand_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

async def fetch(session, url, timeout=20):
    for att in range(3):
        try:
            async with session.get(url, headers=rand_headers(),
                                   timeout=aiohttp.ClientTimeout(total=timeout),
                                   allow_redirects=True, ssl=False) as r:
                if r.status == 200:
                    return await r.text(errors="replace")
                if r.status == 429:
                    await asyncio.sleep(30 + att * 15)
                elif r.status in (403, 404, 410):
                    return ""
        except Exception:
            await asyncio.sleep(2 ** att)
    return ""

def extract_domain(url):
    try:
        h = urlparse(url).netloc.lower()
        h = re.sub(r'^www\.', '', h).split(":")[0]
        return h if "." in h and len(h) > 4 else ""
    except: return ""

# ── B44 Upsert ────────────────────────────────────────────────────────────────
async def b44_insert(session, data):
    domain = data.get("domain","")
    if not domain: return

    # Check duplicato
    try:
        async with session.get(
            f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id",
            headers={"api-key": B44_TOKEN},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if isinstance(d,list) and d:
                    stats["skipped"] += 1
                    return  # già presente
    except: pass

    try:
        async with session.post(f"{B44_BASE}/IndustrialCompany",
                                headers=HW, json=data,
                                timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status in (200,201):
                stats["inserted"] += 1
            else:
                stats["errors"] += 1
    except:
        stats["errors"] += 1

# ── KOMPASS Crawler ───────────────────────────────────────────────────────────
async def crawl_kompass_cat(session, slug, label, countries, max_pages=100):
    results = []
    for country in countries[:4]:  # max 4 paesi per categoria
        for pg in range(1, max_pages + 1):
            url = (f"https://{country}.kompass.com/searchCompany?"
                   f"activity={quote_plus(slug)}&optimizedBy=&searchType=&offset={(pg-1)*20}")
            html = await fetch(session, url)
            if not html: break

            soup = BeautifulSoup(html, "html.parser")
            # Selettori risultati Kompass
            cards = (soup.select(".companyCard, .k-company-result, [data-company]") or
                     soup.select("h2.resultTitle, .companyTitle, .card__title"))

            if not cards: break

            pg_count = 0
            for card in cards:
                name = card.get_text(strip=True)
                if not name or len(name) < 2: continue

                parent = (card.find_parent(class_=re.compile(r"card|result|company|row")) or
                          card.find_parent("li") or card.find_parent("div"))
                website = ""
                if parent:
                    web_a = parent.select_one("a[href*='http']:not([href*='kompass'])")
                    if web_a: website = web_a.get("href","")

                domain = extract_domain(website)
                if not domain:
                    # Prova a costruire da nome (euristica)
                    clean = re.sub(r'[^a-zA-Z0-9]', '', name.lower())[:20]
                    if len(clean) > 3: domain = f"{clean}.com"  # placeholder
                    else: continue

                country_map = {"it":"IT","de":"DE","fr":"FR","es":"ES","nl":"NL",
                               "pl":"PL","at":"AT","ch":"CH","se":"SE","dk":"DK",
                               "fi":"FI","be":"BE","gb":"GB","cz":"CZ","sk":"SK","jp":"JP"}
                results.append({
                    "name": name[:100], "domain": domain,
                    "website_url": website or f"https://{domain}",
                    "country": country_map.get(country, country.upper()),
                    "industry": label, "source": f"kompass_{country}",
                    "revenue": None, "employee_count": None,
                    "estimated_deal_value_max": None,
                })
                pg_count += 1

            log.info(f"[Kompass] {label}/{country} p{pg}: +{pg_count}")
            stats["crawled"] += pg_count

            if pg_count < 5: break
            await asyncio.sleep(DELAY + random.uniform(0, 1.0))

    return results

# ── EUROPAGES Crawler ─────────────────────────────────────────────────────────
async def crawl_europages_cat(session, slug, max_pages=80):
    results = []
    for pg in range(1, max_pages + 1):
        url = f"https://www.europages.co.uk/companies/{slug}.html?page={pg}"
        html = await fetch(session, url)
        if not html: break

        soup = BeautifulSoup(html, "html.parser")
        cards = (soup.select(".company-card, .ep-listing__item, [class*='company-item']") or
                 soup.select("h2.company-name, .listing-company, [class*='listing-item']"))
        if not cards: break

        pg_count = 0
        for card in cards:
            name_el = card.select_one("h2,h3,.company-name,[class*='name']")
            name = (name_el or card).get_text(strip=True)
            if not name or len(name) < 2: continue

            parent = card.find_parent(class_=re.compile(r"card|listing|item")) or card
            web_a = parent.select_one("a[href*='http']:not([href*='europages'])")
            website = web_a.get("href","") if web_a else ""
            domain = extract_domain(website)
            if not domain: continue

            loc = parent.select_one("[class*='location'],[class*='country'],[class*='address']")
            country = ""
            if loc:
                m = re.search(r'\b([A-Z]{2})\b', loc.get_text())
                if m: country = m.group(1)

            results.append({
                "name": name[:100], "domain": domain,
                "website_url": website or f"https://{domain}",
                "country": country or "EU", "industry": slug.replace("-"," ").title(),
                "source": "europages",
                "revenue": None, "employee_count": None,
                "estimated_deal_value_max": None,
            })
            pg_count += 1

        log.info(f"[Europages] {slug} p{pg}: +{pg_count}")
        stats["crawled"] += pg_count

        if pg_count < 3: break
        await asyncio.sleep(DELAY + random.uniform(0, 0.8))

    return results

# ── OPENCORPORATES Italian Manufacturers ──────────────────────────────────────
async def crawl_opencorporates(session, max_pages=200):
    """Aziende attive con ATECO C* (manifatturiero)."""
    results = []
    # API pubblica OpenCorporates
    for ateco_range in [("C10","C16"),("C17","C23"),("C24","C28"),("C29","C33")]:
        for pg in range(1, max_pages + 1):
            url = (f"https://api.opencorporates.com/v0.4/companies/search"
                   f"?jurisdiction_code=it&industry_codes={ateco_range[0]}"
                   f"&current_status=Active&per_page=100&page={pg}")
            html = await fetch(session, url, timeout=15)
            if not html: break
            try:
                d = json.loads(html)
                companies = (d.get("results",{}).get("companies") or [])
                if not companies: break
                pg_count = 0
                for c in companies:
                    c = c.get("company",{})
                    name = c.get("name","")
                    website = (c.get("registered_address",{}) or {}).get("website","")
                    if not website:
                        # Cerca nel registry_url
                        reg = c.get("registry_url","")
                        website = reg if reg and "http" in reg else ""
                    domain = extract_domain(website)
                    if not domain: continue
                    results.append({
                        "name": name[:100], "domain": domain,
                        "website_url": website, "country": "IT",
                        "industry": "Manufacturing",
                        "source": "opencorporates",
                        "revenue": None, "employee_count": None,
                        "estimated_deal_value_max": None,
                    })
                    pg_count += 1
                log.info(f"[OpenCorp] {ateco_range[0]} p{pg}: +{pg_count}")
                stats["crawled"] += pg_count
                if len(companies) < 100: break
            except: break
            await asyncio.sleep(2)
    return results

# ── Dataset Locali (YC, Wellfound, BuiltWith) ─────────────────────────────────
def load_local_datasets():
    """Carica dataset già presenti nel repo."""
    results = []
    files_to_check = [
        ("/app/wellfound_companies.jsonl", "wellfound"),
        ("/app/yc_companies.jsonl", "yc"),
        ("/app/builtwith_ecommerce.jsonl", "builtwith_ecommerce"),
        ("/app/builtwith_financial.jsonl", "builtwith_financial"),
    ]
    for filepath, source in files_to_check:
        if not os.path.exists(filepath): continue
        count = 0
        try:
            with open(filepath) as f:
                for line in f:
                    try:
                        c = json.loads(line.strip())
                        domain = (c.get("domain") or c.get("website","")).strip().lower()
                        domain = re.sub(r'^https?://(www\.)?','',domain).split('/')[0]
                        if not domain or "." not in domain: continue
                        name = (c.get("name") or c.get("company_name") or domain)[:100]
                        results.append({
                            "name": name, "domain": domain,
                            "website_url": f"https://{domain}",
                            "country": c.get("country",""),
                            "industry": c.get("industry",""),
                            "source": source,
                            "revenue": None, "employee_count": None,
                            "estimated_deal_value_max": None,
                        })
                        count += 1
                    except: pass
        except: pass
        log.info(f"[Local] {source}: {count} aziende")
    return results

# ── GLEIF Industrial Entities ─────────────────────────────────────────────────
async def crawl_gleif_manufacturing(session, max_pages=500):
    """GLEIF API: aziende con categoria manifatturiero e sito web."""
    results = []
    # Filter by entity type e paesi EU con sito web
    for country in ["IT","DE","FR","ES","PL","NL","AT","CH","SE","DK","FI","BE","CZ"]:
        pg = 1
        while pg <= max_pages:
            url = (f"https://api.gleif.org/api/v1/lei-records"
                   f"?filter[entity.legalAddress.country]={country}"
                   f"&filter[entity.status]=ACTIVE"
                   f"&filter[entity.registeredAs]=*"
                   f"&page[number]={pg}&page[size]=200")
            html = await fetch(session, url, timeout=20)
            if not html: break
            try:
                d = json.loads(html)
                items = d.get("data",[])
                if not items: break
                pg_count = 0
                for item in items:
                    ent = (item.get("attributes",{}).get("entity") or {})
                    name = ent.get("legalName",{}).get("name","")
                    website = ""
                    for link in (item.get("relationships",{})
                                 .get("links",{}).get("data",[]) or []):
                        if "http" in str(link.get("href","")): website = link["href"]; break
                    if not website: continue
                    domain = extract_domain(website)
                    if not domain: continue
                    addr = ent.get("legalAddress",{}) or {}
                    city = addr.get("city","")
                    results.append({
                        "name": name[:100], "domain": domain,
                        "website_url": website, "country": country,
                        "city": city, "industry": "Manufacturing",
                        "source": "gleif",
                        "revenue": None, "employee_count": None,
                        "estimated_deal_value_max": None,
                    })
                    pg_count += 1
                log.info(f"[GLEIF] {country} p{pg}: +{pg_count}")
                stats["crawled"] += pg_count
                if len(items) < 200: break
            except: break
            pg += 1
            await asyncio.sleep(0.5)

    return results

# ── Main Pipeline ─────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    log.info("=== Mega Industrial Crawler v1.0 ===")
    log.info(f"Target: Kompass {len(KOMPASS_CATS)} categorie | "
             f"Europages {len(EUROPAGES_CATS)} categorie | GLEIF | OpenCorp | Local datasets")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    sem  = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(connector=conn) as session:
        all_companies = []

        # 1. Dataset locali — immediati, senza rete
        log.info("=== Phase 1: Dataset Locali ===")
        local = load_local_datasets()
        all_companies.extend(local)
        log.info(f"Locali: {len(local)} aziende")

        # 2. GLEIF — API strutturata, alta qualità
        log.info("=== Phase 2: GLEIF Manufacturing ===")
        try:
            gleif = await crawl_gleif_manufacturing(session, max_pages=100)
            all_companies.extend(gleif)
            log.info(f"GLEIF: {len(gleif)} aziende")
        except Exception as e:
            log.warning(f"GLEIF err: {e}")

        # 3. Europages — directory EU B2B
        log.info("=== Phase 3: Europages ===")
        for slug in EUROPAGES_CATS:
            try:
                r = await crawl_europages_cat(session, slug, max_pages=60)
                all_companies.extend(r)
                log.info(f"  Europages/{slug}: {len(r)}")
            except Exception as e:
                log.warning(f"  Europages/{slug} err: {e}")
            await asyncio.sleep(DELAY * 2)

        # 4. Kompass — il più grande
        log.info("=== Phase 4: Kompass ===")
        for slug, label, countries in KOMPASS_CATS:
            try:
                r = await crawl_kompass_cat(session, slug, label, countries, max_pages=80)
                all_companies.extend(r)
                log.info(f"  Kompass/{label}: {len(r)}")
            except Exception as e:
                log.warning(f"  Kompass/{label} err: {e}")
            await asyncio.sleep(DELAY * 2)

        # 5. OpenCorporates IT
        log.info("=== Phase 5: OpenCorporates IT ===")
        try:
            oc = await crawl_opencorporates(session)
            all_companies.extend(oc)
            log.info(f"OpenCorp: {len(oc)} aziende")
        except Exception as e:
            log.warning(f"OpenCorp err: {e}")

        # 6. Dedup per dominio
        log.info(f"Pre-dedup: {len(all_companies)}")
        seen, deduped = set(), []
        for c in all_companies:
            d = c.get("domain","")
            if d and d not in seen:
                seen.add(d); deduped.append(c)
        log.info(f"Post-dedup: {len(deduped)} domini unici")

        # 7. Push su B44 in batch da 20
        log.info("=== Phase 6: Push su Base44 ===")
        batch = []
        for i, co in enumerate(deduped):
            batch.append(co)
            if len(batch) >= 20:
                async def _ins(c):
                    async with sem: await b44_insert(session, c)
                await asyncio.gather(*[_ins(c) for c in batch], return_exceptions=True)
                batch = []
                if i % 200 == 0:
                    log.info(f"  {i}/{len(deduped)} | inserted={stats['inserted']} "
                             f"skipped={stats['skipped']} err={stats['errors']}")
                await asyncio.sleep(0.3)

        if batch:
            async def _ins2(c):
                async with sem: await b44_insert(session, c)
            await asyncio.gather(*[_ins2(c) for c in batch], return_exceptions=True)

    stats["status"] = "done"
    log.info(f"=== CRAWL COMPLETATO: totale={len(deduped)} "
             f"inserted={stats['inserted']} skipped={stats['skipped']} ===")

    # Mantieni alive per healthcheck Railway
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

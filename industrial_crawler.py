#!/usr/bin/env python3
"""
Industrial Company Crawler v1.0
Fonti: Kompass, Europages, Thomasnet, Made-in-Italy, German Mittelstand databases
Target: 10.000+ aziende manifatturiere → Base44 IndustrialCompany
"""

import asyncio
import aiohttp
import asyncpg
import os
import json
import re
import logging
import time
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin, urlencode, quote_plus
from http.server import HTTPServer, BaseHTTPRequestHandler
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CRAWLER] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
B44_TOKEN    = os.environ.get("B44_SERVICE_TOKEN") or os.environ.get("BASE44_SERVICE_TOKEN") or ""
APP_ID       = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
B44_BASE     = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW           = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT         = int(os.environ.get("PORT", 8080))
WORKER_ID    = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
CONCURRENCY  = int(os.environ.get("CONCURRENCY", "4"))
MAX_PAGES    = int(os.environ.get("MAX_PAGES_PER_SOURCE", "200"))  # pagine per fonte
DELAY        = float(os.environ.get("REQUEST_DELAY", "2.0"))       # gentile con i server

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "no-cache",
}

stats = {
    "crawled": 0, "inserted": 0, "skipped": 0,
    "errors": 0, "status": "starting",
    "kompass": 0, "europages": 0, "thomasnet": 0, "others": 0,
}

# ─── HEALTHCHECK ──────────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True
).start()

# ─── INDUSTRY CATEGORIES TO CRAWL ────────────────────────────────────────────
# (kompass_code, europages_cat, label, country_targets)
INDUSTRY_TARGETS = [
    # Metalworking / Sheet metal
    ("B04", "sheet-metal-work",          "Sheet Metal / Stamping",     ["it","de","es","fr","pl"]),
    ("B05", "metalworking",               "Metalworking",               ["it","de","at","ch","pl"]),
    ("B09", "machine-tools",              "Machine Tools",              ["it","de","ch","es","jp"]),
    ("B10", "cutting-tools",              "Cutting Tools",              ["it","de","se","us"]),
    # Packaging
    ("H10", "packaging-machinery",        "Packaging Machinery",        ["it","de","fr","ch","es"]),
    ("H11", "food-packaging",             "Food & Bev Packaging",       ["it","de","fr","nl","dk"]),
    # Robotics / Automation
    ("B06", "industrial-robots",          "Industrial Robots",          ["it","de","jp","se","dk"]),
    ("B07", "automation-equipment",       "Automation Equipment",       ["it","de","at","ch","fr"]),
    ("B08", "conveying-equipment",        "Conveyors & Material Handling",["it","de","nl","be","pl"]),
    # Warehouse / Logistics
    ("H08", "warehouse-equipment",        "Warehouse Equipment",        ["it","de","fr","nl","es"]),
    ("H09", "shelving-storage",           "Storage Systems",            ["it","de","fr","es","pl"]),
    # MES / Industry 4.0
    ("I05", "industrial-software",        "Industrial Software / MES",  ["it","de","fr","gb","nl"]),
    ("I06", "control-systems",            "Control Systems / PLC",      ["it","de","at","ch","se"]),
    # Food / Pharma Manufacturing
    ("C02", "food-processing-machinery",  "Food Processing Machinery",  ["it","de","dk","nl","fr"]),
    ("C03", "pharmaceutical-machinery",   "Pharma Machinery",           ["it","de","ch","fr","gb"]),
    # Plastics
    ("D04", "plastics-machinery",         "Plastics Machinery",         ["it","de","at","fr","es"]),
    # Automotive Components
    ("A01", "automotive-components",      "Automotive Components",      ["it","de","pl","cz","sk"]),
    ("A02", "automotive-suppliers",       "Automotive Tier 1-2",        ["it","de","fr","es","pl"]),
    # Electronics Manufacturing
    ("E01", "electronic-components",      "Electronics Manufacturing",  ["de","nl","se","fi","cz"]),
    # Steel / Metal production
    ("F01", "steel-production",           "Steel / Metal Production",   ["it","de","at","se","fi"]),
]

# ─── HTTP UTILS ───────────────────────────────────────────────────────────────
async def fetch(session, url, extra_headers=None, timeout=20):
    h = dict(HEADERS)
    if extra_headers:
        h.update(extra_headers)
    for att in range(3):
        try:
            async with session.get(
                url, headers=h,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True, ssl=False
            ) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    if "text" in ct or "html" in ct or "json" in ct:
                        return await r.text(errors="replace"), r.status
                elif r.status == 429:
                    wait = int(r.headers.get("Retry-After", "30"))
                    log.info(f"Rate limited {url} — waiting {wait}s")
                    await asyncio.sleep(wait)
                elif r.status in (403, 404):
                    return "", r.status
                return "", r.status
        except asyncio.TimeoutError:
            log.debug(f"Timeout att={att}: {url}")
            await asyncio.sleep(2 ** att)
        except Exception as e:
            log.debug(f"Err att={att}: {e}")
            await asyncio.sleep(2 ** att)
    return "", 0


def extract_domain(url):
    """Estrae dominio pulito da URL."""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        host = p.netloc.lower()
        host = re.sub(r'^www\.', '', host)
        return host.split(":")[0]
    except Exception:
        return ""


def clean_text(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style"]): t.decompose()
        return " ".join(soup.get_text(" ").split())[:5000]
    except Exception:
        return html[:2000]


# ─── BASE44 API ───────────────────────────────────────────────────────────────
async def b44_upsert_company(session, data: dict) -> str:
    """Upsert IndustrialCompany per dominio — POST se nuovo, skip se esiste."""
    domain = data.get("domain", "")
    if not domain:
        return ""

    # Check esistenza
    url = f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                existing = await r.json(content_type=None)
                if isinstance(existing, list) and existing:
                    stats["skipped"] += 1
                    return existing[0]["id"]
    except Exception:
        pass

    # POST nuovo
    try:
        async with session.post(f"{B44_BASE}/IndustrialCompany", headers=HW,
                                json=data, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status in (200, 201):
                d = await r.json(content_type=None)
                eid = d.get("id", "") if isinstance(d, dict) else ""
                if eid:
                    stats["inserted"] += 1
                return eid
    except Exception as e:
        log.debug(f"b44_upsert {domain}: {e}")
    stats["errors"] += 1
    return ""


# ─── KOMPASS CRAWLER ──────────────────────────────────────────────────────────
# Kompass.com è un directory B2B globale con ~30M aziende
# URL pattern: https://it.kompass.com/c/{category}/{country}/

async def crawl_kompass(session, cat_code, category_slug, label, countries, max_pages=MAX_PAGES):
    """Crawla la directory Kompass per categoria e paesi."""
    results = []

    for country in countries:
        base_url = f"https://{country}.kompass.com/c/{category_slug}/"
        log.info(f"[Kompass] {label} / {country} → {base_url}")

        for page_n in range(1, max_pages + 1):
            url = base_url if page_n == 1 else f"{base_url}?page={page_n}"
            html, status = await fetch(session, url)
            if not html or status in (403, 404):
                break

            soup = BeautifulSoup(html, "html.parser")

            # Estrai card aziende Kompass
            cards = soup.select(".company-name, .k-company-item, [data-company-name]")
            if not cards:
                # Prova selettori alternativi
                cards = soup.select("h2.company-name, .card-title, .listing-company-name")
            if not cards:
                break

            page_count = 0
            for card in cards:
                try:
                    # Nome azienda
                    name = card.get_text(strip=True)
                    if not name or len(name) < 2:
                        continue

                    # Link azienda
                    link = card.find_parent("a") or card.find("a")
                    if not link:
                        link = card.closest("a") if hasattr(card, "closest") else None
                    href = link.get("href", "") if link else ""

                    # Estrai dettagli dalla card parent
                    parent = card.find_parent(class_=re.compile(r"company|listing|card"))
                    city    = ""
                    website = ""
                    if parent:
                        city_el = parent.select_one(".city, .location, .address")
                        city = city_el.get_text(strip=True) if city_el else ""
                        web_el = parent.select_one("a[href*='http']:not([href*='kompass'])")
                        if web_el:
                            website = web_el.get("href", "")

                    # Deriva dominio dal website o dal link Kompass
                    domain = extract_domain(website) if website else ""
                    if not domain and href:
                        # Cerca il dominio nell'URL del profilo Kompass
                        m = re.search(r'website[=/]([a-z0-9\-\.]+\.[a-z]{2,6})', href, re.I)
                        if m:
                            domain = m.group(1)

                    if not domain:
                        continue

                    # Normalizza country
                    country_map = {
                        "it": "IT", "de": "DE", "fr": "FR", "es": "ES",
                        "nl": "NL", "pl": "PL", "at": "AT", "ch": "CH",
                        "se": "SE", "dk": "DK", "fi": "FI", "be": "BE",
                        "gb": "GB", "cz": "CZ", "sk": "SK", "jp": "JP", "us": "US",
                    }

                    results.append({
                        "name":    name[:100],
                        "domain":  domain,
                        "website_url": website or f"https://{domain}",
                        "country": country_map.get(country, country.upper()),
                        "city":    city[:80],
                        "industry": label,
                        "scan_status": "pending",
                        "source": f"kompass_{country}",
                    })
                    page_count += 1
                except Exception:
                    continue

            stats["kompass"] += page_count
            stats["crawled"] += page_count
            log.info(f"  [Kompass] {country} p{page_n}: +{page_count} aziende")

            if page_count < 5:
                break  # fine risultati

            await asyncio.sleep(DELAY)

    return results


# ─── EUROPAGES CRAWLER ────────────────────────────────────────────────────────
# Europages.com — 3M aziende europee B2B
# URL: https://www.europages.co.uk/companies/{category}.html?page={n}

async def crawl_europages(session, cat_code, category_slug, label, countries, max_pages=MAX_PAGES):
    results = []

    # Europages usa slug in inglese
    base = f"https://www.europages.co.uk/companies/{category_slug}.html"
    log.info(f"[Europages] {label} → {base}")

    for page_n in range(1, max_pages + 1):
        url = base if page_n == 1 else f"{base}?page={page_n}"
        html, status = await fetch(session, url, extra_headers={"Referer": "https://www.europages.co.uk/"})
        if not html or status in (403, 404):
            break

        soup = BeautifulSoup(html, "html.parser")

        # Selettori Europages
        cards = soup.select(".company-name, .ep-company-item__name, h2.company-item__title, .listing__name")
        if not cards:
            cards = soup.select("[class*='company-name'], [class*='company-item']")
        if not cards:
            break

        page_count = 0
        for card in cards:
            try:
                name = card.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                parent = (card.find_parent(class_=re.compile(r"company|listing|item")) or
                          card.find_parent("li") or card.find_parent("div"))

                city = country_code = website = domain = ""

                if parent:
                    # Cerca indirizzo
                    loc = parent.select_one("[class*='location'], [class*='address'], [class*='country']")
                    if loc:
                        loc_text = loc.get_text(strip=True)
                        # Pattern: "City, COUNTRY"
                        m = re.search(r'([A-Z]{2})\s*$', loc_text)
                        if m:
                            country_code = m.group(1)
                            city = loc_text[:m.start()].strip().rstrip(",").strip()

                    # Cerca website
                    web = parent.select_one("a[href*='http']:not([href*='europages'])")
                    if web:
                        website = web.get("href", "")
                        domain  = extract_domain(website)

                # Filtra per paesi target se specificato
                if country_code and countries and country_code.lower() not in countries:
                    continue

                if not domain:
                    continue

                results.append({
                    "name":     name[:100],
                    "domain":   domain,
                    "website_url": website or f"https://{domain}",
                    "country":  country_code or "EU",
                    "city":     city[:80],
                    "industry": label,
                    "scan_status": "pending",
                    "source": "europages",
                })
                page_count += 1

            except Exception:
                continue

        stats["europages"] += page_count
        stats["crawled"]   += page_count
        log.info(f"  [Europages] {label} p{page_n}: +{page_count}")

        if page_count < 3:
            break

        await asyncio.sleep(DELAY)

    return results


# ─── THOMASNET CRAWLER ────────────────────────────────────────────────────────
# ThomasNet — ~500k aziende manifatturiere US + globali
# URL: https://www.thomasnet.com/nsearch.html?what={keyword}&pg={n}

THOMASNET_KEYWORDS = [
    "industrial+automation", "cnc+machining", "robotic+welding",
    "palletizing+systems", "conveyor+systems", "warehouse+automation",
    "machine+vision+systems", "plc+systems", "mes+software",
    "packaging+machinery", "material+handling", "precision+machining",
    "sheet+metal+fabrication", "industrial+robots",
]

async def crawl_thomasnet(session, max_pages=50):
    results = []

    for keyword in THOMASNET_KEYWORDS[:8]:  # 8 keyword × 50 pagine = 400 pagine max
        log.info(f"[ThomasNet] keyword: {keyword}")
        for page_n in range(1, max_pages + 1):
            url = (f"https://www.thomasnet.com/nsearch.html?"
                   f"what={keyword}&pg={page_n}&cov=NA")
            html, status = await fetch(session, url, timeout=15)
            if not html or status in (403, 404):
                break

            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select(".profile-card, .supplier-card, [data-supplier]")
            if not cards:
                cards = soup.select("h2.profile-card__title, .listing-profile")
            if not cards:
                break

            page_count = 0
            for card in cards:
                try:
                    name_el = card.select_one("h2, h3, .company-name, .profile-card__title")
                    name = name_el.get_text(strip=True) if name_el else ""
                    if not name:
                        continue

                    web_el = card.select_one("a[href*='http']:not([href*='thomasnet'])")
                    website = web_el.get("href", "") if web_el else ""
                    domain  = extract_domain(website)
                    if not domain:
                        continue

                    loc_el = card.select_one(".location, .address, .city-state")
                    location = loc_el.get_text(strip=True) if loc_el else ""

                    results.append({
                        "name":     name[:100],
                        "domain":   domain,
                        "website_url": website or f"https://{domain}",
                        "country":  "US",
                        "city":     location[:80],
                        "industry": keyword.replace("+", " ").title(),
                        "scan_status": "pending",
                        "source": "thomasnet",
                    })
                    page_count += 1
                except Exception:
                    continue

            stats["thomasnet"] += page_count
            stats["crawled"]   += page_count
            log.info(f"  [ThomasNet] {keyword} p{page_n}: +{page_count}")

            if page_count < 3:
                break
            await asyncio.sleep(DELAY)

    return results


# ─── CONFINDUSTRIA / ATECO CRAWLER ────────────────────────────────────────────
# Cerca su registri camerali IT (ATECO manifatturiero C10-C33)
# e su directory pubbliche italiane

ITALIAN_DIRS = [
    # Portali camerali / associazioni di categoria
    ("https://www.ucimu.it/aziende-associate/", "machine_tools", "IT"),
    ("https://www.assofoodtec.it/associati/",   "food_machinery", "IT"),
    ("https://www.ucima.it/aziende-associate/", "packaging",     "IT"),
    ("https://www.acimall.com/aziende/",        "woodworking",   "IT"),
    ("https://www.acimac.it/aziende-associate/","ceramics",      "IT"),
    ("https://www.anima.it/associati/",         "industrial",    "IT"),
    ("https://www.federmeccanica.it/",          "metalworking",  "IT"),
]

async def crawl_italian_dirs(session):
    results = []
    for url, industry, country in ITALIAN_DIRS:
        log.info(f"[IT-Dir] {url}")
        html, status = await fetch(session, url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Cerca link aziende nella pagina
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            name = link.get_text(strip=True)

            # Filtra link aziendali (esclude navigazione, social, ecc.)
            if (name and 3 < len(name) < 80 and
                href and "http" in href and
                not any(x in href for x in ["facebook", "twitter", "linkedin",
                                             "youtube", "instagram", "#", "mailto"])):
                domain = extract_domain(href)
                if domain and len(domain) > 4 and "." in domain:
                    results.append({
                        "name":     name[:100],
                        "domain":   domain,
                        "website_url": href,
                        "country":  country,
                        "industry": industry,
                        "scan_status": "pending",
                        "source": "it_directory",
                    })
                    stats["others"] += 1
                    stats["crawled"] += 1

        await asyncio.sleep(DELAY)

    return results


# ─── GERMAN MITTELSTAND DIRECTORIES ──────────────────────────────────────────
GERMAN_DIRS = [
    ("https://www.vdma.org/mitglieder",          "machinery",        "DE"),
    ("https://www.zvei.org/mitglieder/",         "electrical",       "DE"),
    ("https://www.vda.de/en/association/members","automotive",       "DE"),
    ("https://www.wgv.de/mitglieder/",           "metalworking",     "DE"),
    ("https://www.bvmw.de/",                     "sme_industrial",   "DE"),
]

async def crawl_german_dirs(session):
    results = []
    for url, industry, country in GERMAN_DIRS:
        log.info(f"[DE-Dir] {url}")
        html, status = await fetch(session, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if (name and 3 < len(name) < 80 and href and "http" in href and
                not any(x in href for x in ["vdma.org","zvei.org","vda.de","bvmw.de",
                                             "facebook","twitter","linkedin","#","mailto"])):
                domain = extract_domain(href)
                if domain and "." in domain:
                    results.append({
                        "name": name[:100], "domain": domain,
                        "website_url": href, "country": country,
                        "industry": industry, "scan_status": "pending",
                        "source": "de_directory",
                    })
                    stats["others"] += 1
                    stats["crawled"] += 1
        await asyncio.sleep(DELAY)
    return results


# ─── GOOGLE MAPS / PLACES ENRICHMENT ─────────────────────────────────────────
# Per ogni azienda trovata senza employee_count, usa una query DDG per enrichment
async def enrich_company(session, domain: str, name: str) -> dict:
    """Cerca employee count e city via DuckDuckGo."""
    try:
        query = quote_plus(f"{name} site:{domain} employees headquarters")
        url = f"https://html.duckduckgo.com/html/?q={query}"
        html, _ = await fetch(session, url, timeout=8)
        if not html:
            return {}
        text = clean_text(html)

        # Cerca employee count
        emp = 0
        m = re.search(r'(\d[\d,\.]+)\s*(employees|dipendenti|Mitarbeiter|staff)', text, re.I)
        if m:
            emp_str = m.group(1).replace(",", "").replace(".", "")
            try:
                emp = int(emp_str)
            except Exception:
                pass

        # Cerca country
        country = ""
        for code, pattern in [("IT", r'\bItaly\b|\bItalia\b|\bItalien\b'),
                               ("DE", r'\bGermany\b|\bDeutschland\b'),
                               ("FR", r'\bFrance\b|\bFrancia\b'),
                               ("ES", r'\bSpain\b|\bSpagna\b|\bEspana\b'),
                               ("GB", r'\bUnited Kingdom\b|\bUK\b'),
                               ("US", r'\bUnited States\b|\bUSA\b')]:
            if re.search(pattern, text, re.I):
                country = code
                break

        result = {}
        if emp and 5 <= emp <= 500000:
            result["employee_count"] = emp
            sz = ("micro" if emp < 10 else "small" if emp < 50 else
                  "medium" if emp < 250 else "large" if emp < 1000 else "enterprise")
            result["company_size"] = sz
        if country:
            result["country"] = country

        return result
    except Exception:
        return {}


# ─── MAIN PIPELINE ────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    log.info(f"=== Industrial Crawler v1.0 | Worker {WORKER_ID}/{TOTAL_WORKERS} ===")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    sem  = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(connector=conn) as session:

        all_companies = []

        # 1. Crawl Kompass per ogni categoria
        log.info("=== Phase 1: Kompass ===")
        my_targets = [t for i, t in enumerate(INDUSTRY_TARGETS) if i % TOTAL_WORKERS == WORKER_ID]

        for cat_code, slug, label, countries in my_targets:
            try:
                results = await crawl_kompass(session, cat_code, slug, label,
                                              countries[:3], max_pages=min(MAX_PAGES, 50))
                all_companies.extend(results)
                log.info(f"[Kompass] {label}: {len(results)} aziende")
            except Exception as e:
                log.warning(f"[Kompass] {label} ERR: {e}")
            await asyncio.sleep(DELAY * 2)

        # 2. Crawl Europages
        log.info("=== Phase 2: Europages ===")
        for cat_code, slug, label, countries in my_targets[:10]:  # prime 10 cat
            try:
                results = await crawl_europages(session, cat_code, slug, label,
                                                countries[:3], max_pages=min(MAX_PAGES, 30))
                all_companies.extend(results)
                log.info(f"[Europages] {label}: {len(results)} aziende")
            except Exception as e:
                log.warning(f"[Europages] {label} ERR: {e}")
            await asyncio.sleep(DELAY * 2)

        # 3. ThomasNet (solo worker 0)
        if WORKER_ID == 0:
            log.info("=== Phase 3: ThomasNet ===")
            try:
                results = await crawl_thomasnet(session, max_pages=30)
                all_companies.extend(results)
                log.info(f"[ThomasNet] totale: {len(results)} aziende")
            except Exception as e:
                log.warning(f"[ThomasNet] ERR: {e}")

        # 4. Directory IT e DE (solo worker 0)
        if WORKER_ID == 0:
            log.info("=== Phase 4: IT/DE Directories ===")
            try:
                it_res = await crawl_italian_dirs(session)
                de_res = await crawl_german_dirs(session)
                all_companies.extend(it_res + de_res)
                log.info(f"[Dirs] IT={len(it_res)} DE={len(de_res)}")
            except Exception as e:
                log.warning(f"[Dirs] ERR: {e}")

        # 5. Dedup per dominio
        log.info(f"Pre-dedup: {len(all_companies)} aziende")
        seen, deduped = set(), []
        for c in all_companies:
            d = c.get("domain", "")
            if d and d not in seen:
                seen.add(d)
                deduped.append(c)
        log.info(f"Post-dedup: {len(deduped)} aziende uniche")

        # 6. Push su Base44
        log.info("=== Phase 5: Push su Base44 ===")
        batch = []
        for i, company in enumerate(deduped):
            batch.append(company)
            if len(batch) >= 20:
                tasks = [b44_upsert_company(session, c) for c in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                batch = []
                log.info(f"  Pushed {min(i+1, len(deduped))}/{len(deduped)} | "
                         f"inserted={stats['inserted']} skipped={stats['skipped']}")
                await asyncio.sleep(0.5)

        if batch:
            tasks = [b44_upsert_company(session, c) for c in batch]
            await asyncio.gather(*tasks, return_exceptions=True)

    stats["status"] = "done"
    log.info(f"=== CRAWLER COMPLETATO: crawled={stats['crawled']} "
             f"inserted={stats['inserted']} skipped={stats['skipped']} "
             f"errors={stats['errors']} ===")

    # Keep alive per healthcheck Railway
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())

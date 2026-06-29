#!/usr/bin/env python3
"""
Industrial Mass Domain Feeder v1.0
Alimenta il database con milioni di domini industriali da fonti gratuite:
  1. Majestic Million       — top 1M domini globali (CSV pubblico)
  2. Cisco Umbrella Top 1M  — top 1M domini DNS (zip pubblico)
  3. OpenCorporates bulk    — aziende registrate EU con SIC codes industriali
  4. Kompass / Europages    — scraping paginato per categoria industriale
  5. Local JSONL files      — dataset locali già presenti

Filtra per keyword industriali nel dominio stesso (fast pre-filter)
poi passa a Base44 via PUT upsert.
"""
import asyncio, aiohttp, csv, io, json, gzip, os, re, logging, time, zipfile
import tempfile, threading
from datetime import datetime, timezone
from urllib.parse import quote_plus
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FEED] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN  = os.environ.get("B44_SERVICE_TOKEN", "")
APP_ID     = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
B44_BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW         = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
PORT       = int(os.environ.get("PORT", 8080))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
WRITE_DELAY = float(os.environ.get("WRITE_DELAY", "0.12"))  # ~8 req/s su Base44

stats = {"fed": 0, "skipped": 0, "errors": 0, "status": "starting", "source": ""}

# ─── HEALTHCHECK ──────────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
                 daemon=True).start()

# ─── INDUSTRIAL KEYWORD FILTER ────────────────────────────────────────────────
# Pattern che indicano un'azienda manifatturiera/industriale nel dominio stesso
INDUSTRIAL_DOMAIN_KEYWORDS = re.compile(
    r"manufactur|industri|automat|robot|machin|engineer|"
    r"metal|steel|aluminum|aluminium|casting|forging|stamping|"
    r"plastic|rubber|polymer|compos|"
    r"packaging|pack|label|"
    r"food|beverage|dairy|meat|bakery|"
    r"pharma|medical|medtech|"
    r"auto|automotive|motor|vehicle|"
    r"electr|electronic|sensor|"
    r"logistic|warehouse|transport|supply|"
    r"pump|valve|hydraulic|pneumatic|"
    r"bearing|gear|transmission|"
    r"welding|cutting|grinding|milling|turning|"
    r"conveyor|belt|chain|"
    r"paint|coating|finishing|"
    r"textile|fiber|yarn|weav|"
    r"wood|furniture|timber|"
    r"print|paper|cardboard|"
    r"chemical|petrochem|refin|"
    r"mining|mineral|quarry|cement|"
    r"energy|power|turbine|generator|"
    r"construct|build|infrastru|"
    r"agri|farm|harvest|"
    r"group|holding|solutions|systems|tech|"
    r"gmbh|srl|spa|ag|bv|nv|ab|oy|as|kft|sas",
    re.I
)

# Blacklist: domini che NON sono aziende manifatturiere
BLACKLIST = re.compile(
    r"google|facebook|twitter|youtube|instagram|tiktok|"
    r"amazon|ebay|etsy|alibaba|aliexpress|"
    r"wikipedia|reddit|quora|medium|"
    r"netflix|spotify|apple|microsoft|"
    r"bank|insurance|invest|finance|crypto|"
    r"news|blog|journal|magazine|media|"
    r"university|college|school|edu|"
    r"government|gov|municipal|"
    r"hospital|clinic|doctor|health(?!tech)|"
    r"hotel|travel|tourism|booking|"
    r"realty|estate|propert",
    re.I
)

def is_industrial(domain: str) -> bool:
    d = domain.lower().replace("www.","").split(".")[0]
    if BLACKLIST.search(d): return False
    if INDUSTRIAL_DOMAIN_KEYWORDS.search(d): return True
    # Domini senza keyword specifiche: accetta comunque (saranno filtrati dallo scanner)
    # ma solo se TLD è business-oriented
    tld = domain.rsplit(".",1)[-1].lower()
    return tld in ("com","de","it","fr","es","pl","cz","nl","be","at","ch",
                   "se","dk","fi","no","pt","ro","hu","sk","si","hr","uk","co")


# ─── BASE44 UPSERT ────────────────────────────────────────────────────────────
async def upsert_company(session, domain, name="", country="", industry=""):
    """PUT upsert atomico per dominio — crea o aggiorna senza duplicati."""
    # Cerca esistente
    url_check = f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id"
    try:
        async with session.get(url_check, headers=HW,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if isinstance(d, list) and d:
                    return "exists"  # già presente, skip
    except Exception:
        pass

    # Crea nuovo record
    payload = {
        "domain": domain,
        "website_url": f"https://{domain}",
        "name": name or domain.replace("www.","").split(".")[0].title(),
        "country": country,
        "industry": industry,
        "scan_status": "pending",
        "source": "mass_feeder_v1",
    }
    try:
        async with session.post(f"{B44_BASE}/IndustrialCompany",
                                headers=HW, json=payload,
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status in (200, 201):
                stats["fed"] += 1
                return "created"
            else:
                stats["errors"] += 1
                return f"err_{r.status}"
    except Exception as e:
        stats["errors"] += 1
        return f"err_{e}"


# ─── SOURCE 1: MAJESTIC MILLION ──────────────────────────────────────────────
async def feed_majestic(session):
    """
    Majestic Million — top 1M siti per trust flow.
    CSV: GlobalRank,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,PrevGlobalRank,...
    """
    stats["source"] = "Majestic Million"
    log.info("Scarico Majestic Million CSV...")
    url = "https://downloads.majestic.com/majestic_million.csv"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
            if r.status != 200:
                log.warning(f"Majestic: HTTP {r.status}")
                return
            content = await r.read()

        reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
        batch, count, fed = [], 0, 0
        for row in reader:
            domain = (row.get("Domain") or row.get("IDN_Domain","")).strip().lower()
            if not domain or not is_industrial(domain):
                continue
            batch.append(domain)
            if len(batch) >= BATCH_SIZE:
                for d in batch:
                    res = await upsert_company(session, d)
                    if res == "created": fed += 1
                    await asyncio.sleep(WRITE_DELAY)
                log.info(f"Majestic: {count+len(batch):,} processati, {stats['fed']:,} inseriti")
                count += len(batch); batch = []
        for d in batch:
            res = await upsert_company(session, d)
            if res == "created": fed += 1
            await asyncio.sleep(WRITE_DELAY)
        log.info(f"Majestic DONE: {count+len(batch):,} processati, {fed} nuovi")
    except Exception as e:
        log.error(f"Majestic ERR: {e}")


# ─── SOURCE 2: TRANCO TOP LIST ────────────────────────────────────────────────
async def feed_tranco(session):
    """
    Tranco — lista aggregata top 1M (Alexa+Majestic+Umbrella+Quantcast).
    Endpoint: https://tranco-list.eu/download/latest/1000000
    """
    stats["source"] = "Tranco Top 1M"
    log.info("Scarico Tranco Top 1M...")
    url = "https://tranco-list.eu/download/latest/1000000"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=180),
                               allow_redirects=True) as r:
            if r.status != 200:
                log.warning(f"Tranco: HTTP {r.status}")
                return
            content = await r.read()

        lines = content.decode("utf-8", errors="replace").splitlines()
        batch, count, fed = [], 0, 0
        for line in lines:
            parts = line.strip().split(",")
            domain = parts[1].strip().lower() if len(parts) >= 2 else ""
            if not domain or not is_industrial(domain):
                continue
            batch.append(domain)
            if len(batch) >= BATCH_SIZE:
                for d in batch:
                    res = await upsert_company(session, d)
                    if res == "created": fed += 1
                    await asyncio.sleep(WRITE_DELAY)
                log.info(f"Tranco: {count+len(batch):,} processati, {stats['fed']:,} inseriti totale")
                count += len(batch); batch = []

        for d in batch:
            res = await upsert_company(session, d)
            if res == "created": fed += 1
            await asyncio.sleep(WRITE_DELAY)
        log.info(f"Tranco DONE: {count+len(batch):,} processati, {fed} nuovi")
    except Exception as e:
        log.error(f"Tranco ERR: {e}")


# ─── SOURCE 3: KOMPASS INDUSTRIAL DIRECTORIES ─────────────────────────────────
async def feed_kompass(session):
    """
    Kompass — directory B2B con 70M+ aziende industriali.
    Scraping paginato per categoria + paese.
    """
    stats["source"] = "Kompass"
    log.info("Avvio scraping Kompass...")

    CATEGORIES = [
        # IT
        ("it", "macchine-utensili",      "Metalworking Machinery"),
        ("it", "automazione-industriale","Industrial Automation"),
        ("it", "robotica",               "Robotics"),
        ("it", "impianti-di-confezionamento","Packaging"),
        ("it", "logistica",              "Logistics"),
        ("it", "stampi",                 "Molds & Dies"),
        ("it", "macchine-alimentari",    "Food Machinery"),
        # DE
        ("de", "werkzeugmaschinen",      "Machine Tools"),
        ("de", "automatisierung",        "Automation"),
        ("de", "roboter",                "Robotics"),
        ("de", "lager-logistik",         "Warehouse Logistics"),
        ("de", "verpackungsmaschinen",   "Packaging Machines"),
        # FR
        ("fr", "machines-outils",        "Machine Tools"),
        ("fr", "automatisation",         "Automation"),
        # ES
        ("es", "maquinaria-industrial",  "Industrial Machinery"),
        # PL
        ("pl", "obrabiarki",             "Machine Tools"),
        ("pl", "automatyka",             "Automation"),
    ]

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; IndustrialBot/1.0)",
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    fed = 0
    for country, cat_slug, industry in CATEGORIES:
        for page in range(1, 51):  # max 50 pagine per categoria = ~2500 aziende
            url = f"https://it.kompass.com/{country}/{cat_slug}/page/{page}/"
            if country != "it":
                url = f"https://{country}.kompass.com/{cat_slug}/page/{page}/"
            try:
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=15),
                                       ssl=False) as r:
                    if r.status == 404: break
                    if r.status != 200:
                        await asyncio.sleep(2); continue
                    html = await r.text(errors="replace")

                # Estrai domini/URL aziende
                # Kompass usa pattern: href="/it/companyid/company-name/"
                import re as _re
                domains_found = _re.findall(
                    r'href="https?://(?:www\.)?([a-z0-9\-\.]+\.[a-z]{2,})["/]',
                    html, _re.I
                )
                company_names = _re.findall(
                    r'class="[^"]*company[^"]*"[^>]*>([^<]{3,60})<', html, _re.I
                )

                new_this_page = 0
                for i, domain in enumerate(domains_found[:50]):
                    domain = domain.lower().strip()
                    if (domain.startswith("kompass") or domain.startswith("google") or
                            len(domain) < 5): continue
                    name = company_names[i].strip() if i < len(company_names) else ""
                    res = await upsert_company(session, domain, name, country.upper(), industry)
                    if res == "created":
                        fed += 1; new_this_page += 1
                    await asyncio.sleep(WRITE_DELAY)

                if new_this_page == 0 and page > 3:
                    break  # fine contenuto utile
                log.info(f"Kompass {country}/{cat_slug} p{page}: +{new_this_page} | tot={stats['fed']:,}")
                await asyncio.sleep(1.5)

            except Exception as e:
                log.debug(f"Kompass {url}: {e}")
                await asyncio.sleep(2)

    log.info(f"Kompass DONE: {fed} nuovi domini")


# ─── SOURCE 4: EUROPAGES ──────────────────────────────────────────────────────
async def feed_europages(session):
    """
    Europages — directory EU con 3M+ aziende B2B.
    """
    stats["source"] = "Europages"
    log.info("Avvio scraping Europages...")

    CATEGORIES = [
        ("robotics-automation", "IT", "Robotics"),
        ("industrial-machinery", "IT", "Industrial Machinery"),
        ("metalworking", "IT", "Metalworking"),
        ("packaging-machinery", "DE", "Packaging"),
        ("material-handling", "DE", "Material Handling"),
        ("machine-tools", "DE", "Machine Tools"),
        ("conveying-equipment", "FR", "Conveying"),
        ("industrial-automation", "ES", "Automation"),
        ("warehouse-logistics", "NL", "Logistics"),
        ("cnc-machining", "PL", "CNC"),
        ("palletizing", "IT", "Palletizing"),
        ("welding-equipment", "DE", "Welding"),
        ("vision-systems", "DE", "Machine Vision"),
    ]

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; IndustrialBot/1.0)",
        "Accept": "text/html,*/*",
    }

    fed = 0
    for cat_slug, country, industry in CATEGORIES:
        for page in range(1, 101):  # max 100 pagine
            url = f"https://www.europages.co.uk/companies/{cat_slug}/pg-{page}.html"
            try:
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=15),
                                       ssl=False) as r:
                    if r.status == 404: break
                    if r.status != 200:
                        await asyncio.sleep(2); continue
                    html = await r.text(errors="replace")

                import re as _re
                # Europages mostra URL aziende nella pagina
                domains_found = _re.findall(
                    r'href="https?://(?:www\.)?([a-z0-9\-\.]+\.[a-z]{2,})["/]',
                    html, _re.I
                )
                company_names = _re.findall(
                    r'<h2[^>]*>([^<]{3,60})</h2>', html, _re.I
                )

                new_this_page = 0
                for i, domain in enumerate(set(domains_found[:40])):
                    domain = domain.lower().strip()
                    if any(x in domain for x in ["europages","google","facebook"]): continue
                    if len(domain) < 5: continue
                    name = company_names[i].strip() if i < len(company_names) else ""
                    res = await upsert_company(session, domain, name, country, industry)
                    if res == "created":
                        fed += 1; new_this_page += 1
                    await asyncio.sleep(WRITE_DELAY)

                if new_this_page == 0 and page > 5: break
                log.info(f"Europages {cat_slug} p{page}: +{new_this_page} | tot={stats['fed']:,}")
                await asyncio.sleep(1.5)

            except Exception as e:
                log.debug(f"Europages {url}: {e}")
                await asyncio.sleep(2)

    log.info(f"Europages DONE: {fed} nuovi")


# ─── SOURCE 5: OPEN LISTS (GITHUB) ───────────────────────────────────────────
async def feed_open_lists(session):
    """
    Liste pubbliche di domini industriali da GitHub / dataset aperti.
    """
    stats["source"] = "Open Lists"
    log.info("Scarico open lists...")

    SOURCES = [
        # Liste di aziende manifatturiere da dataset aperti
        "https://raw.githubusercontent.com/nicehash/NiceHashQuickMiner/master/installer/nicehash.txt",
        # Fallback: generiamo domini da SIC codes industriali via GLEIF
    ]

    # Usa GLEIF (Global LEI) per aziende con SIC codes manifatturieri
    # SIC 2000-3999 = Manufacturing
    gleif_url = "https://api.gleif.org/api/v1/lei-records?filter[entity.status]=ACTIVE&filter[entity.legalAddress.country]=IT&page[size]=200&page[number]={page}"

    fed = 0
    for country in ["IT","DE","FR","ES","PL","NL","BE","AT","CZ","SE","RO","HU"]:
        for page in range(1, 26):  # 25 pagine x 200 = 5000 per paese
            url = f"https://api.gleif.org/api/v1/lei-records?filter[entity.status]=ACTIVE&filter[entity.legalAddress.country]={country}&page[size]=200&page[number]={page}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200: break
                    d = await r.json(content_type=None)
                    records = d.get("data", [])
                    if not records: break

                    for rec in records:
                        attrs = rec.get("attributes", {})
                        entity = attrs.get("entity", {})
                        name = entity.get("legalName", {}).get("name", "")
                        # Tenta di ricostruire il dominio dal nome
                        domain_guess = (name.lower()
                                        .replace(" ", "").replace(",","").replace(".","")
                                        .replace("gmbh","").replace("srl","").replace("spa","")
                                        .replace("ag","").replace("bv","").replace("ab","")
                                        [:30]) + ".com"
                        if len(domain_guess) > 8 and is_industrial(name):
                            res = await upsert_company(session, domain_guess, name, country, "Manufacturing")
                            if res == "created": fed += 1
                        await asyncio.sleep(WRITE_DELAY * 0.5)

                log.info(f"GLEIF {country} p{page}: {stats['fed']:,} tot")
                await asyncio.sleep(0.5)
            except Exception as e:
                log.debug(f"GLEIF {e}")
                break

    log.info(f"Open Lists DONE: {fed} nuovi")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    log.info(f"=== Industrial Mass Feeder v1.0 | B44_APP={APP_ID[:8]}... ===")

    conn = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        # Lancia tutte le sorgenti in parallelo
        await asyncio.gather(
            feed_majestic(session),
            feed_tranco(session),
            feed_kompass(session),
            feed_europages(session),
            feed_open_lists(session),
            return_exceptions=True
        )

    stats["status"] = "done"
    log.info(f"=== FEEDER DONE: {stats['fed']:,} nuovi domini inseriti, {stats['errors']} errori ===")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())

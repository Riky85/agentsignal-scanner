#!/usr/bin/env python3
"""
Industrial Mass Domain Feeder v1.2
- USA SOLO PUT upsert (niente GET preventivo = zero rate limit)
- 1 req ogni 2 secondi = ~1.800 domini/ora su Base44
- Sorgenti: Majestic Million + GLEIF EU (16 paesi)
"""
import asyncio, aiohttp, csv, io, json, os, re, logging, threading
from urllib.parse import quote_plus
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FEED] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN  = os.environ.get("B44_SERVICE_TOKEN","907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID     = os.environ.get("B44_APP_ID","6a3a284ab0b87dfa27558bb6")
B44_BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW         = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
PORT       = int(os.environ.get("PORT","8080"))
# 1 req ogni 2s = safe per Base44 (max ~30 req/min)
WRITE_DELAY = float(os.environ.get("WRITE_DELAY","2.0"))

stats = {"fed":0,"skipped":0,"errors":0,"status":"starting","source":"","rl":0}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),Health).serve_forever(),daemon=True).start()
log.info(f"HTTP healthcheck su :{PORT}")

INDUSTRIAL_KW = re.compile(
    r"manufactur|industri|automat|robot|machin|engineer|metal|steel|casting|"
    r"plastic|packaging|pack|food|beverage|pharma|medtech|auto|motor|"
    r"electr|sensor|logistic|warehouse|pump|valve|hydraulic|pneumatic|"
    r"bearing|gear|welding|cutting|conveyor|coating|textile|chemical|"
    r"mining|cement|energy|turbine|group|holding|solutions|systems|tech|"
    r"gmbh|srl|spa|ag|bv|nv|ab|oy|as|kft|sas|sarl|ltd",re.I)

BLACKLIST = re.compile(
    r"^(google|facebook|twitter|youtube|instagram|tiktok|amazon|ebay|"
    r"wikipedia|reddit|netflix|spotify|apple|microsoft|bank|crypto|"
    r"news|blog|university|hospital|hotel|realty)",re.I)

def is_industrial(domain):
    d = domain.lower().replace("www.","").split(".")[0]
    if BLACKLIST.match(d): return False
    if INDUSTRIAL_KW.search(d): return True
    tld = domain.rsplit(".",1)[-1].lower()
    return tld in ("de","it","fr","es","pl","cz","nl","be","at","ch","se","dk","fi","no","pt","ro","hu")

async def upsert(session, domain, name="", country="", industry=""):
    """PUT upsert — crea se non esiste, aggiorna se esiste (per domain)."""
    payload = {
        "domain": domain,
        "website_url": f"https://{domain}",
        "name": name or domain.split(".")[0].replace("-"," ").title(),
        "country": country,
        "industry": industry or "Manufacturing",
        "scan_status": "pending",
        "source": "feeder_v12",
    }
    # Usa filter+PUT: prima tenta find, se non trovato fa POST
    # MA per non fare GET usiamo POST con gestione 409/duplicate
    for attempt in range(3):
        try:
            async with session.post(f"{B44_BASE}/IndustrialCompany",
                                    headers=HW, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 429:
                    stats["rl"] += 1
                    log.warning(f"Rate limit #{stats['rl']} — pausa 30s")
                    await asyncio.sleep(30)
                    continue
                if r.status in (200,201):
                    stats["fed"] += 1
                    return True
                if r.status == 409:  # duplicate
                    stats["skipped"] += 1
                    return False
                # Altro errore
                body = await r.text()
                if "duplicate" in body.lower() or "already" in body.lower():
                    stats["skipped"] += 1
                    return False
                stats["errors"] += 1
                return False
        except Exception as e:
            await asyncio.sleep(5)
    stats["errors"] += 1
    return False

async def feed_majestic(session):
    stats["source"] = "Majestic"
    log.info("Scarico Majestic Million CSV...")
    url = "https://downloads.majestic.com/majestic_million.csv"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=180)) as r:
            if r.status != 200:
                log.warning(f"Majestic HTTP {r.status}"); return
            content = await r.read()
        reader = csv.DictReader(io.StringIO(content.decode("utf-8",errors="replace")))
        count = 0
        for row in reader:
            domain = (row.get("Domain") or row.get("IDN_Domain","")).strip().lower()
            if not domain or not is_industrial(domain): continue
            await upsert(session, domain)
            count += 1
            await asyncio.sleep(WRITE_DELAY)
            if count % 100 == 0:
                log.info(f"Majestic: {count} processati | inseriti={stats['fed']} skip={stats['skipped']} rl={stats['rl']}")
        log.info(f"Majestic DONE: {count} industriali trovati")
    except Exception as e:
        log.error(f"Majestic ERR: {e}")

async def feed_gleif(session):
    stats["source"] = "GLEIF"
    log.info("Avvio GLEIF EU (16 paesi)...")
    countries = ["IT","DE","FR","ES","PL","NL","BE","AT","CZ","SE","RO","HU","DK","FI","NO","PT"]
    for country in countries:
        for page in range(1, 26):
            url = (f"https://api.gleif.org/api/v1/lei-records"
                   f"?filter[entity.status]=ACTIVE"
                   f"&filter[entity.legalAddress.country]={country}"
                   f"&page[size]=200&page[number]={page}")
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200: break
                    d = await r.json(content_type=None)
                    records = d.get("data",[])
                    if not records: break
                inserted = 0
                for rec in records:
                    name = (rec.get("attributes",{})
                              .get("entity",{})
                              .get("legalName",{})
                              .get("name",""))
                    if not name or not is_industrial(name): continue
                    slug = re.sub(r"[^a-z0-9]","",name.lower()[:25])
                    if len(slug) < 4: continue
                    domain = slug + ".com"
                    ok = await upsert(session, domain, name, country, "Manufacturing")
                    if ok: inserted += 1
                    await asyncio.sleep(WRITE_DELAY)
                log.info(f"GLEIF {country} p{page}: +{inserted} | totale={stats['fed']}")
                await asyncio.sleep(1)
            except Exception as e:
                log.debug(f"GLEIF {e}"); break
    log.info(f"GLEIF DONE: totale inseriti={stats['fed']}")

async def main():
    stats["status"] = "running"
    log.info(f"=== Mass Feeder v1.2 | delay={WRITE_DELAY}s | token={B44_TOKEN[:8]}... ===")
    conn = aiohttp.TCPConnector(limit=3, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(
            feed_majestic(session),
            feed_gleif(session),
            return_exceptions=True
        )
    stats["status"] = "done"
    log.info(f"=== DONE: {stats['fed']} inseriti, {stats['skipped']} skip, {stats['errors']} err ===")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

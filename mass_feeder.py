#!/usr/bin/env python3
"""
Industrial Mass Domain Feeder v1.1
- WRITE_DELAY default 0.5s (max ~2 req/s su Base44)
- Batch di 50 record con pausa 5s tra batch
- Controlla existence prima di inserire (no duplicati)
- Sorgenti: Majestic Million + GLEIF EU + Kompass
"""
import asyncio, aiohttp, csv, io, json, os, re, logging, threading
from urllib.parse import quote_plus
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FEED] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN   = os.environ.get("B44_SERVICE_TOKEN","")
APP_ID      = os.environ.get("B44_APP_ID","6a3a284ab0b87dfa27558bb6")
B44_BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW          = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
PORT        = int(os.environ.get("PORT","8080"))
WRITE_DELAY = float(os.environ.get("WRITE_DELAY","0.5"))
BATCH_PAUSE = float(os.environ.get("BATCH_PAUSE","5.0"))
BATCH_SIZE  = int(os.environ.get("BATCH_SIZE","50"))

stats = {"fed":0,"skipped":0,"errors":0,"status":"starting","source":"","rate_limit_hits":0}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),Health).serve_forever(),daemon=True).start()

INDUSTRIAL_KW = re.compile(
    r"manufactur|industri|automat|robot|machin|engineer|metal|steel|casting|"
    r"plastic|packaging|pack|food|beverage|pharma|medtech|auto|motor|"
    r"electr|sensor|logistic|warehouse|pump|valve|hydraulic|pneumatic|"
    r"bearing|gear|welding|cutting|conveyor|coating|textile|chemical|"
    r"mining|cement|energy|turbine|group|holding|solutions|systems|tech|"
    r"gmbh|srl|spa|ag|bv|nv|ab|oy|as|kft|sas|sarl|ltd",re.I)

BLACKLIST = re.compile(
    r"google|facebook|twitter|youtube|instagram|tiktok|amazon|ebay|"
    r"wikipedia|reddit|netflix|spotify|apple|microsoft|bank|crypto|"
    r"news|blog|university|hospital|hotel|realty",re.I)

def is_industrial(domain):
    d = domain.lower().replace("www.","").split(".")[0]
    if BLACKLIST.search(d): return False
    if INDUSTRIAL_KW.search(d): return True
    tld = domain.rsplit(".",1)[-1].lower()
    return tld in ("com","de","it","fr","es","pl","cz","nl","be","at","ch","se","dk","fi","no","pt","ro","hu")

async def b44_exists(session, domain):
    url = f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 429:
                stats["rate_limit_hits"] += 1
                await asyncio.sleep(10)
                return True  # skip per sicurezza
            if r.status == 200:
                d = await r.json(content_type=None)
                return bool(isinstance(d,list) and d)
    except: pass
    return False

async def b44_create(session, domain, name="", country="", industry=""):
    payload = {"domain":domain,"website_url":f"https://{domain}",
                "name":name or domain.split(".")[0].title(),
                "country":country,"industry":industry,"scan_status":"pending","source":"mass_feeder_v1"}
    for attempt in range(3):
        try:
            async with session.post(f"{B44_BASE}/IndustrialCompany", headers=HW, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 429:
                    stats["rate_limit_hits"] += 1
                    log.warning(f"Rate limit — pausa 15s")
                    await asyncio.sleep(15)
                    continue
                if r.status in (200,201):
                    stats["fed"] += 1
                    return True
                stats["errors"] += 1
                return False
        except Exception as e:
            await asyncio.sleep(5)
    stats["errors"] += 1
    return False

async def feed_majestic(session):
    stats["source"] = "Majestic Million"
    log.info("Scarico Majestic Million...")
    url = "https://downloads.majestic.com/majestic_million.csv"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=180)) as r:
            if r.status != 200: log.warning(f"Majestic HTTP {r.status}"); return
            content = await r.read()
        reader = csv.DictReader(io.StringIO(content.decode("utf-8",errors="replace")))
        batch, count = [], 0
        for row in reader:
            domain = (row.get("Domain") or row.get("IDN_Domain","")).strip().lower()
            if not domain or not is_industrial(domain): continue
            batch.append(domain)
            if len(batch) >= BATCH_SIZE:
                for d in batch:
                    if not await b44_exists(session, d):
                        await b44_create(session, d)
                    await asyncio.sleep(WRITE_DELAY)
                count += len(batch)
                log.info(f"Majestic: {count:,} filtrati | B44 inseriti: {stats['fed']:,} | RL hits: {stats['rate_limit_hits']}")
                batch = []
                await asyncio.sleep(BATCH_PAUSE)
        for d in batch:
            if not await b44_exists(session, d):
                await b44_create(session, d)
            await asyncio.sleep(WRITE_DELAY)
        log.info(f"Majestic DONE: {count:,} processati")
    except Exception as e:
        log.error(f"Majestic ERR: {e}")

async def feed_gleif(session):
    stats["source"] = "GLEIF"
    log.info("Avvio GLEIF EU...")
    for country in ["IT","DE","FR","ES","PL","NL","BE","AT","CZ","SE","RO","HU","DK","FI","NO","PT"]:
        for page in range(1, 51):
            url = f"https://api.gleif.org/api/v1/lei-records?filter[entity.status]=ACTIVE&filter[entity.legalAddress.country]={country}&page[size]=200&page[number]={page}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200: break
                    d = await r.json(content_type=None)
                    records = d.get("data",[])
                    if not records: break
                for rec in records:
                    name = (rec.get("attributes",{}).get("entity",{}).get("legalName",{}).get("name",""))
                    if not name or not is_industrial(name): continue
                    slug = re.sub(r"[^a-z0-9]","",name.lower()[:25])
                    if len(slug) < 4: continue
                    domain = slug + ".com"
                    if not await b44_exists(session, domain):
                        await b44_create(session, domain, name, country, "Manufacturing")
                    await asyncio.sleep(WRITE_DELAY)
                log.info(f"GLEIF {country} p{page}: totale inseriti={stats['fed']:,}")
                await asyncio.sleep(BATCH_PAUSE)
            except Exception as e:
                log.debug(f"GLEIF {e}"); break

async def main():
    stats["status"] = "running"
    log.info(f"=== Mass Feeder v1.1 | delay={WRITE_DELAY}s | batch_pause={BATCH_PAUSE}s ===")
    conn = aiohttp.TCPConnector(limit=5, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(feed_majestic(session), feed_gleif(session), return_exceptions=True)
    stats["status"] = "done"
    log.info(f"=== DONE: {stats['fed']:,} inseriti, {stats['errors']} errori ===")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

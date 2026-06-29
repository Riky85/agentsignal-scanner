#!/usr/bin/env python3
"""
Mass Scanner Runner — carica domini 'pending' da Base44 e li scansiona
Usa lo stesso industrial_scanner ma con lista dinamica da Base44
"""
import asyncio, aiohttp, os, json, logging
from industrial_scanner import scan_company, stats, B44_BASE, HW, PORT
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MASS] %(message)s")
log = logging.getLogger(__name__)

WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "3"))
CONCURRENCY   = int(os.environ.get("CONCURRENCY", "8"))
BATCH         = int(os.environ.get("BATCH_SIZE", "500"))

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


async def load_pending(session, skip=0):
    """Carica batch di aziende da scansionare — scan_status=pending O null."""
    # Prima prova con pending esplicito
    for status_filter in ["pending", ""]:
        if status_filter:
            url = f"{B44_BASE}/IndustrialCompany?scan_status=pending&limit={BATCH}&skip={skip}&fields=id,name,domain,country,industry,scan_status"
        else:
            # Nessun filtro status — prende tutti e filtra in locale
            url = f"{B44_BASE}/IndustrialCompany?limit={BATCH}&skip={skip}&fields=id,name,domain,country,industry,scan_status"
        try:
            async with session.get(url, headers=HW,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    if isinstance(d, list):
                        if status_filter == "":
                            # filtra solo quelli non-done e non-scanning
                            d = [c for c in d if c.get("scan_status") not in ("done","scanning")]
                        if d:
                            return d
        except Exception as e:
            log.warning(f"load_pending ERR ({status_filter}): {e}")
    return []


async def main():
    stats["status"] = "running"
    log.info(f"=== Mass Scanner | Worker {WORKER_ID}/{TOTAL_WORKERS} | CONCURRENCY={CONCURRENCY} ===")

    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 3, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        skip = WORKER_ID * BATCH  # ogni worker parte da un offset diverso
        total_done = 0

        while True:
            batch = await load_pending(session, skip=skip)
            if not batch:
                log.info(f"Nessun pending trovato (skip={skip}) — attendo 5min...")
                await asyncio.sleep(300)
                skip = WORKER_ID * BATCH  # reset
                continue

            log.info(f"Caricati {len(batch)} pending da Base44 (skip={skip})")

            async def _run(c):
                async with sem:
                    try:
                        company = {
                            "domain":   c.get("domain",""),
                            "name":     c.get("name",""),
                            "country":  c.get("country",""),
                            "industry": c.get("industry",""),
                        }
                        if company["domain"]:
                            await scan_company(session, company)
                    except Exception as e:
                        log.warning(f"ERR {c.get('domain','?')}: {e}")
                    await asyncio.sleep(0.5)

            await asyncio.gather(*[_run(c) for c in batch], return_exceptions=True)
            total_done += len(batch)
            skip += TOTAL_WORKERS * BATCH
            log.info(f"Batch done. Totale scansionati questa sessione: {total_done:,}")

if __name__ == "__main__":
    asyncio.run(main())

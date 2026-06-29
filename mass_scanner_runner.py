#!/usr/bin/env python3
"""
Industrial Mass Scanner Runner v2.1
12.156 aziende industriali in B44 — 4 worker in parallelo.
Ogni worker copre 1/4 del database (segmentazione per skip deterministico).
"""
import asyncio, aiohttp, os, json, logging, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from industrial_scanner import scan_company, stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [IND] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN     = os.environ.get("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID        = os.environ.get("B44_APP_ID",        "6a3a284ab0b87dfa27558bb6")
B44_BASE      = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW            = {"api-key": B44_TOKEN}
WORKER_ID     = int(os.environ.get("WORKER_ID",     "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "4"))
CONCURRENCY   = int(os.environ.get("CONCURRENCY",  "12"))
BATCH         = 200          # records per chiamata API
PORT          = int(os.environ.get("PORT", "8080"))

# Healthcheck
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({**stats, "worker": WORKER_ID}).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True).start()

async def load_batch(session, skip):
    """Carica un batch di IndustrialCompany dalla posizione skip."""
    url = (f"{B44_BASE}/IndustrialCompany"
           f"?limit={BATCH}&skip={skip}"
           f"&fields=id,name,domain,country,city,industry,"
           f"employee_count,scanned,annual_revenue_eur_k,description")
    try:
        async with session.get(url, headers=HW,
                               timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                return d if isinstance(d, list) else []
    except Exception as e:
        log.warning(f"load_batch skip={skip}: {e}")
    return []

async def main():
    log.info(f"=== Industrial Scanner v2.1 | Worker {WORKER_ID}/{TOTAL_WORKERS} "
             f"| Conc={CONCURRENCY} | ~{12156 // TOTAL_WORKERS} az. per worker ===")

    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        cycle = 0
        while True:
            cycle += 1
            done_this_cycle = 0

            # Ogni worker parte da un offset diverso e avanza di TOTAL_WORKERS*BATCH
            # Worker 0: 0, 4*200, 8*200 ...   → aziende 0-3050 circa
            # Worker 1: 200, 5*200, 9*200 ...  → aziende 200-3250 circa
            # Worker 2: 400, 6*200, 10*200 ... → aziende 400-3450 circa
            # Worker 3: 600, 7*200, 11*200 ... → aziende 600-3650 circa
            skip = WORKER_ID * BATCH

            while True:
                batch = await load_batch(session, skip)
                if not batch:
                    break

                # Salta record già scansionati in questo ciclo
                to_scan = [c for c in batch
                           if not c.get("scanned") or c["scanned"] == 0]

                if to_scan:
                    log.info(f"  skip={skip} | batch={len(batch)} | da_scansionare={len(to_scan)}")

                    async def _run(c):
                        async with sem:
                            try:
                                await scan_company(session, c)
                            except Exception as e:
                                log.warning(f"ERR {c.get('domain','?')}: {e}")
                            await asyncio.sleep(0.2)

                    await asyncio.gather(*[_run(c) for c in to_scan],
                                         return_exceptions=True)
                    done_this_cycle += len(to_scan)

                if len(batch) < BATCH:
                    break  # fine database

                skip += TOTAL_WORKERS * BATCH
                await asyncio.sleep(0.5)

            log.info(
                f"Ciclo {cycle} OK | scansionate={done_this_cycle} | "
                f"totale_sess={stats.get('scanned',0)} | "
                f"segnali={stats.get('signals',0)} | "
                f"opps={stats.get('opportunities',0)} | "
                f"rev_trovati={stats.get('revenue_found',0)}"
            )
            if done_this_cycle == 0:
                log.info("Tutto scansionato. Pausa 10min poi ricomincia da capo...")
                await asyncio.sleep(600)

if __name__ == "__main__":
    asyncio.run(main())

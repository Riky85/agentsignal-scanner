#!/usr/bin/env python3
"""
Industrial Mass Scanner Runner v2.2
- 4 worker in parallelo, 12 thread ciascuno = 48 scan paralleli
- Skip deterministico: Worker N copre record N*200, (N+4)*200, (N+8)*200...
- "Non scansionato" = estimated_deal_value_max IS NULL
- "Scansionato" = estimated_deal_value_max > 0 (impostato dal scanner)
"""
import asyncio, aiohttp, os, json, logging, threading, time
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
BATCH         = 200
PORT          = int(os.environ.get("PORT", "8080"))

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({**stats, "worker": WORKER_ID, "ts": int(time.time())}).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True).start()

async def load_batch(session, skip):
    url = (f"{B44_BASE}/IndustrialCompany"
           f"?limit={BATCH}&skip={skip}"
           f"&fields=id,name,domain,country,city,industry,"
           f"employee_count,revenue,estimated_deal_value_max,description")
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                return d if isinstance(d, list) else []
    except Exception as e:
        log.warning(f"load_batch skip={skip}: {e}")
    return []

async def main():
    log.info(f"=== Industrial Scanner v2.2 | Worker {WORKER_ID}/{TOTAL_WORKERS} | Conc={CONCURRENCY} ===")
    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        cycle = 0
        while True:
            cycle += 1
            done_this = 0
            skip = WORKER_ID * BATCH  # partenza deterministica per questo worker

            while True:
                batch = await load_batch(session, skip)
                if not batch:
                    break

                # "Non ancora scansionato" = deal_max è NULL/0
                pending = [c for c in batch
                           if not c.get("estimated_deal_value_max")
                           or c["estimated_deal_value_max"] == 0]

                if pending:
                    log.info(f"  skip={skip:>6} | batch={len(batch)} | pending={len(pending)}")

                    async def _run(c):
                        async with sem:
                            try:
                                await scan_company(session, c)
                            except Exception as e:
                                log.warning(f"ERR {c.get('domain','?')}: {e}")
                            await asyncio.sleep(0.2)

                    await asyncio.gather(*[_run(c) for c in pending], return_exceptions=True)
                    done_this += len(pending)

                if len(batch) < BATCH:
                    break

                skip += TOTAL_WORKERS * BATCH
                await asyncio.sleep(0.3)

            log.info(
                f"Ciclo {cycle} | done={done_this} | "
                f"scansionate={stats.get('scanned',0)} | "
                f"segnali={stats.get('signals',0)} | "
                f"opps={stats.get('opportunities',0)} | "
                f"rev={stats.get('revenue_found',0)}"
            )
            if done_this == 0:
                log.info("Tutto scansionato. Pausa 10min...")
                await asyncio.sleep(600)

if __name__ == "__main__":
    asyncio.run(main())

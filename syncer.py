#!/usr/bin/env python3
"""Syncer FINAL v2 — bulk POST + salvataggio base44_id + healthcheck HTTP"""
import asyncio, aiohttp, asyncpg, os, json, logging
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
B44_TOKEN = (os.environ.get("B44_SERVICE_TOKEN") or
             os.environ.get("BASE44_SERVICE_TOKEN") or
             os.environ.get("AGENTSIGNAL_SERVICE_TOKEN") or
             os.environ.get("BASE44_TOKEN") or "")
APP_ID   = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE_URL = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company"
BULK_URL = f"{BASE_URL}/bulk"
HW       = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
BATCH    = 200
DELAY    = 13.0
PORT     = int(os.environ.get("PORT", 8080))

stats = {"pushed": 0, "errors": 0, "cycle": 0}

def tl(v):
    if isinstance(v, list): return v
    try: p=json.loads(v); return p if isinstance(p,list) else []
    except: return []

def tod(v):
    if isinstance(v, dict): return v
    try: p=json.loads(v); return p if isinstance(p,dict) else {}
    except: return {}

def build(r):
    w = (r.get("website") or f"https://{r['domain']}").rstrip("/")
    n = (r.get("name") or r["domain"].split(".")[0].title())[:100]
    return {
        "name": n, "website": w,
        "tech_stack":     tl(r.get("tech_stack")),
        "ai_stack":       tl(r.get("ai_stack")),
        "technology_dna": tod(r.get("technology_dna")),
        "description":    (r.get("description") or "")[:500],
        "industry":       r.get("industry") or "",
        "employee_count": int(r.get("employee_count") or 0),
        "country":        r.get("country") or "",
        "global_rank":    int(r.get("global_rank") or 0),
        "source":         r.get("source") or "scanner",
        "ai_adoption_score":      int(r.get("ai_score") or 0),
        "ai_maturity_score":      int(r.get("maturity_score") or 0),
        "cloud_score":            int(r.get("cloud_score") or 0),
        "automation_score":       int(r.get("automation_score") or 0),
        "commerce_score":         int(r.get("commerce_score") or 0),
        "growth_score":           int(r.get("growth_score") or 0),
        "ai_buying_intent_score": int(r.get("intent_score") or 0),
    }

async def load_batch(pool):
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT id,domain,name,website,source,global_rank,
                   ai_stack,tech_stack,technology_dna,
                   ai_score,maturity_score,cloud_score,automation_score,
                   intent_score,commerce_score,growth_score,
                   description,industry,employee_count,country,
                   org_chart,ats_product_signals,base44_id
            FROM companies
            WHERE last_scan_date IS NOT NULL
              AND base44_id IS NULL
              AND COALESCE(scan_errors,0) < 5
            ORDER BY global_rank ASC NULLS LAST
            LIMIT $1
        """, BATCH)
    return [dict(r) for r in rows]

async def push_batch(session, pool, records):
    payloads = [build(r) for r in records]
    try:
        async with session.post(BULK_URL, headers=HW, json=payloads,
                                timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status == 200:
                inserted = await resp.json(content_type=None) or []
                async with pool.acquire() as c:
                    for r, item in zip(records, inserted):
                        iid = item.get("id") if isinstance(item, dict) else None
                        if iid:
                            await c.execute(
                                "UPDATE companies SET base44_id=$1,last_push_date=NOW() WHERE domain=$2",
                                iid, r["domain"])
                ok = len([i for i in inserted if isinstance(i,dict) and i.get("id")])
                return ok, len(records)-ok
            else:
                body = await resp.text()
                log.warning(f"Bulk {resp.status}: {body[:120]}")
                return 0, len(records)
    except Exception as e:
        log.warning(f"Bulk ERR: {e}")
        return 0, len(records)

async def healthcheck(request):
    return web.Response(text=json.dumps({
        "status": "ok",
        "pushed": stats["pushed"],
        "errors": stats["errors"],
        "cycle":  stats["cycle"]
    }), content_type="application/json")

async def run_syncer(pool):
    conn = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=conn) as session:
        while True:
            batch = await load_batch(pool)
            if not batch:
                log.info(f"Sync completo: pushed={stats['pushed']} errors={stats['errors']}. Sleep 5min.")
                await asyncio.sleep(300)
                continue
            stats["cycle"] += 1
            log.info(f"[C{stats['cycle']}] {len(batch)} records")
            ok, err = await push_batch(session, pool, batch)
            stats["pushed"] += ok
            stats["errors"] += err
            log.info(f"[C{stats['cycle']}] ok={ok} err={err} | totale pushed={stats['pushed']}")
            await asyncio.sleep(DELAY)

async def main():
    log.info(f"=== Syncer FINAL v2 — token: {B44_TOKEN[:12]}... PORT={PORT} ===")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    async with pool.acquire() as c:
        total = await c.fetchval(
            "SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL AND base44_id IS NULL")
        log.info(f"Record da sincronizzare: {total:,}")

    # Avvia healthcheck HTTP in background
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Healthcheck su :{PORT}")

    # Avvia syncer loop
    await run_syncer(pool)

if __name__ == "__main__":
    asyncio.run(main())

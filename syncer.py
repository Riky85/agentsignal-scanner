#!/usr/bin/env python3
"""Syncer v3 — upsert sicuro: GET per website prima di POST. Zero duplicati."""
import asyncio, aiohttp, asyncpg, os, json, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
B44_TOKEN    = (os.environ.get("B44_SERVICE_TOKEN") or os.environ.get("BASE44_SERVICE_TOKEN") or os.environ.get("AGENTSIGNAL_SERVICE_TOKEN") or os.environ.get("BASE44_TOKEN") or "")
APP_ID       = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE_URL     = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company"
HW           = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
BATCH_SIZE   = 30
PUSH_DELAY   = 11.0

def tl(v):
    if isinstance(v, list): return v
    try: p=json.loads(v); return p if isinstance(p,list) else []
    except: return []

def to(v):
    if isinstance(v, dict): return v
    try: p=json.loads(v); return p if isinstance(p,dict) else {}
    except: return {}

def build_payload(r):
    w = (r.get("website") or f"https://{r['domain']}").rstrip("/")
    n = (r.get("name") or r["domain"].split(".")[0].title())[:100]
    return {
        "name": n, "website": w,
        "tech_stack": tl(r.get("tech_stack")),
        "ai_stack":   tl(r.get("ai_stack")),
        "technology_dna": to(r.get("technology_dna")),
        "description": (r.get("description") or "")[:500],
        "industry":    r.get("industry") or "",
        "employee_count": int(r.get("employee_count") or 0),
        "country":     r.get("country") or "",
        "global_rank": int(r.get("global_rank") or 0),
        "source":      r.get("source") or "scanner",
        "ai_adoption_score":    int(r.get("ai_score") or 0),
        "ai_maturity_score":    int(r.get("maturity_score") or 0),
        "cloud_score":          int(r.get("cloud_score") or 0),
        "automation_score":     int(r.get("automation_score") or 0),
        "commerce_score":       int(r.get("commerce_score") or 0),
        "growth_score":         int(r.get("growth_score") or 0),
        "ai_buying_intent_score": int(r.get("intent_score") or 0),
    }

async def upsert(session, pool, r):
    w = (r.get("website") or f"https://{r['domain']}").rstrip("/")
    b44_id = r.get("base44_id")
    if not b44_id:
        try:
            async with session.get(BASE_URL, headers=HW,
                params={"website": w, "limit": 5, "fields": "id,website"},
                timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    items = await resp.json(content_type=None)
                    for item in (items or []):
                        if (item.get("website") or "").rstrip("/").lower() == w.lower():
                            b44_id = item["id"]; break
        except: pass
    p = build_payload(r)
    if b44_id:
        async with session.put(f"{BASE_URL}/{b44_id}", headers=HW, json=p,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                async with pool.acquire() as c:
                    await c.execute("UPDATE companies SET base44_id=$1,last_push_date=NOW() WHERE domain=$2", b44_id, r["domain"])
                return "upd"
    else:
        async with session.post(BASE_URL, headers=HW, json=p,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                cr = await resp.json(content_type=None)
                nid = cr.get("id") if isinstance(cr,dict) else None
                if nid:
                    async with pool.acquire() as c:
                        await c.execute("UPDATE companies SET base44_id=$1,last_push_date=NOW() WHERE domain=$2", nid, r["domain"])
                    return "new"
    return "err"

async def load_batch(pool):
    async with pool.acquire() as c:
        rows = await c.fetch("""SELECT id,domain,name,website,source,global_rank,
            ai_stack,tech_stack,technology_dna,ai_score,maturity_score,
            cloud_score,automation_score,intent_score,commerce_score,growth_score,
            description,industry,employee_count,country,org_chart,ats_product_signals,
            base44_id FROM companies
            WHERE last_scan_date IS NOT NULL AND base44_id IS NULL
            AND COALESCE(scan_errors,0)<5
            ORDER BY global_rank ASC NULLS LAST LIMIT $1""", BATCH_SIZE)
    return [dict(r) for r in rows]

async def main():
    log.info("=== Syncer v3 start ===")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    async with pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM companies WHERE base44_id IS NOT NULL")
        await c.execute("UPDATE companies SET base44_id=NULL,last_push_date=NULL")
        log.info(f"RESET: {n:,} base44_id azzerati")
        total = await c.fetchval("SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL")
        log.info(f"Pronti: {total:,}")
    conn = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(connector=conn) as session:
        new_n=upd_n=err_n=cycle=0
        while True:
            batch = await load_batch(pool)
            if not batch:
                log.info(f"Sync completo: new={new_n} upd={upd_n} err={err_n}")
                await asyncio.sleep(300); continue
            cycle+=1
            log.info(f"[C{cycle}] {len(batch)} records")
            for r in batch:
                try:
                    res = await upsert(session, pool, r)
                    if res=="new": new_n+=1
                    elif res=="upd": upd_n+=1
                    else: err_n+=1
                except Exception as e:
                    log.warning(f"upsert err: {e}"); err_n+=1
                await asyncio.sleep(PUSH_DELAY)
            log.info(f"[C{cycle}] done new={new_n} upd={upd_n} err={err_n}")

if __name__=="__main__":
    asyncio.run(main())

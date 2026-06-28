#!/usr/bin/env python3
"""
AgentSignal Syncer — CLEAN v3
Strategia upsert SICURA:
  1. Carica batch da Railway (solo base44_id IS NULL)
  2. Per ogni record: GET Base44 per website esatto
     - Trovato  → PUT (aggiorna) + salva base44_id su Railway
     - Non trovato → POST (crea)  + salva base44_id su Railway
  3. Delay 11s tra ogni record per rispettare rate limit
"""
import asyncio, aiohttp, asyncpg, os, json, logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
B44_TOKEN    = os.environ["B44_SERVICE_TOKEN"]
APP_ID       = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE_URL     = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company"
HW           = {"api-key": B44_TOKEN, "Content-Type": "application/json"}

BATCH_SIZE   = 50    # piccolo, gestibile
PUSH_DELAY   = 11.0  # secondi tra record (rate limit)

def build_payload(r):
    def to_list(v):
        if isinstance(v, list): return v
        if isinstance(v, str):
            try: parsed = json.loads(v); return parsed if isinstance(parsed, list) else []
            except: return []
        return []
    def to_obj(v):
        if isinstance(v, dict): return v
        if isinstance(v, str):
            try: parsed = json.loads(v); return parsed if isinstance(parsed, dict) else {}
            except: return {}
        return {}
    website = (r.get("website") or f"https://{r['domain']}").rstrip("/")
    name    = (r.get("name") or r["domain"].replace("-"," ").replace("."," ").title())[:100]
    payload = {
        "name":    name,
        "website": website,
        "tech_stack":       to_list(r.get("tech_stack")),
        "ai_stack":         to_list(r.get("ai_stack")),
        "technology_dna":   to_obj(r.get("technology_dna")),
        "description":      (r.get("description") or "")[:500],
        "industry":         r.get("industry") or "",
        "employee_count":   r.get("employee_count") or 0,
        "country":          r.get("country") or "",
        "global_rank":      r.get("global_rank") or 0,
        "source":           r.get("source") or "scanner",
        "ai_adoption_score":    int(r.get("ai_score") or 0),
        "ai_maturity_score":    int(r.get("maturity_score") or 0),
        "cloud_score":          int(r.get("cloud_score") or 0),
        "automation_score":     int(r.get("automation_score") or 0),
        "commerce_score":       int(r.get("commerce_score") or 0),
        "growth_score":         int(r.get("growth_score") or 0),
        "ai_buying_intent_score": int(r.get("intent_score") or 0),
    }
    if r.get("org_chart"):       payload["org_chart"]       = to_obj(r["org_chart"])
    if r.get("ats_product_signals"): payload["ats_product_signals"] = to_list(r["ats_product_signals"])
    return payload

async def upsert_one(session, pool, r):
    """GET → PUT se esiste, POST se nuovo. Salva sempre base44_id."""
    website = (r.get("website") or f"https://{r['domain']}").rstrip("/")
    b44_id  = r.get("base44_id")

    # Se non abbiamo l'ID, cerca per website
    if not b44_id:
        try:
            async with session.get(BASE_URL, headers=HW,
                params={"website": website, "limit": 1, "fields": "id,website"},
                timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    existing = await resp.json(content_type=None)
                    if existing and isinstance(existing, list):
                        # Verifica match esatto
                        for item in existing:
                            ew = (item.get("website") or "").rstrip("/")
                            if ew.lower() == website.lower():
                                b44_id = item["id"]
                                break
        except Exception as e:
            log.debug(f"GET fallito per {website}: {e}")

    payload = build_payload(r)

    if b44_id:
        # PUT — aggiorna record esistente
        try:
            async with session.put(f"{BASE_URL}/{b44_id}", headers=HW, json=payload,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    async with pool.acquire() as c:
                        await c.execute(
                            "UPDATE companies SET base44_id=$1, last_push_date=NOW() WHERE domain=$2",
                            b44_id, r["domain"])
                    return "updated", b44_id
                else:
                    body = await resp.text()
                    return "err_put", body[:80]
        except Exception as e:
            return "err_put", str(e)[:80]
    else:
        # POST — crea nuovo
        try:
            async with session.post(BASE_URL, headers=HW, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    created = await resp.json(content_type=None)
                    new_id  = created.get("id") if isinstance(created, dict) else None
                    if new_id:
                        async with pool.acquire() as c:
                            await c.execute(
                                "UPDATE companies SET base44_id=$1, last_push_date=NOW() WHERE domain=$2",
                                new_id, r["domain"])
                        return "created", new_id
                    return "err_no_id", ""
                else:
                    body = await resp.text()
                    return "err_post", body[:80]
        except Exception as e:
            return "err_post", str(e)[:80]

async def load_batch(pool):
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT id, domain, name, website, source, global_rank,
                   ai_stack, tech_stack, technology_dna,
                   ai_score, maturity_score, cloud_score, automation_score,
                   intent_score, commerce_score, growth_score,
                   description, industry, employee_count, country,
                   org_chart, ats_product_signals,
                   base44_id, last_scan_date
            FROM companies
            WHERE last_scan_date IS NOT NULL
              AND base44_id IS NULL
              AND COALESCE(scan_errors, 0) < 5
            ORDER BY global_rank ASC NULLS LAST
            LIMIT $1
        """, BATCH_SIZE)
    return [dict(r) for r in rows]

async def run():
    log.info("=== Syncer v3 avviato ===")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    # Reset base44_id per ripartire puliti
    async with pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM companies WHERE base44_id IS NOT NULL")
        if n > 0:
            await c.execute("UPDATE companies SET base44_id=NULL, last_push_date=NULL")
            log.info(f"RESET: {n:,} base44_id azzerati — ripartenza pulita")
        total = await c.fetchval("SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL")
        log.info(f"Pronti per sync: {total:,} record")

    conn = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=conn) as session:
        pushed = created = updated = errors = 0
        cycle  = 0
        while True:
            batch = await load_batch(pool)
            if not batch:
                log.info(f"✅ Sync completato! created={created} updated={updated} errors={errors}")
                await asyncio.sleep(60)
                continue

            cycle += 1
            log.info(f"[C{cycle}] {len(batch)} record da sincronizzare")

            for r in batch:
                action, detail = await upsert_one(session, pool, r)
                if action in ("created","updated"):
                    pushed += 1
                    if action == "created": created += 1
                    else: updated += 1
                    if pushed % 10 == 0:
                        log.info(f"  pushed={pushed} created={created} updated={updated} err={errors}")
                else:
                    errors += 1
                    log.warning(f"  ERR {action}: {detail} ({r.get('domain')})")
                await asyncio.sleep(PUSH_DELAY)

            log.info(f"[C{cycle}] done — total pushed={pushed} created={created} updated={updated} errors={errors}")

if __name__ == "__main__":
    asyncio.run(run())

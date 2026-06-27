#!/usr/bin/env python3
"""
AgentSignal Base44 Dedup — Railway
Scarica tutti i record da Base44, identifica duplicati, elimina tenendo il migliore.
"""
import asyncio, aiohttp, json, os, logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

TOKEN     = ""
APP_ID    = "6a3a284ab0b87dfa27558bb6"
BASE_URL  = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company"
HDR       = {"api-key": TOKEN, "Content-Type": "application/json"}
FIELDS    = "id,name,website,ai_adoption_score,description,employee_count,org_chart,industry,ai_stack,logo_url,country"

async def fetch_all(session):
    all_cos, skip = [], 0
    while True:
        async with session.get(f"{BASE_URL}?limit=500&skip={skip}&fields={FIELDS}",
            headers=HDR, timeout=aiohttp.ClientTimeout(total=30), ssl=False) as r:
            batch = await r.json(content_type=None)
        if not batch or not isinstance(batch, list): break
        all_cos.extend(batch)
        log.info(f"  Scaricati {len(all_cos):,}...")
        if len(batch) < 500: break
        skip += 500
        await asyncio.sleep(0.3)
    return all_cos

def richness(c):
    ai = c.get("ai_stack") or []
    if isinstance(ai, str):
        try: ai = json.loads(ai)
        except: ai = []
    return (
        bool(c.get("description")) * 10 +
        bool(c.get("employee_count")) * 5 +
        bool(c.get("org_chart") and c["org_chart"] not in [[], "[]", None]) * 5 +
        bool(c.get("industry")) * 3 +
        bool(c.get("logo_url")) * 2 +
        len(ai) * 2 +
        float(c.get("ai_adoption_score") or 0) * 0.1
    )

def norm(s):
    return (s or "").strip().lower().replace(" ","").replace("-","").replace(".","")

async def main():
    # Health server
    from aiohttp import web as aio_web
    app_web = aio_web.Application()
    async def health(req): return aio_web.Response(text="OK")
    app_web.router.add_get("/", health)
    app_web.router.add_get("/health", health)
    runner = aio_web.AppRunner(app_web)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await aio_web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"Healthcheck :{port}")

    connector = aiohttp.TCPConnector(limit=5, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        log.info("=== BASE44 DEDUP START ===")
        all_cos = await fetch_all(session)
        log.info(f"Totale scaricati: {len(all_cos):,}")

        # Raggruppa per nome normalizzato
        by_name = defaultdict(list)
        for c in all_cos:
            k = norm(c.get("name",""))
            if k: by_name[k].append(c)

        dup_groups = {k:v for k,v in by_name.items() if len(v) > 1}
        total_extra = sum(len(v)-1 for v in dup_groups.values())
        log.info(f"Gruppi duplicati: {len(dup_groups):,} | Record extra: {total_extra:,}")

        deleted = merged = errors = 0
        for i, (name_key, records) in enumerate(dup_groups.items()):
            sorted_r = sorted(records, key=richness, reverse=True)
            keeper   = sorted_r[0]
            dupes    = sorted_r[1:]

            # Merge verso il keeper
            patch = {}
            for d in dupes:
                if not keeper.get("description") and d.get("description"):
                    patch["description"] = d["description"]
                if not keeper.get("employee_count") and d.get("employee_count"):
                    patch["employee_count"] = d["employee_count"]
                if not keeper.get("industry") and d.get("industry"):
                    patch["industry"] = d["industry"]
                if not keeper.get("logo_url") and d.get("logo_url"):
                    patch["logo_url"] = d["logo_url"]
                d_ai = d.get("ai_stack") or []
                k_ai = keeper.get("ai_stack") or []
                if isinstance(d_ai, str):
                    try: d_ai = json.loads(d_ai)
                    except: d_ai = []
                if isinstance(k_ai, str):
                    try: k_ai = json.loads(k_ai)
                    except: k_ai = []
                if len(d_ai) > len(k_ai):
                    patch["ai_stack"]          = d_ai
                    patch["ai_adoption_score"] = float(d.get("ai_adoption_score") or 0)
            
            if patch:
                try:
                    async with session.put(f"{BASE_URL}/{keeper['id']}",
                        headers=HDR, json=patch, timeout=aiohttp.ClientTimeout(total=15), ssl=False) as r:
                        if r.status == 200: merged += 1
                except: pass

            # Elimina duplicati con throttle
            for d in dupes:
                try:
                    async with session.delete(f"{BASE_URL}/{d['id']}",
                        headers=HDR, timeout=aiohttp.ClientTimeout(total=10), ssl=False) as r:
                        if r.status in (200,204): deleted += 1
                        else: errors += 1
                except: errors += 1
                await asyncio.sleep(0.06)

            if (i+1) % 100 == 0:
                log.info(f"  [{i+1}/{len(dup_groups)}] eliminati={deleted:,} merged={merged} err={errors}")

        log.info(f"=== DEDUP COMPLETATO ===")
        log.info(f"  Eliminati:  {deleted:,}")
        log.info(f"  Merge dati: {merged:,}")
        log.info(f"  Errori:     {errors:,}")
        log.info(f"  Record finali stimati: ~{len(all_cos) - deleted:,}")
        log.info("Dedup done — container idle")
        
        # Rimane attivo per healthcheck
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

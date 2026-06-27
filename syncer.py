#!/usr/bin/env python3
"""AgentSignal Syncer v2 — Postgres Railway to Base44, persistent session"""
import asyncio, aiohttp, asyncpg, os, json, logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
BASE44_TOKEN = os.environ["BASE44_TOKEN"]
APP_ID       = os.environ["APP_ID"]
PORT         = int(os.environ.get("PORT","8080"))
BASE_URL     = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company"
HW           = {"api-key": BASE44_TOKEN, "Content-Type": "application/json"}

RATE_DELAY   = 11    # 1 push every 11s = ~5.4/min (Base44 limit ~6/min)
BATCH_SIZE   = 50
pushed_total = 0
errors_total = 0
start_time   = time.time()


def build_payload(r):
    def sj(v, fallback=None):
        """Parse JSON string → list/dict, oppure restituisce il valore."""
        if v is None: return fallback
        if isinstance(v, (list, dict)): return v
        if isinstance(v, str):
            try: return json.loads(v)
            except: return fallback
        return fallback

    def sstr(v):
        """Stringa o None."""
        return str(v).strip() if v and str(v).strip() else None

    def sfloat(v):
        try: return float(v) if v is not None else 0.0
        except: return 0.0

    # org_chart: lista di people [{name, title, linkedin}]
    org = sj(r.get("org_chart"), [])

    # Estrai CEO dal org_chart
    ceo_name = None
    for person in (org or []):
        title = str(person.get("title","")).lower()
        if any(t in title for t in ["ceo","chief executive","founder","co-founder"]):
            ceo_name = person.get("name","")
            break

    p = {
        # Dati aziendali base
        "name":                    sstr(r.get("name")) or r["domain"].split(".")[0].title(),
        "website":                 sstr(r.get("website")) or "https://" + r["domain"],
        "source":                  sstr(r.get("source")) or "railway",
        "description":             sstr(r.get("description")),
        "industry":                sstr(r.get("industry")),
        "country":                 sstr(r.get("country")),
        "logo_url":                sstr(r.get("logo_url")),
        "employee_count":          int(r["employee_count"]) if r.get("employee_count") else None,
        "revenue_range":           sstr(r.get("revenue_range")),

        # Stack tecnologico
        "ai_stack":                sj(r.get("ai_stack"), []),
        "tech_stack":              sj(r.get("tech_stack"), []),

        # Org chart (people con ruoli)
        "org_chart":               org,

        # Scores AI & digitali
        "ai_adoption_score":       sfloat(r.get("ai_score")),
        "ai_maturity_score":       sfloat(r.get("maturity_score")),
        "ai_buying_intent_score":  sfloat(r.get("intent_score")),
        "ai_transformation_score": sfloat(r.get("maturity_score")),
        "cloud_score":             sfloat(r.get("cloud_score")),
        "automation_score":        sfloat(r.get("automation_score")),
        "developer_score":         sfloat(r.get("developer_score")),
        "security_score":          sfloat(r.get("security_score")),
        "growth_score":            sfloat(r.get("growth_score")),
        "innovation_score":        sfloat(r.get("innovation_score")),
        "commerce_score":          sfloat(r.get("commerce_score")),
        "tech_gap_score":          sfloat(r.get("tech_gap_score")),

        # Meta
        "global_rank":             int(r["global_rank"]) if r.get("global_rank") else None,
        "ats_hiring_signals":      f"CEO: {ceo_name}" if ceo_name else None,
    }
    if r.get("last_scan_date"):
        p["last_scan_date"] = r["last_scan_date"].isoformat() if hasattr(r["last_scan_date"], "isoformat") else str(r["last_scan_date"])

    # Filtra valori vuoti ma mantieni 0.0 per gli scores
    return {k: v for k, v in p.items()
            if v is not None and v != [] and v != {}}


async def push_one(session, pool, r):
    global pushed_total, errors_total
    payload = build_payload(r)
    domain  = r["domain"]
    b44_id  = r.get("base44_id")
    
    # UPSERT GUARD: se non abbiamo base44_id, cerca per website prima di fare POST
    if not b44_id:
        name_q = (r.get("name") or domain.split(".")[0]).replace(" ","+")
        try:
            async with session.get(
                f"{BASE_URL}?limit=3&fields=id,name,website",
                headers={"api-key": HW["api-key"]},
                params={"name_filter": r.get("name","")},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as gr:
                existing = await gr.json(content_type=None) if gr.status == 200 else []
            # Cerca match per website
            my_site = payload.get("website","").rstrip("/").lower()
            for ex in (existing or []):
                ex_site = (ex.get("website","") or "").rstrip("/").lower()
                if ex_site and ex_site == my_site:
                    b44_id = ex["id"]
                    async with pool.acquire() as c:
                        await c.execute("UPDATE companies SET base44_id=$1 WHERE domain=$2", b44_id, domain)
                    break
        except: pass
    
    try:
        if b44_id:
            async with session.put(
                f"{BASE_URL}/{b44_id}", headers=HW, json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.ok:
                    async with pool.acquire() as c:
                        await c.execute(
                            "UPDATE companies SET last_push_date=NOW() WHERE domain=$1", domain)
                    pushed_total += 1
                    return True
                elif resp.status == 404:
                    async with pool.acquire() as c:
                        await c.execute(
                            "UPDATE companies SET base44_id=NULL WHERE domain=$1", domain)
                elif resp.status == 429:
                    log.warning("429 — sleep 60s")
                    await asyncio.sleep(60)
        else:
            async with session.post(
                BASE_URL, headers=HW, json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.ok:
                    created = await resp.json()
                    new_id  = created.get("id", "")
                    async with pool.acquire() as c:
                        await c.execute(
                            "UPDATE companies SET base44_id=$1, last_push_date=NOW() WHERE domain=$2",
                            new_id, domain)
                    pushed_total += 1
                    return True
                elif resp.status == 429:
                    log.warning("429 — sleep 60s")
                    await asyncio.sleep(60)
                else:
                    body = await resp.text()
                    log.warning(f"POST {domain}: HTTP {resp.status} — {body[:100]}")
    except Exception as e:
        log.warning(f"push error {domain}: {type(e).__name__}: {e}")
    errors_total += 1
    return False


async def load_batch(pool):
    async with pool.acquire() as c:
        rows = await c.fetch("""
            SELECT id, domain, name, website, source, global_rank,
                   ai_stack, tech_stack, ai_score, maturity_score,
                   cloud_score, automation_score, developer_score,
                   security_score, growth_score, innovation_score,
                   intent_score, commerce_score, tech_gap_score,
                   base44_id, last_scan_date, last_push_date
            FROM companies
            WHERE last_scan_date IS NOT NULL
              AND (base44_id IS NULL OR last_push_date < last_scan_date)
            ORDER BY ai_score DESC NULLS LAST, global_rank ASC NULLS LAST
            LIMIT $1
        """, BATCH_SIZE)
    return [dict(r) for r in rows]


async def healthcheck_server(pool):
    async def handler(reader, writer):
        try:
            await reader.read(512)
            async with pool.acquire() as c:
                total   = await c.fetchval("SELECT COUNT(*) FROM companies") or 0
                on_b44  = await c.fetchval(
                    "SELECT COUNT(*) FROM companies WHERE base44_id IS NOT NULL") or 0
                pending = await c.fetchval(
                    "SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL AND base44_id IS NULL") or 0
            uptime = (time.time() - start_time) / 3600
            rate_h = int(pushed_total / max(uptime, 0.001))
            body = json.dumps({
                "status": "ok",
                "db_total": total,
                "on_base44": on_b44,
                "pending_push": pending,
                "session_pushed": pushed_total,
                "session_errors": errors_total,
                "rate_per_hour": rate_h,
                "uptime_h": round(uptime, 2),
            }).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(body)).encode() + b"\r\n\r\n" + body
            )
            await writer.drain()
        except Exception:
            pass
        finally:
            try: writer.close()
            except: pass

    server = await asyncio.start_server(handler, "0.0.0.0", PORT)
    log.info(f"Healthcheck on :{PORT}")
    async with server:
        await server.serve_forever()


async def sync_loop(pool):
    """
    Sessione aiohttp PERSISTENTE per tutto il ciclo di vita del processo.
    Nessun 'async with session' — la sessione resta aperta per sempre.
    Questo risolve il bug 'Session is closed'.
    """
    # ── DEDUP all'avvio: elimina duplicati Base44 ───────────────────────

    log.info(f"Sync loop start | rate=1/{RATE_DELAY}s | batch={BATCH_SIZE}")

    # Connector con keepalive lungo
    connector = aiohttp.TCPConnector(
        limit=1,
        keepalive_timeout=120,
        enable_cleanup_closed=True,
    )
    session = aiohttp.ClientSession(connector=connector)

    cycle = 0
    try:
        while True:
            batch = await load_batch(pool)

            if not batch:
                log.info(
                    f"Nothing to push — sleep 5min | "
                    f"total_pushed={pushed_total:,} errors={errors_total}"
                )
                await asyncio.sleep(300)
                continue

            cycle += 1
            log.info(f"[C{cycle}] {len(batch)} records to push | pushed_so_far={pushed_total:,}")

            ok_n = 0
            for i, r in enumerate(batch):
                ok = await push_one(session, pool, r)
                if ok:
                    ok_n += 1
                await asyncio.sleep(RATE_DELAY)
                if (i + 1) % 10 == 0:
                    log.info(f"  [{i+1}/{len(batch)}] ok={ok_n} total={pushed_total:,}")

            log.info(
                f"[C{cycle}] done ok={ok_n}/{len(batch)} | "
                f"total_pushed={pushed_total:,} errors={errors_total}"
            )

            # Dedup ogni 5 cicli (≈45 min) — rimuove duplicati dal DB Base44
            if cycle % 5 == 0:
                log.info("=== DEDUP START (ogni 5 cicli) ===")
                try:
                    await _dedup_b44(session)
                except Exception as _de:
                    log.warning(f"Dedup error: {_de}")
                log.info("=== DEDUP DONE ===")

    finally:
        await session.close()
        await connector.close()


async def _dedup_b44(session):
    """Scarica tutti i record Base44, trova duplicati per nome, elimina i meno ricchi."""
    from collections import defaultdict
    import re
    HDR = {"api-key": B44_API_KEY, "Content-Type": "application/json"}

    def norm(s): return re.sub(r"[\s\-\.]", "", (s or "").strip().lower())
    def richness(c):
        ai = c.get("ai_stack") or []
        if isinstance(ai, str):
            try: ai = json.loads(ai)
            except: ai = []
        return (bool(c.get("description"))*10 + bool(c.get("employee_count"))*5 +
                bool(c.get("industry"))*3 + bool(c.get("logo_url"))*2 +
                len(ai)*2 + float(c.get("ai_adoption_score") or 0)*0.1)

    # Scarica tutti i record paginati
    all_cos, skip = [], 0
    while True:
        try:
            async with session.get(
                f"{BASE_URL}?limit=500&skip={skip}&fields=id,name,website,ai_adoption_score,description,employee_count,industry,ai_stack,logo_url",
                headers=HDR, timeout=aiohttp.ClientTimeout(total=30), ssl=False
            ) as r:
                batch = await r.json(content_type=None)
            if not batch or not isinstance(batch, list): break
            all_cos.extend(batch)
            if len(batch) < 500: break
            skip += 500
            await asyncio.sleep(0.3)
        except Exception as e:
            log.warning(f"Dedup fetch err: {e}")
            break

    log.info(f"  Dedup: {len(all_cos):,} record scaricati")

    by_name = defaultdict(list)
    for c in all_cos:
        k = norm(c.get("name", ""))
        if k: by_name[k].append(c)

    dup_groups = {k: v for k, v in by_name.items() if len(v) > 1}
    extra = sum(len(v)-1 for v in dup_groups.values())
    log.info(f"  Dedup: {len(dup_groups)} gruppi | {extra} duplicati da rimuovere")
    if not dup_groups: return

    deleted = merged = 0
    for records in dup_groups.values():
        sorted_r = sorted(records, key=richness, reverse=True)
        keeper, dupes = sorted_r[0], sorted_r[1:]

        patch = {}
        for d in dupes:
            if not keeper.get("description") and d.get("description"): patch["description"] = d["description"]
            if not keeper.get("employee_count") and d.get("employee_count"): patch["employee_count"] = d["employee_count"]
            if not keeper.get("industry") and d.get("industry"): patch["industry"] = d["industry"]
            if not keeper.get("logo_url") and d.get("logo_url"): patch["logo_url"] = d["logo_url"]

        if patch:
            try:
                async with session.put(f"{BASE_URL}/{keeper['id']}", headers=HDR,
                    json=patch, timeout=aiohttp.ClientTimeout(total=12), ssl=False) as r:
                    if r.status == 200: merged += 1
            except: pass

        for d in dupes:
            try:
                async with session.delete(f"{BASE_URL}/{d['id']}", headers=HDR,
                    timeout=aiohttp.ClientTimeout(total=8), ssl=False) as r:
                    if r.status in (200, 204): deleted += 1
            except: pass
            await asyncio.sleep(0.08)

    log.info(f"  Dedup: eliminati={deleted} merged={merged} ✓")

    def norm(s): return re.sub(r"[\s\-\.]","", (s or "").strip().lower())
    def richness(c):
        ai = c.get("ai_stack") or []
        if isinstance(ai, str):
            try: ai = json.loads(ai)
            except: ai = []
        return (bool(c.get("description"))*10 + bool(c.get("employee_count"))*5 +
                bool(c.get("org_chart") and c["org_chart"] not in [[],"[]",None,""])*5 +
                bool(c.get("industry"))*3 + bool(c.get("logo_url"))*2 +
                len(ai)*2 + float(c.get("ai_adoption_score") or 0)*0.1)

    # Scarica tutti i record paginati
    all_cos, skip = [], 0
    while True:
        try:
            async with session.get(
                f"{B44_URL}?limit=500&skip={skip}&fields=id,name,website,ai_adoption_score,description,employee_count,org_chart,industry,ai_stack,logo_url",
                headers=B44_HDR, timeout=aiohttp.ClientTimeout(total=30), ssl=False
            ) as r:
                batch = await r.json(content_type=None)
            if not batch or not isinstance(batch, list): break
            all_cos.extend(batch)
            if len(batch) < 500: break
            skip += 500
            await asyncio.sleep(0.2)
        except Exception as e:
            log.warning(f"Dedup fetch err: {e}")
            break

    log.info(f"  Dedup: {len(all_cos):,} record scaricati")
    by_name = defaultdict(list)
    for c in all_cos:
        k = norm(c.get("name",""))
        if k: by_name[k].append(c)

    dup_groups = {k:v for k,v in by_name.items() if len(v) > 1}
    extra = sum(len(v)-1 for v in dup_groups.values())
    log.info(f"  Dedup: {len(dup_groups)} gruppi | {extra} record extra")
    if not dup_groups:
        log.info("  Dedup: nessun duplicato trovato ✓")
        return

    deleted = merged = 0
    for records in dup_groups.values():
        sorted_r = sorted(records, key=richness, reverse=True)
        keeper, dupes = sorted_r[0], sorted_r[1:]

        patch = {}
        for d in dupes:
            if not keeper.get("description") and d.get("description"): patch["description"] = d["description"]
            if not keeper.get("employee_count") and d.get("employee_count"): patch["employee_count"] = d["employee_count"]
            if not keeper.get("industry") and d.get("industry"): patch["industry"] = d["industry"]
            if not keeper.get("logo_url") and d.get("logo_url"): patch["logo_url"] = d["logo_url"]
            d_ai = d.get("ai_stack") or []; k_ai = keeper.get("ai_stack") or []
            if isinstance(d_ai,str):
                try: d_ai=json.loads(d_ai)
                except: d_ai=[]
            if isinstance(k_ai,str):
                try: k_ai=json.loads(k_ai)
                except: k_ai=[]
            if len(d_ai) > len(k_ai):
                patch["ai_stack"] = d_ai
                patch["ai_adoption_score"] = float(d.get("ai_adoption_score") or 0)

        if patch:
            try:
                async with session.put(f"{B44_URL}/{keeper['id']}", headers=B44_HDR,
                    json=patch, timeout=aiohttp.ClientTimeout(total=12), ssl=False) as r:
                    if r.status == 200: merged += 1
            except: pass

        for d in dupes:
            try:
                async with session.delete(f"{B44_URL}/{d['id']}", headers=B44_HDR,
                    timeout=aiohttp.ClientTimeout(total=8), ssl=False) as r:
                    if r.status in (200,204): deleted += 1
            except: pass
            await asyncio.sleep(0.05)

    log.info(f"  Dedup: eliminati={deleted} merged={merged} ✓")

async def main():
    log.info("=== AgentSignal Syncer v2 — Railway -> Base44 ===")
    pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=5, command_timeout=30
    )
    async with pool.acquire() as c:
        total   = await c.fetchval("SELECT COUNT(*) FROM companies") or 0
        scanned = await c.fetchval(
            "SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL") or 0
        on_b44  = await c.fetchval(
            "SELECT COUNT(*) FROM companies WHERE base44_id IS NOT NULL") or 0
    log.info(
        f"Postgres OK | total={total:,} scanned={scanned:,} "
        f"on_base44={on_b44:,} to_push={scanned - on_b44:,}"
    )
    await asyncio.gather(
        healthcheck_server(pool),
        sync_loop(pool),
    )


if __name__ == "__main__":
    # Dedup prioritario se richiesto
    if os.environ.get("DEDUP_NOW","0") == "1":
        import importlib.util, sys
        log.info("=== DEDUP_NOW=1 → avvio dedup Base44 ===")
        spec = importlib.util.spec_from_file_location("dedup_b44","./dedup_b44.py")
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        asyncio.run(mod.main())
        sys.exit(0)
    mode = os.environ.get("MODE","syncer")
    if mode == "enricher":
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location("enricher_worker","./enricher_worker.py")
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        asyncio.run(mod.main())
    else:
        asyncio.run(main())
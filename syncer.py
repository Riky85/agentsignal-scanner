from datetime import datetime
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
BULK_URL     = f"https://app.base44.com/api/apps/{APP_ID}/entities/Company/bulk"
HW           = {"api-key": BASE44_TOKEN, "Content-Type": "application/json"}

RATE_DELAY   = 2     # 2s tra bulk push
BATCH_SIZE   = 200
BULK_SIZE    = 50
pushed_total = 0
errors_total = 0
start_time   = time.time()



# ── Buying Intent: converti raw keys → etichette leggibili ────────────────────
_BI_LABELS = {
    "has_ai_page":        "🤖 AI Product Page detected",
    "has_careers":        "👥 Active Hiring",
    "has_api_docs":       "📡 Public API / Developer Docs",
    "ai_hiring":          "🔍 Hiring AI/ML Engineers",
    "ai_stack_detected":  "⚡ AI Stack in Production",
    "ai_blog":            "📝 AI Blog Activity",
    "ai_product":         "🚀 AI Product Feature",
    "ai_docs":            "📚 AI in Documentation",
    "ai_changelog":       "🔄 AI Changelog Updates",
    "ai_integration":     "🔗 AI Integration Partner",
}

def _normalize_buying_intent(signals: list) -> list:
    """Converti raw keys (has_ai_page) in etichette leggibili per la UI."""
    result = []
    for s in (signals or []):
        if isinstance(s, str):
            result.append(_BI_LABELS.get(s, s))   # se non trovata, usa la key originale
        elif isinstance(s, dict):
            result.append(s)   # già formattato
    return result

def build_payload(r):
    """Payload per Base44 — mapping corretto dai campi Railway DB"""
    def _list(v):
        if v is None: return []
        if isinstance(v, list): return v
        if isinstance(v, str):
            try:
                r2 = json.loads(v)
                return r2 if isinstance(r2, list) else []
            except: return []
        return []

    def _dict(v):
        if isinstance(v, dict): return v
        if isinstance(v, str):
            try: return json.loads(v)
            except: return {}
        return {}

    def sstr(v):
        s = str(v).strip() if v is not None else ""
        return s if s and s not in ("None","null","[]","{}","") else None

    def sint(v, cap=100):
        try: return max(0, min(cap, int(float(v)))) if v is not None else 0
        except: return 0

    ts = _list(r.get("tech_stack"))
    if not ts:
        td = _dict(r.get("technology_dna") or r.get("biz_stack"))
        ts = [t for tools in td.values() for t in (tools if isinstance(tools, list) else [])]

    ai = _list(r.get("ai_stack"))

    digital_maturity = sint(r.get("maturity_score") or r.get("digital_maturity") or 0)
    ai_readiness     = sint(r.get("ai_score") or 0)
    buying_intent    = sint(r.get("intent_score") or 0)

    website = sstr(r.get("website")) or "https://" + r["domain"]

    return {
        "name":                   sstr(r.get("name")) or r["domain"].split(".")[0].title(),
        "website":                website,
        "source":                 "railway_scan",
        "description":            sstr(r.get("description")),
        "industry":               sstr(r.get("industry")),
        "country":                sstr(r.get("country")),
        "logo_url":               sstr(r.get("logo_url")),
        "linkedin_url":           sstr(r.get("linkedin_url")),
        "employee_count":         int(r["employee_count"]) if r.get("employee_count") else None,
        "revenue_range":          sstr(r.get("revenue_range")),
        "global_rank":            int(r["global_rank"]) if r.get("global_rank") else None,
        "tech_stack":             ts,
        "ai_stack":               ai,
        "buying_intent_signals":  _normalize_buying_intent(_list(r.get("buying_intent_signals"))),
        "acquisition_signals":    _list(r.get("acquisition_signals")),
        "org_chart":              _list(r.get("org_chart")),
        "ai_adoption_score":      ai_readiness,
        "ai_maturity_score":      min(5, digital_maturity // 20),
        "ai_buying_intent_score": buying_intent,
        "ai_transformation_score":ai_readiness,
        "cloud_score":            sint(r.get("cloud_score") or 0),
        "automation_score":       sint(r.get("automation_score") or 0),
        "developer_score":        sint(r.get("developer_score") or 0),
        "security_score":         sint(r.get("security_score") or 0),
        "growth_score":           sint(r.get("growth_score") or 0),
        "innovation_score":       sint(r.get("innovation_score") or 0),
        "commerce_score":         sint(r.get("commerce_score") or 0),
        "tech_gap_score":         sint(r.get("tech_gap_score") or 0),
    }


# ── ScanHistory: cronologia snapshot per ogni azienda ────────────────────────
SCAN_HISTORY_URL = BASE_URL.replace("/Company", "/ScanHistory")

async def write_scan_history(session, company_id: str, r: dict, payload: dict):
    """Salva uno snapshot su ScanHistory dopo ogni push riuscito verso Company."""
    try:
        def _to_list(v):
            if isinstance(v, list): return v
            if isinstance(v, str):
                try:    return json.loads(v)
                except: return [v] if v else []
            return []

        tech_stack  = _to_list(r.get("tech_stack") or r.get("ai_stack"))
        ai_stack    = _to_list(r.get("ai_stack"))
        bi_signals  = _to_list(r.get("buying_intent_signals"))
        acq_signals = _to_list(r.get("acquisition_signals"))

        snap = {
            "company_id":            company_id,
            "website":               r.get("domain",""),
            "scanned_at":            (str(r.get("last_scan_date")) if r.get("last_scan_date") else datetime.utcnow().isoformat()),
            "tech_stack":            tech_stack,
            "ai_stack":              ai_stack,
            "buying_intent_signals": bi_signals,
            "acquisition_signals":   acq_signals,
            "digital_maturity_score": int(payload.get("ai_adoption_score") or 0),
            "ai_readiness_score":    int(payload.get("ai_maturity_score") or 0),
            "automation_score":      int(payload.get("automation_score") or 0),
            "cloud_score":           int(payload.get("cloud_score") or 0),
            "commerce_score":        int(payload.get("commerce_score") or 0),
            "buying_intent_score":   int(payload.get("ai_buying_intent_score") or 0),
            "employee_count":        int(r.get("employee_count") or 0) or None,
            "description":           (r.get("description") or "")[:500],
            "changes_vs_previous":   [],  # TODO: diff con scan precedente
        }
        # Serializza datetime e altri tipi non-JSON
        def _jsonify(v):
            if hasattr(v, "isoformat"): return v.isoformat()
            if isinstance(v, (list, tuple)): return [_jsonify(i) for i in v]
            if isinstance(v, dict): return {k: _jsonify(vv) for k,vv in v.items()}
            return v
        snap = {k: _jsonify(v) for k, v in snap.items()}
        async with session.post(SCAN_HISTORY_URL, headers=HW, json=snap,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.ok:
                log.info(f"ScanHistory ✓ {r.get('domain','')}")
            else:
                body = await resp.text()
                log.warning(f"ScanHistory ERR {r.get('domain')}: {resp.status} {body[:80]}")
    except Exception as e:
        log.warning(f"write_scan_history error {r.get('domain','')}: {type(e).__name__}: {e}")


async def push_bulk(session, pool, records):
    """Bulk insert con upsert guard per website — zero duplicati"""
    global pushed_total, errors_total
    if not records: return 0, 0

    # Separa record nuovi (base44_id=None) da aggiornamenti (base44_id noto)
    new_recs    = [r for r in records if not r.get("base44_id")]
    update_recs = [r for r in records if r.get("base44_id")]

    inserted_n = 0; updated_n = 0; err_n = 0

    # INSERT nuovi via /bulk
    if new_recs:
        payloads = [build_payload(r) for r in new_recs]
        try:
            async with session.post(BULK_URL, headers=HW, json=payloads,
                                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    inserted = await resp.json(content_type=None)
                    inserted_ids = [item.get("id") for item in (inserted or []) if item.get("id")]
                    async with pool.acquire() as c:
                        for r, iid in zip(new_recs, inserted_ids):
                            if iid:
                                await c.execute(
                                    "UPDATE companies SET base44_id=$1, last_push_date=NOW() WHERE domain=$2",
                                    iid, r["domain"])
                    inserted_n = len(inserted_ids)
                    pushed_total += inserted_n
                else:
                    body = await resp.text()
                    log.warning(f"Bulk POST HTTP {resp.status}: {body[:100]}")
                    err_n += len(new_recs)
        except Exception as e:
            log.warning(f"Bulk POST ERR: {e}")
            err_n += len(new_recs)

    # UPDATE esistenti via PUT singolo (hanno già base44_id)
    for r in update_recs:
        payload = build_payload(r)
        try:
            async with session.put(f"{BASE_URL}/{r['base44_id']}", headers=HW, json=payload,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    updated_n += 1; pushed_total += 1
                    async with pool.acquire() as c:
                        await c.execute("UPDATE companies SET last_push_date=NOW() WHERE domain=$1", r["domain"])
                else:
                    err_n += 1
        except Exception as e:
            err_n += 1

    errors_total += err_n
    return inserted_n + updated_n, err_n

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
                    pass  # ScanHistory disabled — entity not in AgentSignal app
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
                    pass  # ScanHistory disabled — entity not in AgentSignal app
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
                   ai_stack, tech_stack, biz_stack, technology_dna,
                   ai_score, maturity_score,
                   cloud_score, automation_score, developer_score,
                   security_score, growth_score, innovation_score,
                   intent_score, commerce_score, tech_gap_score,
                   description, industry, employee_count, revenue_range,
                   country, logo_url, linkedin_url, org_chart,
                   ats_documentation, ats_product_signals,
                   base44_id, last_scan_date, last_push_date
            FROM companies
            WHERE last_scan_date IS NOT NULL
              AND base44_id IS NULL
              AND COALESCE(scan_errors, 0) < 5
            ORDER BY
              CASE WHEN jsonb_array_length(COALESCE(tech_stack,'[]'::jsonb)) > 0 THEN 0 ELSE 1 END,
              ai_score DESC NULLS LAST,
              global_rank ASC NULLS LAST
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
            # Bulk push: raggruppa in chunk da BULK_SIZE e invia in una sola chiamata
            for chunk_start in range(0, len(batch), BULK_SIZE):
                chunk = batch[chunk_start:chunk_start + BULK_SIZE]
                ok_chunk, err_chunk = await push_bulk(session, pool, chunk)
                ok_n += ok_chunk
                await asyncio.sleep(RATE_DELAY)
                log.info(f"  [{min(chunk_start+BULK_SIZE,len(batch))}/{len(batch)}] ok={ok_n} total={pushed_total:,}")

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
    HDR = {"api-key": HW["api-key"], "Content-Type": "application/json"}

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
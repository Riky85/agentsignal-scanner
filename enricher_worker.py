#!/usr/bin/env python3
"""
AgentSignal Enricher Worker — Railway
Arricchisce le aziende su PostgreSQL con description, CEO, employees, revenue
usando DuckDuckGo Instant API + Schema.org + Clearbit Logo.
Poi aggiorna Base44 via PUT selettivo.
"""
import asyncio, aiohttp, asyncpg, json, re, os, logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

DB_URL    = os.environ["DATABASE_URL"]
B44_TOKEN = os.environ["B44_SERVICE_TOKEN"]
B44_APP   = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE_URL  = f"https://app.base44.com/api/apps/{B44_APP}/entities/Company"
HDR_B44   = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
DDG_HDR   = {"User-Agent": "Mozilla/5.0 (compatible; AgentSignal-Enricher/1.0)"}
BATCH     = 200    # aziende per ciclo
DELAY     = 0.3    # secondi tra richieste DDG
PUSH_DELAY= 12     # secondi tra PUT su Base44


async def ddg_lookup(session, name):
    """DuckDuckGo Instant Answer — ritorna dict con campi aziendali."""
    try:
        async with session.get(
            "https://api.duckduckgo.com/",
            params={"q": name, "format": "json", "no_html": "1", "skip_disambig": "1"},
            headers=DDG_HDR, timeout=aiohttp.ClientTimeout(total=8), ssl=False
        ) as r:
            if r.status != 200: return {}
            d = await r.json(content_type=None)
    except Exception as e:
        log.debug(f"DDG error {name}: {e}")
        return {}

    result = {}
    abstract = (d.get("AbstractText") or d.get("Abstract") or "").strip()
    if len(abstract) > 40:
        result["description"] = abstract[:500]

    infobox = d.get("Infobox") or {}
    items   = infobox.get("content", []) if isinstance(infobox, dict) else []
    for item in items:
        if not isinstance(item, dict): continue
        label = item.get("label", "").lower()
        value = str(item.get("value", "")).strip()
        if not value: continue
        if "founded" in label:
            y = re.search(r"\d{4}", value)
            if y: result["founded_year"] = y.group()
        elif "revenue" in label:
            result["revenue_range"] = value[:80]
        elif "employee" in label:
            n = re.search(r"[\d,]+", value.replace(",", ""))
            if n:
                try: result["employee_count"] = int(n.group().replace(",", ""))
                except: pass
        elif "industry" in label:
            result["industry"] = value[:80]
        elif "key people" in label or "ceo" in label or "founder" in label:
            result.setdefault("key_people_raw", []).append(value[:100])
        elif "headquarters" in label:
            parts = value.split(",")
            result["country"] = parts[-1].strip()[:50]

    # Estrai CEO
    org_chart = []
    for raw in result.pop("key_people_raw", []):
        m = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+)", raw)
        title_m = re.search(r"\(([^)]+)\)", raw)
        if m:
            name_found = m.group(1)
            title      = title_m.group(1) if title_m else "Executive"
            org_chart.append({"name": name_found, "title": title})
    if org_chart:
        result["org_chart"] = json.dumps(org_chart)
        # CEO per ats_hiring_signals
        for p in org_chart:
            t = p["title"].lower()
            if any(x in t for x in ["ceo","chief exec","founder"]):
                result["ats_hiring_signals"] = f"CEO: {p['name']} ({p['title']})"
                break

    return result


async def schema_org_lookup(session, domain):
    """Estrae Schema.org dal sito aziendale."""
    result = {}
    try:
        async with session.get(
            f"https://{domain}", headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=7), ssl=False, allow_redirects=True
        ) as r:
            if r.status != 200: return {}
            html = (await r.read()).decode("utf-8", errors="replace")[:200_000]
    except: return {}

    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    ):
        try:
            d = json.loads(m.group(1).strip())
            nodes = d if isinstance(d, list) else d.get("@graph", [d])
            for node in nodes:
                t = str(node.get("@type", ""))
                if not any(x in t for x in ("Organization","Corporation","Company","LocalBusiness")):
                    continue
                desc = node.get("description") or node.get("slogan", "")
                if desc and not result.get("description"): result["description"] = str(desc)[:500]
                fd = node.get("foundingDate", "")
                if fd and not result.get("founded_year"): result["founded_year"] = str(fd)[:4]
                logo = node.get("logo")
                if logo and not result.get("logo_url"):
                    result["logo_url"] = logo.get("url","") if isinstance(logo,dict) else str(logo)
                same_as = node.get("sameAs") or []
                if isinstance(same_as, str): same_as = [same_as]
                for url in same_as:
                    if "linkedin.com/company" in url and not result.get("linkedin_url"):
                        result["linkedin_url"] = url
        except: pass

    return result


async def enrich_one(session, pg_pool, row):
    """Arricchisce un singolo record nel DB locale e su Base44."""
    domain  = row["domain"]
    name    = row.get("name") or domain.split(".")[0].title()
    b44_id  = row.get("base44_id")

    # 1. Prova Schema.org dalla homepage
    schema = await schema_org_lookup(session, domain)
    await asyncio.sleep(DELAY)

    # 2. DuckDuckGo per dati strutturati
    ddg = await ddg_lookup(session, name)
    await asyncio.sleep(DELAY)

    # 3. Merge (schema.org ha priorità)
    enriched = {**ddg, **schema}
    if not enriched.get("logo_url"):
        enriched["logo_url"] = f"https://logo.clearbit.com/{domain}"

    if not enriched: return

    # 4. Aggiorna DB locale
    fields = ["description","industry","employee_count","revenue_range",
              "org_chart","logo_url","linkedin_url","founded_year"]
    updates = {k: enriched[k] for k in fields if k in enriched}
    if updates:
        set_parts = [f"{k} = ${i+2}" for i, k in enumerate(updates)]
        vals = [domain] + list(updates.values())
        async with pg_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE companies SET {', '.join(set_parts)} WHERE domain = $1",
                *vals
            )

    # 5. Push su Base44 se hai un b44_id
    if not b44_id: return
    b44_payload = {}
    if enriched.get("description"):    b44_payload["description"]       = enriched["description"]
    if enriched.get("employee_count"): b44_payload["employee_count"]    = int(enriched["employee_count"])
    if enriched.get("revenue_range"):  b44_payload["revenue_range"]     = enriched["revenue_range"]
    if enriched.get("industry"):       b44_payload["industry"]          = enriched["industry"]
    if enriched.get("country"):        b44_payload["country"]           = enriched["country"]
    if enriched.get("logo_url"):       b44_payload["logo_url"]          = enriched["logo_url"]
    if enriched.get("linkedin_url"):   b44_payload["linkedin_url"]      = enriched["linkedin_url"]
    if enriched.get("org_chart"):
        try: b44_payload["org_chart"] = json.loads(enriched["org_chart"]) if isinstance(enriched["org_chart"],str) else enriched["org_chart"]
        except: pass
    if enriched.get("ats_hiring_signals"):
        b44_payload["ats_hiring_signals"] = enriched["ats_hiring_signals"]

    if not b44_payload: return

    try:
        async with session.put(
            f"{BASE_URL}/{b44_id}", headers=HDR_B44, json=b44_payload,
            timeout=aiohttp.ClientTimeout(total=20), ssl=False
        ) as resp:
            if resp.status == 429:
                log.warning("429 Base44 — sleep 60s")
                await asyncio.sleep(60)
    except Exception as e:
        log.warning(f"Base44 push error {domain}: {e}")


async def main():
    log.info("=== AgentSignal Enricher v1.0 ===")

    # Health server
    from aiohttp import web
    app_web = web.Application()
    async def health(req): return web.Response(text="OK")
    app_web.router.add_get("/", health)
    app_web.router.add_get("/health", health)
    runner = web.AppRunner(app_web)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Healthcheck on :{port}")

    pg_pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=5)
    log.info("Postgres connected")

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        cycle = 0
        while True:
            cycle += 1
            # Prendi aziende con base44_id ma senza description
            async with pg_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT domain, name, base44_id, employee_count, description
                    FROM companies
                    WHERE base44_id IS NOT NULL
                      AND (description IS NULL OR description = \'\')
                    ORDER BY ai_score DESC NULLS LAST
                    LIMIT $1
                """, BATCH)

            if not rows:
                log.info(f"[C{cycle}] Tutte le aziende arricchite — sleep 30min")
                await asyncio.sleep(1800)
                continue

            log.info(f"[C{cycle}] Arricchimento {len(rows)} aziende...")
            ok = 0
            for row in rows:
                try:
                    await enrich_one(session, pg_pool, dict(row))
                    ok += 1
                    if ok % 10 == 0:
                        log.info(f"  [{ok}/{len(rows)}] arricchite")
                    await asyncio.sleep(PUSH_DELAY)
                except Exception as e:
                    log.warning(f"Error {row['domain']}: {e}")

            log.info(f"[C{cycle}] Done: {ok}/{len(rows)} arricchite")

if __name__ == "__main__":
    asyncio.run(main())

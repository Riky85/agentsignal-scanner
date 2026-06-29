#!/usr/bin/env python3
"""
Industrial Revenue Enricher v1.0
Arricchisce IndustrialCompany con:
  - revenue (fatturato stimato)
  - employee_count
  - description
  - city / country

Fonti (in ordine di priorità):
  1. Apollo.io organizations/enrich  (se APOLLO_KEY disponibile)
  2. Clearbit Company API            (se CLEARBIT_KEY disponibile)
  3. Schema.org JSON-LD scraping     (gratuito, da homepage)
  4. DuckDuckGo Instant Answer       (gratuito, fallback)

Revenue è salvato come stringa in 'revenue_range' campo su IndustrialCompany.
Campo custom 'description' viene arricchito con il summary aziendale.
"""
import asyncio, aiohttp, os, json, re, logging, time, threading
from urllib.parse import quote_plus
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ENRICH] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN  = os.environ.get("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID     = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
B44_BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW         = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
APOLLO_KEY = os.environ.get("APOLLO_KEY", "")
PORT       = int(os.environ.get("PORT", "8080"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))
WORKER_ID   = int(os.environ.get("WORKER_ID", "0"))
TOTAL_W     = int(os.environ.get("TOTAL_WORKERS", "1"))

stats = {"enriched": 0, "errors": 0, "apollo_hits": 0, "schema_hits": 0, "ddg_hits": 0, "status": "starting"}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()


# ─── FONTI REVENUE ──────────────────────────────────────────────────────────

async def enrich_apollo(session, domain):
    """Apollo.io organization enrich — restituisce revenue, employees, description."""
    if not APOLLO_KEY:
        return {}
    url = "https://api.apollo.io/api/v1/organizations/enrich"
    try:
        async with session.get(url, params={"domain": domain},
                               headers={"x-api-key": APOLLO_KEY, "accept": "application/json"},
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                org = d.get("organization") or {}
                if org:
                    stats["apollo_hits"] += 1
                    rev = org.get("annual_revenue_printed") or org.get("estimated_num_employees","")
                    return {
                        "revenue_range":  _normalize_revenue(org.get("annual_revenue_printed","") or org.get("revenue_range","")),
                        "employee_count": org.get("estimated_num_employees") or org.get("num_employees"),
                        "description":    (org.get("short_description") or org.get("seo_description",""))[:500],
                        "city":           org.get("city",""),
                        "country":        org.get("country",""),
                        "linkedin_url":   org.get("linkedin_url",""),
                        "_source":        "apollo",
                    }
    except Exception as e:
        log.debug(f"Apollo {domain}: {e}")
    return {}


async def enrich_schema_org(session, domain):
    """Scraping JSON-LD Schema.org dalla homepage — Organization, LocalBusiness."""
    url = f"https://{domain}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12),
                               headers={"User-Agent":"Mozilla/5.0 (compatible; IndustrialBot/1.0)"}) as r:
            if r.status not in (200, 301, 302): return {}
            html = await r.text(errors="replace")

        # Estrai tutti i tag <script type="application/ld+json">
        scripts = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S|re.I)
        for raw in scripts:
            try:
                d = json.loads(raw.strip())
                # Può essere array o oggetto
                items = d if isinstance(d, list) else [d]
                for item in items:
                    t = item.get("@type","")
                    if not isinstance(t, str): t = t[0] if t else ""
                    if t not in ("Organization","LocalBusiness","Corporation","Company"): continue
                    rev_raw = (item.get("revenue") or item.get("annualRevenue") or
                               item.get("numberOfEmployees",{}).get("value") if isinstance(item.get("numberOfEmployees"),dict) else None)
                    emp = None
                    emp_node = item.get("numberOfEmployees")
                    if isinstance(emp_node, dict):
                        emp = emp_node.get("value")
                    elif isinstance(emp_node, (int,float)):
                        emp = int(emp_node)
                    stats["schema_hits"] += 1
                    return {
                        "description": (item.get("description",""))[:500],
                        "employee_count": emp,
                        "city": (item.get("address",{}) or {}).get("addressLocality","") if isinstance(item.get("address"),dict) else "",
                        "country": (item.get("address",{}) or {}).get("addressCountry","") if isinstance(item.get("address"),dict) else "",
                        "_source": "schema_org",
                    }
            except: pass
    except Exception as e:
        log.debug(f"Schema {domain}: {e}")
    return {}


async def enrich_ddg(session, domain, company_name):
    """DuckDuckGo Instant Answer API — ottieni revenue range da snippet."""
    query = f"{company_name} annual revenue employees"
    url = f"https://api.duckduckgo.com/?q={quote_plus(query)}&format=json&no_html=1"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10),
                               headers={"User-Agent":"Mozilla/5.0"}) as r:
            if r.status != 200: return {}
            d = await r.json(content_type=None)
            abstract = d.get("AbstractText","") or d.get("Answer","")
            if not abstract: return {}
            rev = _extract_revenue_from_text(abstract)
            emp = _extract_employees_from_text(abstract)
            if rev or emp:
                stats["ddg_hits"] += 1
                return {
                    "revenue_range": rev,
                    "employee_count": emp,
                    "description": abstract[:500] if len(abstract) > 50 else "",
                    "_source": "duckduckgo",
                }
    except Exception as e:
        log.debug(f"DDG {domain}: {e}")
    return {}


def _normalize_revenue(raw):
    """Normalizza revenue string a range leggibile."""
    if not raw: return ""
    raw = str(raw).strip()
    # Già formattato (es. "$10M-$50M")
    if any(c in raw for c in ["M","B","K","€","$","mln","mio"]):
        return raw[:80]
    # Numero puro
    try:
        n = float(re.sub(r"[^\d.]","",raw))
        if n >= 1_000_000_000: return f">${n/1e9:.0f}B"
        if n >= 100_000_000:   return f"€{n/1e6:.0f}M+"
        if n >= 10_000_000:    return f"€{n/1e6:.0f}M"
        if n >= 1_000_000:     return f"€{n/1e6:.1f}M"
        if n >= 1_000:         return f"€{n/1e3:.0f}K"
    except: pass
    return raw[:80]


def _extract_revenue_from_text(text):
    """Estrai range fatturato da testo libero."""
    patterns = [
        r"(?:annual\s+)?revenue[^\d]*\$?([\d,.]+)\s*(million|billion|M|B)",
        r"\$([\d,.]+)\s*(million|billion|M|B)\s*(?:in\s+)?(?:annual\s+)?revenue",
        r"fatturato[^\d]*€?([\d,.]+)\s*(milion|miliard|M|B|mln)",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            n = float(m.group(1).replace(",",""))
            unit = m.group(2).lower()
            mult = 1_000_000_000 if unit in ("billion","b") else 1_000_000
            return _normalize_revenue(str(int(n * mult)))
    return ""


def _extract_employees_from_text(text):
    """Estrai numero dipendenti da testo."""
    patterns = [
        r"([\d,]+)\s+employees",
        r"([\d,]+)\s+dipendenti",
        r"workforce\s+of\s+([\d,]+)",
        r"([\d,]+)\s+staff",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            try: return int(m.group(1).replace(",",""))
            except: pass
    return None


# ─── MAIN ENRICHMENT ─────────────────────────────────────────────────────────

async def enrich_company(session, c):
    """Arricchisce un'azienda con revenue + employees + description."""
    cid    = c["id"]
    domain = (c.get("domain") or "").strip().lower()
    name   = c.get("name", domain)
    if not domain: return

    # Skip se ha già revenue e description
    if c.get("description") and c.get("employee_count") and c.get("revenue_range"):
        return

    result = {}

    # 1. Apollo
    if not result.get("revenue_range"):
        result = await enrich_apollo(session, domain)

    # 2. Schema.org (sempre, per description/city se mancano)
    if not result.get("description"):
        schema = await enrich_schema_org(session, domain)
        for k,v in schema.items():
            if k.startswith("_"): continue
            if not result.get(k) and v: result[k] = v

    # 3. DDG fallback per revenue
    if not result.get("revenue_range"):
        ddg = await enrich_ddg(session, domain, name)
        for k,v in ddg.items():
            if k.startswith("_"): continue
            if not result.get(k) and v: result[k] = v

    # Costruisci payload PUT (solo campi non-null che non sovrascrivono esistenti)
    update = {}
    if result.get("revenue_range") and not c.get("revenue_range"):
        update["revenue_range"] = result["revenue_range"]
    if result.get("employee_count") and not c.get("employee_count"):
        try: update["employee_count"] = int(result["employee_count"])
        except: pass
    if result.get("description") and not c.get("description"):
        update["description"] = result["description"][:500]
    if result.get("city") and not c.get("city"):
        update["city"] = result["city"]
    if result.get("country") and not c.get("country"):
        update["country"] = result["country"]

    if not update: return

    # PUT su B44
    try:
        async with session.put(f"{B44_BASE}/IndustrialCompany/{cid}",
                               headers=HW, json=update,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status in (200,201):
                stats["enriched"] += 1
                src = result.get("_source","?")
                rev = update.get("revenue_range","")
                emp = update.get("employee_count","")
                log.info(f"[ENRICHED] {name:<30} rev={rev:<15} emp={emp:<6} src={src}")
            else:
                stats["errors"] += 1
    except Exception as e:
        stats["errors"] += 1
        log.debug(f"PUT {domain}: {e}")


async def load_batch(session, skip=0, batch=200):
    """Carica batch da arricchire (priorità: done senza revenue)."""
    url = f"{B44_BASE}/IndustrialCompany?limit={batch}&skip={skip}&fields=id,name,domain,country,city,description,employee_count,revenue_range,scan_status"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                return d if isinstance(d,list) else []
    except: pass
    return []


async def main():
    stats["status"] = "running"
    log.info(f"=== Industrial Enricher v1.0 | Worker {WORKER_ID}/{TOTAL_W} | Apollo={'yes' if APOLLO_KEY else 'no'} ===")

    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY*2, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        cycle = 0
        while True:
            skip = WORKER_ID * 200
            total = 0
            while True:
                batch = await load_batch(session, skip=skip)
                if not batch: break

                # Priorità a quelli senza revenue
                to_enrich = [c for c in batch
                             if not c.get("revenue_range") or not c.get("employee_count")]
                log.info(f"Batch skip={skip}: {len(batch)} letti, {len(to_enrich)} da arricchire")

                async def _run(c):
                    async with sem:
                        await enrich_company(session, c)
                        await asyncio.sleep(1.0)  # throttle gentile

                await asyncio.gather(*[_run(c) for c in to_enrich], return_exceptions=True)
                total += len(to_enrich)

                if len(batch) < 200: break
                skip += TOTAL_W * 200
                await asyncio.sleep(2)

            cycle += 1
            log.info(f"Ciclo {cycle} completato. Arricchite: {stats['enriched']} | Attendo 10min...")
            await asyncio.sleep(600)  # pausa 10 min tra cicli

if __name__ == "__main__":
    asyncio.run(main())

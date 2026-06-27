#!/usr/bin/env python3
"""
AgentSignal Scanner v3.2 — Railway Worker
Gira in loop continuo, scansiona aziende pending, scrive su Base44 via PUT.
Nessun reset, nessun timeout, always-on.
"""
import asyncio, aiohttp, time, os, logging, random, json, re
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOKEN  = os.environ["BASE44_TOKEN"]
APP_ID = os.environ["APP_ID"]
BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HR     = {"api-key": TOKEN}
HW     = {"api-key": TOKEN, "Content-Type": "application/json"}

WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
THREADS       = int(os.environ.get("THREADS", "20"))
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", "300"))

# ── AI Signatures (L1-L4 confidence) ──────────────────────────────────────────
AI_SIGNATURES = {
    # L1 — API endpoints (95%)
    "OpenAI":       [r"api\.openai\.com", r"openai\.com/v1", r"OPENAI_API_KEY"],
    "Anthropic":    [r"api\.anthropic\.com", r"anthropic\.com/v1", r"ANTHROPIC_API_KEY"],
    "Google AI":    [r"generativelanguage\.googleapis\.com", r"aiplatform\.googleapis\.com"],
    "Azure OpenAI": [r"openai\.azure\.com", r"azure-openai"],
    "Cohere":       [r"api\.cohere\.ai", r"cohere\.com/generate"],
    "Mistral":      [r"api\.mistral\.ai", r"mistral\.ai/v1"],
    "Groq":         [r"api\.groq\.com", r"groq-sdk"],
    "Perplexity":   [r"api\.perplexity\.ai"],
    "Together AI":  [r"api\.together\.xyz", r"together-ai"],
    "Replicate":    [r"api\.replicate\.com", r"replicate\.run"],
    # L2 — SDK imports (85%)
    "LangChain":    [r"langchain", r"@langchain/"],
    "LlamaIndex":   [r"llama.index", r"llamaindex"],
    "Vercel AI":    [r"ai\.vercel", r"@vercel/ai", r"useChat", r"useCompletion"],
    "Hugging Face": [r"huggingface\.co", r"transformers"],
    "Pinecone":     [r"pinecone\.io", r"@pinecone-database"],
    "Weaviate":     [r"weaviate\.io", r"weaviate-client"],
    "Chroma":       [r"chromadb", r"chroma\.client"],
    "Qdrant":       [r"qdrant\.tech", r"qdrant-client"],
    "Eleven Labs":  [r"elevenlabs\.io", r"@elevenlabs"],
    "Stability AI": [r"stability\.ai", r"stabilityai"],
    "Midjourney":   [r"midjourney", r"discord\.gg/midjourney"],
    "xAI Grok":     [r"api\.x\.ai", r"x\.ai/api"],
    "Ollama":       [r"ollama\.ai", r"ollama\.com", r"localhost:11434"],
    "OpenRouter":   [r"openrouter\.ai", r"@openrouter"],
    # L3 — explicit mentions (75%)
    "AWS Bedrock":  [r"bedrock\.amazonaws", r"aws-sdk.*bedrock"],
    "Databricks":   [r"databricks\.com", r"mlflow"],
    "Snowflake":    [r"snowflake\.com", r"snowpark"],
}

TECH_SIGNATURES = {
    "React":      [r"react\.development\.js", r"react\.production\.min\.js", r"__react"],
    "Next.js":    [r"_next/static", r"__NEXT_DATA__", r"next\.js"],
    "Vue.js":     [r"vue\.global\.js", r"vue\.esm"],
    "Angular":    [r"angular\.min\.js", r"ng-version"],
    "Vercel":     [r"vercel\.app", r"x-vercel-id"],
    "Cloudflare": [r"cloudflare\.com", r"__cf_bm", r"cf-ray"],
    "AWS":        [r"amazonaws\.com", r"aws-amplify"],
    "GCP":        [r"googleapis\.com", r"firebase"],
    "Azure":      [r"azure\.com", r"azurewebsites"],
    "Shopify":    [r"cdn\.shopify\.com", r"shopify\.js"],
    "Stripe":     [r"js\.stripe\.com", r"stripe-js"],
    "HubSpot":    [r"js\.hs-scripts\.com", r"hubspot\.com/hs"],
    "Salesforce": [r"salesforce\.com/lightning", r"force\.com"],
    "Intercom":   [r"widget\.intercom\.io", r"intercomSettings"],
    "Segment":    [r"cdn\.segment\.com", r"analytics\.js"],
    "Datadog":    [r"browser-intake-datadoghq\.com", r"datadoghq\.com/datadog-logs"],
    "Sentry":     [r"browser\.sentry-cdn\.com", r"sentry\.init"],
    "Webflow":    [r"webflow\.com", r"Webflow\.require"],
    "WordPress":  [r"wp-content", r"wp-includes"],
    "Contentful": [r"contentful\.com", r"cdn\.contentful"],
}

async def fetch_page(session, url, timeout=8):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                allow_redirects=True, ssl=False) as resp:
            if resp.status == 200:
                return await resp.text(errors='replace')
    except: pass
    return ""

def detect_tech(html, sig_map):
    found = []
    text = html.lower()
    for name, patterns in sig_map.items():
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                found.append(name)
                break
    return list(set(found))

def extract_json_data(html):
    """Estrae __NEXT_DATA__ e altri JSON embedded"""
    chunks = []
    for pat in [r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
                r'__NEXT_DATA__\s*=\s*({.*?})\s*;',
                r'window\.__(?:INITIAL|APP)_STATE__\s*=\s*({.*?})\s*[;<]']:
        for m in re.finditer(pat, html, re.DOTALL | re.IGNORECASE):
            chunks.append(m.group(1))
    return " ".join(chunks)

async def scan_company(session, company):
    cid  = company["id"]
    url  = company.get("website") or ""
    name = company.get("name") or ""
    if not url:
        return cid, False, [], [], "no_url"

    # Fetch homepage
    html = await fetch_page(session, url)
    if not html:
        # Tentativo senza www
        alt = url.replace("://www.", "://")
        html = await fetch_page(session, alt)
    if not html:
        return cid, False, [], [], "blocked"

    # Aggiungi JSON embedded
    html += " " + extract_json_data(html)

    ai_stack   = detect_tech(html, AI_SIGNATURES)
    tech_stack = detect_tech(html, TECH_SIGNATURES)

    # Score semplice
    ai_score = min(100, len(ai_stack) * 20 + (10 if len(tech_stack) > 5 else 0))

    return cid, True, ai_stack, tech_stack, "ok"

async def write_result(session, cid, ai_stack, tech_stack, fetch_ok):
    payload = {
        "last_scan_date": datetime.now(timezone.utc).isoformat(),
        "ai_stack":   ai_stack,
        "tech_stack": tech_stack,
        "ai_adoption_score": min(100, len(ai_stack) * 20) if fetch_ok else None,
    }
    for attempt in range(3):
        try:
            async with session.put(f"{BASE}/Company/{cid}",
                                    headers=HW, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 429:
                    await asyncio.sleep(8)
                    continue
                return r.ok
        except: await asyncio.sleep(1)
    return False

async def load_batch():
    """Carica aziende pending per questo worker (randomized skip per evitare sovrapposizioni)"""
    connector = aiohttp.TCPConnector(limit=5, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Range randomizzato per questo worker
        total_skip_range = 40000
        zone_size = total_skip_range // TOTAL_WORKERS
        zone_start = WORKER_ID * zone_size
        skip = zone_start + random.randint(0, zone_size - BATCH_SIZE)
        skip = max(0, skip)

        for attempt in range(5):
            try:
                async with session.get(f"{BASE}/Company",
                    headers=HR,
                    params={"limit": BATCH_SIZE, "skip": skip,
                            "fields": "id,name,website,last_scan_date"},
                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.ok:
                        data = await r.json()
                        # Filtra solo non scansionate (o scansionate >24h fa)
                        cutoff = time.time() - 86400
                        pending = []
                        for c in data:
                            if not isinstance(c, dict): continue
                            lsd = c.get("last_scan_date")
                            if not lsd:
                                pending.append(c)
                            # Se vuoi re-scansionare anche le vecchie: decommentare
                            # else:
                            #     try:
                            #         ts = datetime.fromisoformat(lsd.replace("Z","+00:00")).timestamp()
                            #         if ts < cutoff: pending.append(c)
                            #     except: pass
                        return pending
            except Exception as e:
                log.warning(f"load_batch attempt {attempt}: {e}")
                await asyncio.sleep(3)
    return []

async def run_worker():
    log.info(f"=== AgentSignal Scanner v3.2 | worker={WORKER_ID}/{TOTAL_WORKERS} | threads={THREADS} ===")
    total_scanned = total_ai = total_errors = 0
    start = time.time()

    connector = aiohttp.TCPConnector(limit=THREADS, ssl=False)
    async with aiohttp.ClientSession(connector=connector,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AgentSignalBot/3.2)"}) as session:
        while True:
            batch = await load_batch()
            if not batch:
                log.info("Nessuna azienda pending — attendo 5 minuti...")
                await asyncio.sleep(300)
                continue

            log.info(f"Batch: {len(batch)} aziende pending")

            sem = asyncio.Semaphore(THREADS)
            done = ok = ai_hits = 0

            async def process(company):
                nonlocal done, ok, ai_hits
                async with sem:
                    cid, fetched, ai_stack, tech_stack, status = await scan_company(session, company)
                    written = await write_result(session, cid, ai_stack, tech_stack, fetched)
                    done += 1
                    if written: ok += 1
                    if ai_stack: ai_hits += 1
                    if done % 50 == 0:
                        elapsed = time.time() - start
                        rate = int(done / (elapsed / 60)) if elapsed > 0 else 0
                        log.info(f"  {done}/{len(batch)} | {rate}/min | written:{ok} | AI:{ai_hits} ({ai_hits/done*100:.1f}%)")

            await asyncio.gather(*[process(c) for c in batch])

            total_scanned += done
            log.info(f"Batch completato: {done} scansionate, {ok} scritte, {ai_hits} AI hit")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_worker())

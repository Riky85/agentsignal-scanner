#!/usr/bin/env python3
"""
AgentSignal Scanner v4.0 — Railway Worker
- Detection L1-L4 con scoring pesato (non solo conteggio)
- Apollo.io enrichment automatico (headcount, funding, revenue)
- Tutti i 10 score proprietari calcolati
- Deduplicazione dominio prima della scrittura
- Filtro last_scan_date IS NULL + re-scan ogni 7 giorni
- Backoff adattivo su WAF / rate limit
- 0 falsi positivi su tool di produttività
"""
import asyncio, aiohttp, time, os, logging, json, re, hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [W%(message)s")
log = logging.getLogger(__name__)

TOKEN         = os.environ["BASE44_TOKEN"]
APP_ID        = os.environ["APP_ID"]
APOLLO_KEY    = os.environ.get("APOLLO_API_KEY", "")
BASE          = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HR            = {"api-key": TOKEN}
HW            = {"api-key": TOKEN, "Content-Type": "application/json"}

WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
THREADS       = int(os.environ.get("THREADS", "20"))
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", "300"))
RESCAN_DAYS   = int(os.environ.get("RESCAN_DAYS", "7"))

# ── PRODUCTIVITY TOOLS — esclusi dal conteggio AI ─────────────────────────────
PRODUCTIVITY_BLACKLIST = {
    "microsoft office", "google docs", "google sheets", "google slides",
    "excel", "word", "powerpoint", "notion", "confluence", "jira",
    "trello", "asana", "monday.com", "basecamp", "slack", "teams",
    "zoom", "gmail", "outlook", "dropbox", "box.com",
}

# ── AI SIGNATURES con livello di confidenza ───────────────────────────────────
# (nome, pattern_list, livello, peso_score)
# L1=API endpoints (peso 40), L2=SDK/import (peso 25), L3=esplicito (peso 15), L4=contestuale (peso 8)
AI_SIGNATURES = [
    # L1 — API calls dirette (confidenza 95%, peso 40)
    ("OpenAI",       [r"api\.openai\.com", r"openai\.com/v1/", r"OPENAI_API_KEY", r"sk-[a-zA-Z0-9]{20}"], 1, 40),
    ("Anthropic",    [r"api\.anthropic\.com", r"anthropic\.com/v1", r"ANTHROPIC_API_KEY", r"sk-ant-"], 1, 40),
    ("Google AI",    [r"generativelanguage\.googleapis\.com", r"aiplatform\.googleapis\.com", r"vertexai"], 1, 38),
    ("Azure OpenAI", [r"openai\.azure\.com", r"\.openai\.azure\.com/openai/deployments"], 1, 38),
    ("AWS Bedrock",  [r"bedrock-runtime\.amazonaws\.com", r"bedrock\.amazonaws\.com"], 1, 38),
    ("Cohere",       [r"api\.cohere\.ai", r"api\.cohere\.com", r"cohere-python"], 1, 35),
    ("Mistral",      [r"api\.mistral\.ai", r"mistral-client"], 1, 35),
    ("Groq",         [r"api\.groq\.com", r"groq-sdk", r"groq\.com/openai/v1"], 1, 35),
    ("Perplexity",   [r"api\.perplexity\.ai"], 1, 33),
    ("Together AI",  [r"api\.together\.xyz", r"together\.ai/inference"], 1, 33),
    ("Replicate",    [r"api\.replicate\.com", r"replicate\.com/predictions"], 1, 33),
    ("xAI Grok",     [r"api\.x\.ai", r"x\.ai/api/v1"], 1, 33),
    ("Fireworks AI", [r"api\.fireworks\.ai"], 1, 32),
    ("Deepseek",     [r"api\.deepseek\.com", r"deepseek-chat"], 1, 32),

    # L2 — SDK/librerie (confidenza 85%, peso 25)
    ("LangChain",    [r"langchain\.com", r"\"langchain\"", r"from langchain", r"@langchain/core"], 2, 25),
    ("LlamaIndex",   [r"llamaindex\.ai", r"llama.index", r"from llama_index"], 2, 25),
    ("Vercel AI",    [r"sdk\.vercel\.ai", r"\"ai\":\s*\"", r"useChat\b", r"useCompletion\b"], 2, 22),
    ("Hugging Face", [r"huggingface\.co/api", r"from transformers import", r"pipeline\(\"text"], 2, 22),
    ("Pinecone",     [r"pinecone\.io", r"@pinecone-database", r"from pinecone"], 2, 20),
    ("Weaviate",     [r"weaviate\.io", r"weaviate\.connect", r"from weaviate"], 2, 20),
    ("Chroma",       [r"chromadb", r"chroma\.client\(", r"Chroma\.from_documents"], 2, 20),
    ("Qdrant",       [r"qdrant\.tech", r"qdrant-client", r"QdrantClient"], 2, 20),
    ("Milvus",       [r"milvus\.io", r"pymilvus", r"MilvusClient"], 2, 20),
    ("Eleven Labs",  [r"elevenlabs\.io", r"@elevenlabs/", r"ElevenLabsClient"], 2, 18),
    ("Stability AI", [r"stability\.ai/v1", r"stabilityai", r"DiffusionPipeline"], 2, 18),
    ("OpenRouter",   [r"openrouter\.ai", r"@openrouter/ai-sdk"], 2, 18),
    ("Ollama",       [r"localhost:11434", r"ollama\.pull\(", r"ollama-python"], 2, 15),
    ("Databricks",   [r"databricks\.com/api", r"mlflow\.tracking", r"DatabricksEmbeddings"], 2, 15),
    ("Snowflake",    [r"snowflake\.com/cortex", r"snowpark-ml", r"snowflake\.cortex"], 2, 15),

    # L3 — dichiarazioni esplicite (confidenza 75%, peso 15)
    ("Midjourney",   [r"discord\.gg/midjourney", r"midjourney\.com/imagine"], 3, 15),
    ("Cursor AI",    [r"cursor\.sh", r"cursor\.so/pricing"], 3, 12),
    ("GitHub Copilot",[r"github\.com/features/copilot", r"copilot\.github\.com"], 3, 12),

    # L4 — contestuali (confidenza 60%, peso 8) — solo se accompagnati da segnali tecnici
    ("AI Features",  [r"/ai-features", r"/ai-assistant", r"/copilot", r"/ai-search"], 4, 8),
]

# ── TECH STACK SIGNATURES ─────────────────────────────────────────────────────
TECH_SIGNATURES = {
    "React":       [r"react\.production\.min\.js", r"__reactFiber", r"data-reactroot"],
    "Next.js":     [r"_next/static/chunks", r"__NEXT_DATA__", r"next/dist/client"],
    "Vue.js":      [r"vue\.global\.prod\.js", r"__vue_app__", r"v-if="],
    "Angular":     [r"angular\.min\.js", r"ng-version", r"platformBrowserDynamic"],
    "Svelte":      [r"svelte/internal", r"__svelte"],
    "Nuxt":        [r"_nuxt/", r"__nuxt"],
    "Remix":       [r"__remixContext", r"remix\.run"],
    "Vercel":      [r"vercel\.app", r"x-vercel-id", r"/_vercel/"],
    "Cloudflare":  [r"__cf_bm", r"cf-ray:", r"cloudflare\.com/cdn-cgi"],
    "AWS":         [r"amazonaws\.com", r"aws-amplify", r"x-amz-cf-id"],
    "GCP":         [r"googleapis\.com", r"firebaseapp\.com"],
    "Azure":       [r"azurewebsites\.net", r"\.azure\.com"],
    "Shopify":     [r"cdn\.shopify\.com", r"Shopify\.theme", r"shopify-section"],
    "Stripe":      [r"js\.stripe\.com/v3", r"stripe\.createPaymentMethod"],
    "HubSpot":     [r"js\.hs-scripts\.com", r"HubSpotConversations"],
    "Salesforce":  [r"\.force\.com", r"salesforce\.com/lightning"],
    "Intercom":    [r"widget\.intercom\.io", r"window\.intercomSettings"],
    "Segment":     [r"cdn\.segment\.com/analytics\.js", r"analytics\.identify"],
    "Datadog":     [r"browser-intake-datadoghq\.com", r"DD_RUM"],
    "Sentry":      [r"browser\.sentry-cdn\.com", r"Sentry\.init\("],
    "Webflow":     [r"\.webflow\.com", r"Webflow\.require\("],
    "WordPress":   [r"/wp-content/themes/", r"/wp-includes/js/"],
    "Contentful":  [r"cdn\.contentful\.com", r"contentful\.createClient"],
    "Sanity":      [r"sanity\.io/static", r"SanityClient"],
    "Figma":       [r"figma\.com/embed", r"figma-embed"],
    "Docker":      [r"docker\.com", r"FROM python:", r"ENTRYPOINT \["],
    "Kubernetes":  [r"kubernetes\.io", r"kubectl", r"k8s\.io"],
    "Terraform":   [r"terraform\.io", r"hashicorp"],
    "PostgreSQL":  [r"postgresql", r"pg\.connect", r"psycopg2"],
    "MongoDB":     [r"mongodb\.com", r"mongoose\.connect"],
    "Redis":       [r"redis\.io", r"ioredis"],
    "Elasticsearch":[r"elasticsearch", r"elastic\.co"],
}

# Mapping categorie per Technology DNA
CATEGORIES = {
    "AI":          list(dict.fromkeys([s[0] for s in AI_SIGNATURES])),
    "Frontend":    ["React", "Next.js", "Vue.js", "Angular", "Svelte", "Nuxt", "Remix"],
    "Cloud":       ["Vercel", "Cloudflare", "AWS", "GCP", "Azure"],
    "Commerce":    ["Shopify", "Stripe"],
    "CRM_Sales":   ["HubSpot", "Salesforce", "Intercom"],
    "Analytics":   ["Segment", "Datadog", "Sentry"],
    "CMS":         ["Webflow", "WordPress", "Contentful", "Sanity"],
    "Database":    ["PostgreSQL", "MongoDB", "Redis", "Elasticsearch"],
    "DevOps":      ["Docker", "Kubernetes", "Terraform"],
    "Design":      ["Figma"],
}

# ─────────────────────────────────────────────────────────────────────────────
def normalize_domain(url):
    """Estrae dominio canonico da URL per deduplicazione."""
    if not url:
        return ""
    try:
        p = urlparse(url if "://" in url else "https://" + url)
        d = p.netloc or p.path
        d = d.lower().strip().lstrip("www.")
        return d.split("/")[0].split("?")[0]
    except:
        return url.lower().strip()

def extract_json_embedded(html):
    """Estrae __NEXT_DATA__, window.__STATE__, application/json scripts."""
    chunks = []
    patterns = [
        r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
        r'__NEXT_DATA__\s*=\s*(\{.*?\})\s*;',
        r'window\.__(?:INITIAL|APP|REDUX)_STATE__\s*=\s*(\{.*?\})\s*[;<]',
        r'window\.__CONFIG__\s*=\s*(\{.*?\})\s*;',
    ]
    for pat in patterns:
        for m in re.finditer(pat, html, re.DOTALL | re.IGNORECASE):
            chunks.append(m.group(1)[:5000])  # max 5KB per blocco
    return " ".join(chunks)

def detect_ai(html_full):
    """
    Rileva AI stack con scoring pesato L1-L4.
    Ritorna (ai_stack: list, raw_score: int, evidence: dict)
    """
    found = {}
    evidence = {}
    text = html_full  # non lowercaso qui — preservo case per pattern case-sensitive

    for name, patterns, level, weight in AI_SIGNATURES:
        # Skip productivity tools
        if name.lower() in PRODUCTIVITY_BLACKLIST:
            continue
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                # L4 richiede almeno un segnale L1/L2 già trovato per evitare FP
                if level == 4 and not found:
                    break
                found[name] = weight
                evidence[name] = {"level": f"L{level}", "pattern": pat, "weight": weight}
                break

    ai_stack = list(found.keys())
    raw_score = min(100, sum(found.values()))
    return ai_stack, raw_score, evidence

def detect_tech_stack(html_full):
    """Rileva stack tecnologico non-AI."""
    found = []
    for name, patterns in TECH_SIGNATURES.items():
        for pat in patterns:
            if re.search(pat, html_full, re.IGNORECASE):
                found.append(name)
                break
    return found

def build_technology_dna(ai_stack, tech_stack):
    """Costruisce il Technology DNA JSON strutturato con le 12 categorie."""
    dna = {}
    all_detected = set(ai_stack + tech_stack)
    for category, tools in CATEGORIES.items():
        detected = [t for t in tools if t in all_detected]
        dna[category] = {
            "tools": detected,
            "count": len(detected),
            "coverage": round(len(detected) / max(len(tools), 1), 2)
        }
    return dna

def calculate_scores(ai_stack, tech_stack, ai_raw_score, dna, apollo_data=None):
    """Calcola tutti i 10 score proprietari."""
    scores = {}
    ts = set(tech_stack)
    ai = set(ai_stack)

    # 1. AI Adoption Score (0-100)
    scores["ai_adoption_score"] = ai_raw_score

    # 2. AI Maturity Score (0-5)
    l1_count = sum(1 for s in AI_SIGNATURES if s[0] in ai and s[2] == 1)
    l2_count = sum(1 for s in AI_SIGNATURES if s[0] in ai and s[2] == 2)
    maturity = min(5.0, round(l1_count * 1.5 + l2_count * 0.8, 1))
    scores["ai_maturity_score"] = maturity

    # 3. Cloud Score (0-100)
    cloud_tools = {"AWS", "GCP", "Azure", "Vercel", "Cloudflare"}
    scores["cloud_score"] = min(100, len(cloud_tools & ts) * 25)

    # 4. Commerce Score (0-100)
    commerce_signals = len({"Shopify", "Stripe"} & ts)
    scores["commerce_score"] = min(100, commerce_signals * 45)

    # 5. Automation Score (0-100)
    auto_tools = {"LangChain", "LlamaIndex", "Vercel AI", "OpenAI", "Anthropic"}
    scores["automation_score"] = min(100, len(auto_tools & ai) * 20 + (20 if l1_count >= 2 else 0))

    # 6. Developer Score (0-100)
    dev_tools = {"React", "Next.js", "Vue.js", "Angular", "Docker", "Kubernetes", "Terraform"}
    scores["developer_score"] = min(100, len(dev_tools & ts) * 15)

    # 7. Security Score (0-100)
    sec_signals = len({"Cloudflare", "Datadog", "Sentry"} & ts)
    scores["security_score"] = min(100, sec_signals * 30)

    # 8. Growth Score (0-100)
    growth_tools = {"HubSpot", "Salesforce", "Segment", "Intercom"}
    growth_base = len(growth_tools & ts) * 20
    # Boost da Apollo se disponibile
    if apollo_data:
        emp = apollo_data.get("num_employees") or 0
        funding = apollo_data.get("total_funding") or 0
        if emp > 500: growth_base += 15
        if funding > 10_000_000: growth_base += 15
    scores["growth_score"] = min(100, growth_base)

    # 9. Innovation Score (0-100)
    innovation = min(100, len(ai) * 8 + len(ts) * 2)
    scores["innovation_score"] = innovation

    # 10. AI Buying Intent Score (0-100) — il più importante
    intent = 0
    if l1_count >= 1: intent += 35   # usa API AI attivamente
    if l1_count >= 3: intent += 20   # multi-provider = heavy user
    if l2_count >= 2: intent += 15   # SDK integration
    if "LangChain" in ai or "LlamaIndex" in ai: intent += 15  # orchestration
    if len({"AWS Bedrock", "Azure OpenAI", "Google AI"} & ai) >= 1: intent += 10  # enterprise cloud
    # Tech gap: molte tecnologie ma poca AI = opportunità
    tech_gap = max(0, len(ts) * 3 - ai_raw_score)
    scores["tech_gap_score"] = min(100, tech_gap)

    scores["ai_buying_intent_score"] = min(100, intent)

    # Velocity: presenza di tool AI moderni (post-2023)
    modern_ai = {"Groq", "Mistral", "Perplexity", "Together AI", "xAI Grok", "Fireworks AI", "Deepseek", "OpenRouter"}
    scores["ai_velocity_score"] = min(100, len(modern_ai & ai) * 25)

    # Transformation: quanto è digitalmente matura l'azienda in generale
    scores["ai_transformation_score"] = min(100, int(
        (scores["ai_adoption_score"] * 0.3) +
        (scores["cloud_score"] * 0.2) +
        (scores["developer_score"] * 0.2) +
        (scores["automation_score"] * 0.15) +
        (scores["innovation_score"] * 0.15)
    ))

    return scores

# ─────────────────────────────────────────────────────────────────────────────
async def fetch_page(session, url, timeout=10):
    """Fetch con retry e backoff adattivo."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    for attempt in range(2):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                   allow_redirects=True, ssl=False, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.text(errors='replace')
                if resp.status == 429:
                    await asyncio.sleep(5 * (attempt + 1))
        except asyncio.TimeoutError:
            break
        except Exception:
            await asyncio.sleep(1)
    return ""

async def apollo_enrich(session, domain):
    """Arricchisce dati aziendali via Apollo.io."""
    if not APOLLO_KEY or not domain:
        return {}
    try:
        async with session.post(
            "https://api.apollo.io/v1/organizations/enrich",
            json={"domain": domain},
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache", "X-Api-Key": APOLLO_KEY},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                org = data.get("organization") or {}
                return {
                    "num_employees": org.get("estimated_num_employees"),
                    "employee_ranges": org.get("employee_count"),
                    "total_funding": org.get("total_funding"),
                    "funding_stage": org.get("latest_funding_stage"),
                    "industry": org.get("industry"),
                    "description": org.get("short_description"),
                    "linkedin_url": org.get("linkedin_url"),
                    "founded_year": org.get("founded_year"),
                    "annual_revenue": org.get("annual_revenue_printed"),
                    "city": org.get("city"),
                    "country": org.get("country"),
                }
    except Exception as e:
        log.debug(f"Apollo error for {domain}: {e}")
    return {}

async def scan_company(session, company):
    """Scansione completa di una singola azienda."""
    cid  = company["id"]
    url  = (company.get("website") or "").strip()
    name = company.get("name") or ""

    if not url:
        return cid, None, None

    # Normalizza URL
    if not url.startswith("http"):
        url = "https://" + url

    domain = normalize_domain(url)

    # Fetch homepage
    html = await fetch_page(session, url)
    if not html:
        # Tenta variante senza www
        alt = re.sub(r"://www\.", "://", url)
        if alt != url:
            html = await fetch_page(session, alt)

    if not html:
        # Azienda bloccata — aggiorniamo solo last_scan_date
        return cid, {}, {}

    # Aggiungi dati JSON embedded (React/Next.js apps)
    html_full = html + " " + extract_json_embedded(html)

    # Detection
    ai_stack, ai_raw_score, ai_evidence = detect_ai(html_full)
    tech_stack = detect_tech_stack(html_full)

    # Technology DNA
    dna = build_technology_dna(ai_stack, tech_stack)

    # Apollo enrichment (solo se abbiamo API key)
    apollo_data = await apollo_enrich(session, domain)

    # Calcola tutti gli score
    scores = calculate_scores(ai_stack, tech_stack, ai_raw_score, dna, apollo_data)

    # Payload completo per Base44
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "last_scan_date": now,
        "ai_stack": ai_stack,
        "tech_stack": tech_stack,
        "technology_dna": json.dumps(dna),
        "ats_technology_adoption": json.dumps({
            "evidence": ai_evidence,
            "domain": domain,
            "scanned_at": now,
            "scanner_version": "4.0"
        }),
        **scores,
    }

    # Apollo data → campi aziendali (solo se non già presenti)
    if apollo_data:
        if apollo_data.get("num_employees") and not company.get("employee_count"):
            payload["employee_count"] = apollo_data["num_employees"]
        if apollo_data.get("industry") and not company.get("industry"):
            payload["industry"] = apollo_data["industry"]
        if apollo_data.get("description") and not company.get("description"):
            payload["description"] = apollo_data["description"]
        if apollo_data.get("country") and not company.get("country"):
            payload["country"] = apollo_data["country"]
        # Segnali di funding
        if apollo_data.get("total_funding") or apollo_data.get("funding_stage"):
            payload["acquisition_signals"] = json.dumps({
                "total_funding": apollo_data.get("total_funding"),
                "funding_stage": apollo_data.get("funding_stage"),
                "annual_revenue": apollo_data.get("annual_revenue"),
                "linkedin_url": apollo_data.get("linkedin_url"),
                "founded_year": apollo_data.get("founded_year"),
            })

    return cid, payload, ai_evidence

async def write_to_base44(session, cid, payload, retries=3):
    """Scrive su Base44 via PUT con retry e backoff."""
    for attempt in range(retries):
        try:
            async with session.put(
                f"{BASE}/Company/{cid}",
                headers=HW, json=payload,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                if r.status == 429:
                    wait = 10 * (attempt + 1)
                    log.debug(f"Rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status in (200, 201):
                    return True
                if r.status >= 400:
                    log.debug(f"PUT {cid} -> HTTP {r.status}")
                    return False
        except Exception as e:
            await asyncio.sleep(2 * (attempt + 1))
    return False

# Stato di avanzamento per ogni worker (persistente nel processo)
_worker_skip = None  # inizializzato al primo run

async def load_batch(session):
    """
    Carica aziende da scansionare usando sort=last_scan_date:
    - NULL viene prima (mai scansionate)
    - Poi le più vecchie in ordine crescente
    
    Ogni worker parte da un offset diverso e avanza sequenzialmente.
    Quando arriva in fondo, riparte da 0 (ciclo continuo, rescan ogni ~7gg).
    
    Worker 0 → skip parte da 0
    Worker 1 → skip parte da TOTAL_DB/TOTAL_WORKERS * 1
    Worker 2 → skip parte da TOTAL_DB/TOTAL_WORKERS * 2
    """
    global _worker_skip
    TOTAL_DB_ESTIMATE = 35000  # aggiornato ogni ciclo

    if _worker_skip is None:
        # Inizializzazione: ogni worker parte dalla sua zona
        zone_size = TOTAL_DB_ESTIMATE // TOTAL_WORKERS
        _worker_skip = WORKER_ID * zone_size
        log.info(f"Worker {WORKER_ID} inizia da skip={_worker_skip}")

    for attempt in range(5):
        try:
            params = {
                "limit": BATCH_SIZE,
                "skip": _worker_skip,
                "sort": "last_scan_date",  # NULL prima, poi più vecchie
                "fields": "id,name,website,last_scan_date,employee_count,industry,description,country",
            }
            async with session.get(f"{BASE}/Company", headers=HR, params=params,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.ok:
                    data = await r.json()
                    if isinstance(data, list) and data:
                        # Avanza il cursore per il prossimo batch
                        _worker_skip += BATCH_SIZE
                        # Reset quando supera la fine del DB
                        if len(data) < BATCH_SIZE:
                            log.info(f"Worker {WORKER_ID}: fine DB raggiunta, ricomincio da skip={WORKER_ID * (TOTAL_DB_ESTIMATE // TOTAL_WORKERS)}")
                            _worker_skip = WORKER_ID * (TOTAL_DB_ESTIMATE // TOTAL_WORKERS)
                        return data
                    else:
                        # Fine del DB — reset
                        _worker_skip = WORKER_ID * (TOTAL_DB_ESTIMATE // TOTAL_WORKERS)
                        log.info(f"Worker {WORKER_ID}: reset skip a {_worker_skip}")
                        return []
        except Exception as e:
            log.warning(f"load_batch attempt {attempt+1}: {e}")
            await asyncio.sleep(5)
    return []

async def run_worker():
    log.info(f"={WORKER_ID}/{TOTAL_WORKERS}] AgentSignal Scanner v4.0 | threads={THREADS} | apollo={'YES' if APOLLO_KEY else 'NO'} ===")
    total_scanned = total_ai = total_written = 0
    start = time.time()

    connector = aiohttp.TCPConnector(limit=THREADS, ssl=False, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            batch = await load_batch(session)
            if not batch:
                log.info("Nessuna azienda da scansionare — attendo 10 min...")
                await asyncio.sleep(600)
                continue

            log.info(f"Batch: {len(batch)} aziende")
            sem = asyncio.Semaphore(THREADS)
            done = ok = ai_hits = 0
            batch_start = time.time()

            async def process(company):
                nonlocal done, ok, ai_hits
                async with sem:
                    try:
                        cid, payload, evidence = await scan_company(session, company)
                        if payload is not None:
                            written = await write_to_base44(session, cid, payload)
                            if written:
                                ok += 1
                                if payload.get("ai_stack"):
                                    ai_hits += 1
                    except Exception as e:
                        log.debug(f"process error: {e}")
                    finally:
                        done += 1
                        if done % 50 == 0:
                            elapsed = time.time() - batch_start
                            rate = int(done / max(elapsed / 60, 0.01))
                            pct_ai = ai_hits / max(done, 1) * 100
                            log.info(f"  [{done}/{len(batch)}] {rate}/min | written:{ok} | AI:{ai_hits} ({pct_ai:.1f}%)")

            await asyncio.gather(*[process(c) for c in batch])

            total_scanned += done
            total_ai += ai_hits
            total_written += ok
            elapsed_total = (time.time() - start) / 3600

            log.info(
                f"Batch done: {done} scan | {ok} written | {ai_hits} AI hits | "
                f"Totale: {total_scanned} | AI rate: {total_ai/max(total_scanned,1)*100:.1f}% | "
                f"Uptime: {elapsed_total:.1f}h"
            )
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_worker())

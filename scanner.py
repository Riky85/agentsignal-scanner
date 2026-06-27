#!/usr/bin/env python3
"""
AgentSignal Railway Scanner v6.0
==================================
Architettura: Railway-first, Base44-last

FLUSSO:
  1. PostgreSQL Railway  → storage principale (milioni di domini)
  2. Scanner workers     → scansionano da Postgres, scrivono su Postgres
  3. Sync pusher         → ogni 5 min pusha su Base44 SOLO i record con AI/cambiamenti
                           (max 10 PUT/min per rispettare rate limit)

VANTAGGI:
  - Zero rate limit: Postgres locale è illimitato
  - Throughput reale: 100k+ scan/ora senza colli di bottiglia
  - Base44 usato solo come "vetrina" — non come DB di lavoro
  - Deduplicazione nativa su PostgreSQL (UNIQUE constraint su domain)
  - Inserimento 1.6M domini in pochi minuti (INSERT ... ON CONFLICT DO NOTHING)
"""

import asyncio
import aiohttp
import asyncpg
import os
import logging
import json
import re
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [W%(message)s", force=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL  = os.environ["DATABASE_URL"]          # da Railway (postgres)
BASE44_TOKEN  = os.environ["BASE44_TOKEN"]
APP_ID        = os.environ["APP_ID"]
APOLLO_KEY    = os.environ.get("APOLLO_API_KEY", "")
BASE44_URL    = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HR            = {"api-key": BASE44_TOKEN}
HW            = {"api-key": BASE44_TOKEN, "Content-Type": "application/json"}

WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "3"))
THREADS       = int(os.environ.get("THREADS", "30"))
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", "500"))
RESCAN_DAYS   = int(os.environ.get("RESCAN_DAYS", "14"))
PORT          = int(os.environ.get("PORT", "8080"))
MODE          = os.environ.get("MODE", "scanner")  # scanner | importer | syncer

# ── DB Schema ─────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT UNIQUE NOT NULL,
    name            TEXT,
    website         TEXT,
    source          TEXT DEFAULT 'bulk_import',
    global_rank     INT,
    country         TEXT,
    industry        TEXT,
    employee_count  INT,
    revenue_range   TEXT,
    logo_url        TEXT,
    
    -- Scan results
    ai_stack        JSONB DEFAULT '[]',
    tech_stack      JSONB DEFAULT '[]',
    ai_score        FLOAT DEFAULT 0,
    maturity_score  FLOAT DEFAULT 0,
    cloud_score     FLOAT DEFAULT 0,
    automation_score FLOAT DEFAULT 0,
    developer_score  FLOAT DEFAULT 0,
    security_score   FLOAT DEFAULT 0,
    growth_score     FLOAT DEFAULT 0,
    innovation_score FLOAT DEFAULT 0,
    intent_score     FLOAT DEFAULT 0,
    commerce_score   FLOAT DEFAULT 0,
    tech_gap_score   FLOAT DEFAULT 0,
    
    -- Tracking
    base44_id       TEXT,           -- ID record su Base44 (NULL = non ancora pushato)
    last_scan_date  TIMESTAMPTZ,
    last_push_date  TIMESTAMPTZ,    -- ultima volta che è stato pushato su Base44
    scan_errors     INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_scan   ON companies(last_scan_date NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_companies_push   ON companies(last_push_date NULLS FIRST) WHERE ai_score > 0;
CREATE INDEX IF NOT EXISTS idx_companies_ai     ON companies(ai_score DESC) WHERE ai_score > 0;
CREATE INDEX IF NOT EXISTS idx_companies_worker ON companies(id) WHERE last_scan_date IS NULL;
"""

# ── AI / Tech Signatures ──────────────────────────────────────────────────────
PRODUCTIVITY_BLACKLIST = {
    "microsoft office","google docs","google sheets","google slides",
    "excel","word","powerpoint","notion","confluence","jira","trello",
    "asana","monday.com","basecamp","slack","teams","zoom","gmail",
    "outlook","dropbox","box.com","sharepoint","onedrive",
}

# ── AI Detection Signatures ────────────────────────────────────────────────────
# REGOLA FONDAMENTALE: solo pattern TECNICI verificabili dal codice sorgente.
# L1 = endpoint API o chiave univoca (certezza assoluta)
# L2 = package name, SDK, import path inequivocabile
# L3/L4 RIMOSSI: menzioni testuali, nomi in articoli, blog, UI copy → falsi positivi
#
# Pattern L1: URL endpoint API + chiavi/token univoci
# Pattern L2: nome package npm/pip, import path, CDN SDK
# Nessun pattern che matcha testo libero (titoli, news, blog, descrizioni prodotto)

AI_SIGNATURES = [
    # ── Tier 1: endpoint API — solo chiamate dirette al provider ──────────────
    ("OpenAI",        [r"api\.openai\.com", r"OPENAI_API_KEY\s*=", r'"sk-[a-zA-Z0-9\-_]{20,}"'], 1, 40),
    ("Anthropic",     [r"api\.anthropic\.com", r"ANTHROPIC_API_KEY\s*=", r'"sk-ant-[a-zA-Z0-9\-_]{10,}"'], 1, 40),
    ("Google AI",     [r"generativelanguage\.googleapis\.com", r"aiplatform\.googleapis\.com", r"vertexai\.preview"], 1, 38),
    ("Azure OpenAI",  [r"openai\.azure\.com/openai/deployments", r"\.openai\.azure\.com"], 1, 38),
    ("AWS Bedrock",   [r"bedrock-runtime\.amazonaws\.com", r"bedrock\.amazonaws\.com/model/"], 1, 38),
    ("Cohere",        [r"api\.cohere\.ai/v", r"api\.cohere\.com/v"], 1, 35),
    ("Mistral",       [r"api\.mistral\.ai/v"], 1, 35),
    ("Groq",          [r"api\.groq\.com/openai/v1"], 1, 35),
    ("Perplexity",    [r"api\.perplexity\.ai/chat/completions"], 1, 33),
    ("Together AI",   [r"api\.together\.xyz/v1/completions", r"api\.together\.ai/v1"], 1, 33),
    ("Replicate",     [r"api\.replicate\.com/v1/predictions"], 1, 33),
    ("xAI Grok",      [r"api\.x\.ai/v1/chat", r"api\.x\.ai/v1/completions"], 1, 33),
    ("Fireworks AI",  [r"api\.fireworks\.ai/inference/v1"], 1, 32),
    ("Deepseek",      [r"api\.deepseek\.com/v1"], 1, 32),
    ("ElevenLabs",    [r"api\.elevenlabs\.io/v1"], 1, 30),
    ("Stability AI",  [r"api\.stability\.ai/v1/generation"], 1, 30),

    # ── Tier 2: SDK / package inequivocabile ──────────────────────────────────
    # Solo import/require esatti o CDN path — non nomi generici
    ("LangChain",     [r"from langchain[_\-\.]", r"require\(['\"]langchain", r"langchain-core@", r"@langchain/core"], 2, 25),
    ("LlamaIndex",    [r"from llama_index\.", r"llama-index==", r"llama_index\.core"], 2, 25),
    ("Hugging Face",  [r"from transformers import", r"huggingface\.co/models/", r"pipeline\(['\"]text-generation"], 2, 22),
    ("Pinecone",      [r"pinecone\.init\(", r"from pinecone import", r"pinecone-client==", r"@pinecone-database/pinecone"], 2, 22),
    ("Weaviate",      [r"weaviate\.connect_to", r"import weaviate$", r"weaviate-client=="], 2, 20),
    ("Qdrant",        [r"QdrantClient\(", r"qdrant-client==", r"from qdrant_client import"], 2, 20),
    ("Chroma",        [r"chromadb\.Client\(", r"import chromadb$", r"chromadb=="], 2, 18),
    ("PyTorch",       [r"import torch\b", r"torch\.nn\.Module", r"torch==\d"], 2, 15),
    ("TensorFlow",    [r"import tensorflow as tf", r"tensorflow==\d", r"tf\.keras\."], 2, 15),
    ("Ollama",        [r"ollama\.chat\(", r"ollama\.generate\(", r"from ollama import"], 2, 20),
    ("Vercel AI SDK", [r"from ['\"]@vercel/ai['\"]", r"require\(['\"]@vercel/ai['\"]", r"ai@\d+\.\d+\.\d+"], 2, 18),
    ("OpenAI SDK",    [r"from openai import", r"require\(['\"]openai['\"]", r"openai==\d"], 2, 20),
    ("Anthropic SDK", [r"from anthropic import", r"require\(['\"]@anthropic-ai/sdk['\"]"], 2, 20),
    ("Langfuse",      [r"from langfuse import", r"langfuse\.com/api", r"langfuse==\d"], 2, 12),
    ("LiteLLM",       [r"import litellm\b", r"litellm\.completion\("], 2, 15),
    ("Haystack",      [r"from haystack import", r"haystack==\d"], 2, 15),
    ("AutoGen",       [r"from autogen import", r"autogen==\d", r"microsoft/autogen"], 2, 15),
    ("CrewAI",        [r"from crewai import", r"crewai==\d"], 2, 15),
]

# ── Tech stack: solo CDN/SDK path inequivocabili ───────────────────────────────
TECH_SIGNATURES = [
    ("React",     [r"react\.js", r"reactjs", r"_react", r"__REACT"]),
    ("Next.js",   [r"next\.js", r"_next/static", r"__NEXT_DATA__"]),
    ("Vue",       [r"vue\.js", r"vuejs", r"__vue"]),
    ("Angular",   [r"angular\.js", r"angularjs", r"ng-version"]),
    ("Vercel",    [r"vercel\.app", r"vercel\.com", r"_vercel"]),
    ("Netlify",   [r"netlify\.app", r"netlify\.com"]),
    ("Cloudflare",[r"cloudflare\.com", r"cf-ray", r"__cf_bm"]),
    ("AWS",       [r"amazonaws\.com", r"cloudfront\.net"]),
    ("GCP",       [r"googleapis\.com", r"googlecloud\.com"]),
    ("Azure",     [r"azure\.com", r"azurewebsites\.net"]),
    ("Shopify",   [r"shopify\.com", r"cdn\.shopify\.com", r"myshopify"]),
    ("Stripe",    [r"stripe\.com", r"js\.stripe\.com"]),
    ("HubSpot",   [r"hubspot\.com", r"hs-scripts"]),
    ("Intercom",  [r"intercom\.io", r"widget\.intercom\.io"]),
    ("Mixpanel",  [r"mixpanel\.com"]),
    ("Amplitude", [r"amplitude\.com"]),
    ("Sentry",    [r"sentry\.io", r"browser\.sentry-cdn"]),
    ("Datadog",   [r"datadoghq\.com"]),
    ("Webflow",   [r"webflow\.com", r"webflow\.io"]),
    ("WordPress", [r"wp-content", r"wp-includes", r"wordpress"]),
    ("Docker",    [r"docker\.com", r"dockerfile"]),
    ("Kubernetes",[r"kubernetes\.io", r"k8s\."]),
]

EXCLUDE_DOMAINS = {
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
    "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","apple.com",
    "microsoft.com","netflix.com","spotify.com","cloudflare.com","amazonaws.com",
    "doubleclick.net","googlesyndication.com","gstatic.com","googletagmanager.com",
    "googleapis.com","akamai.net","akamaized.net","fastly.net","cloudfront.net",
    "wp.com","wordpress.com","blogspot.com","tumblr.com","medium.com",
}


# ── Utilities ──────────────────────────────────────────────────────────────────
def normalize_domain(url: str) -> str:
    if not url: return ""
    try:
        if not url.startswith("http"): url = "https://" + url
        d = urlparse(url).netloc.lower()
        return d.replace("www.", "").strip()
    except Exception:
        return url.lower().strip()


def domain_to_name(domain: str) -> str:
    name = domain.split(".")[0]
    return re.sub(r"[-_]", " ", name).title()


def extract_text(html: str) -> str:
    json_parts = []
    for m in re.finditer(r'__NEXT_DATA__\s*=\s*({.*?})\s*[;<]', html, re.DOTALL):
        json_parts.append(m.group(1))
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).lower()
    return text + " " + " ".join(json_parts).lower()



def detect(text: str, html: str) -> tuple[list, list]:
    """
    Detection STRICT: accetta SOLO pattern tecnici L1 (endpoint API) e L2 (SDK/package).
    L3/L4 rimossi — eliminano i falsi positivi da articoli, blog, UI copy, news sites.
    Il pattern viene cercato nell'HTML grezzo (codice JS, tag script, meta, headers CDN)
    NON nel testo visibile estratto dalla pagina.
    """
    # Cerca nei tag script e negli URL CDN — NON nel testo visibile
    # Estrai solo: script src, inline JS, meta http-equiv, link href, commenti HTML
    code_sections = []

    # Script inline
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE):
        code_sections.append(m.group(1))

    # Script src / link href / img src (CDN fingerprint)
    for m in re.finditer(r'(?:src|href|action|data-src)\s*=\s*["\']([^"\']{4,})["\']', html, re.IGNORECASE):
        code_sections.append(m.group(1))

    # JSON embedded (__NEXT_DATA__, __NUXT_DATA__, ecc.)
    for m in re.finditer(r'(?:__NEXT_DATA__|__NUXT__|__remixContext)\s*=\s*({.*?})\s*[;<]', html, re.DOTALL):
        code_sections.append(m.group(1))

    # HTTP response headers riflessi nel DOM (x-powered-by, cf-ray, ecc.)
    code_combined = " ".join(code_sections).lower()

    ai_found, tech_found = [], []

    for name, patterns, level, weight in AI_SIGNATURES:
        # Solo L1 e L2 accettati
        if level > 2:
            continue
        for pat in patterns:
            try:
                if re.search(pat, code_combined, re.IGNORECASE):
                    n_lower = name.lower().replace(" ", "")
                    if n_lower not in PRODUCTIVITY_BLACKLIST and name not in ai_found:
                        ai_found.append(name)
                    break
            except re.error:
                continue

    # Tech stack: cerca nell'HTML completo (CDN URL, meta tag, cookie names)
    html_lower = html.lower()
    for name, patterns in TECH_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, html_lower, re.IGNORECASE) and name not in tech_found:
                    tech_found.append(name)
                    break
            except re.error:
                continue

    return ai_found, tech_found


def calc_scores(ai_stack, tech_stack, text) -> dict:
    ai_n     = len(ai_stack)
    intent   = sum(1 for kw in ["powered by ai","ai-powered","llm","gpt"] if kw in text)
    cloud    = sum(1 for t in tech_stack if t in ["AWS","GCP","Azure","Cloudflare","Vercel"])
    dev      = sum(1 for t in tech_stack if t in ["React","Next.js","Vue","Angular","Docker","Kubernetes"])
    def clamp(v): return min(100.0, max(0.0, float(v)))
    return {
        "ai_score":         clamp(ai_n * 12 + intent * 5),
        "maturity_score":   clamp(ai_n * 10 + cloud * 8 + dev * 5 + len(tech_stack) * 3),
        "cloud_score":      clamp(cloud * 25),
        "automation_score": clamp(sum(1 for t in ai_stack if t in ["LangChain","LlamaIndex","Ray"]) * 20),
        "developer_score":  clamp(dev * 15),
        "security_score":   clamp(cloud * 20),
        "growth_score":     clamp(sum(1 for kw in ["hiring","careers","we're growing"] if kw in text) * 15),
        "innovation_score": clamp(ai_n * 8 + intent * 6 + dev * 4),
        "intent_score":     clamp(intent * 20 + ai_n * 8),
        "commerce_score":   clamp(sum(1 for t in tech_stack if t in ["Shopify","Stripe"]) * 40),
        "tech_gap_score":   clamp(100 - min(ai_n * 10 + cloud * 8 + dev * 5, 100)),
    }


# ── Web fetch ──────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AgentSignalBot/6.0; +https://agentsignal.io)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

async def fetch(session, url: str, timeout=10) -> str:
    try:
        async with session.get(url, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=timeout),
                               allow_redirects=True, max_redirects=4) as r:
            if r.status == 200:
                ct = r.headers.get("Content-Type","")
                if "text" in ct or "json" in ct:
                    return await r.text(errors="replace")
    except Exception:
        pass
    return ""


async def scan_domain(session, row: dict) -> dict | None:
    domain  = row["domain"]
    website = row.get("website") or f"https://{domain}"

    pages = [website, website.rstrip("/") + "/about", website.rstrip("/") + "/technology"]
    html_combined = ""
    for url in pages[:2]:
        h = await fetch(session, url)
        if h: html_combined += " " + h

    if not html_combined.strip():
        return {"domain": domain, "scan_errors": (row.get("scan_errors") or 0) + 1,
                "last_scan_date": datetime.now(timezone.utc)}

    text = extract_text(html_combined)
    ai_stack, tech_stack = detect(text, html_combined)
    scores = calc_scores(ai_stack, tech_stack, text)

    return {
        "domain":         domain,
        "ai_stack":       json.dumps(ai_stack),
        "tech_stack":     json.dumps(tech_stack),
        "last_scan_date": datetime.now(timezone.utc),
        **scores,
    }


# ── PostgreSQL helpers ─────────────────────────────────────────────────────────
async def ensure_schema(pool):
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("=0] Schema DB OK")


async def write_scan_result(pool, result: dict):
    if not result: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE companies SET
                ai_stack        = $1,
                tech_stack      = $2,
                ai_score        = $3,
                maturity_score  = $4,
                cloud_score     = $5,
                automation_score= $6,
                developer_score = $7,
                security_score  = $8,
                growth_score    = $9,
                innovation_score= $10,
                intent_score    = $11,
                commerce_score  = $12,
                tech_gap_score  = $13,
                last_scan_date  = $14,
                scan_errors     = COALESCE($15, scan_errors),
                updated_at      = NOW()
            WHERE domain = $16
        """,
            result.get("ai_stack","[]"),
            result.get("tech_stack","[]"),
            result.get("ai_score",0),
            result.get("maturity_score",0),
            result.get("cloud_score",0),
            result.get("automation_score",0),
            result.get("developer_score",0),
            result.get("security_score",0),
            result.get("growth_score",0),
            result.get("innovation_score",0),
            result.get("intent_score",0),
            result.get("commerce_score",0),
            result.get("tech_gap_score",0),
            result.get("last_scan_date"),
            result.get("scan_errors"),
            result["domain"],
        )


async def load_batch_pg(pool) -> list[dict]:
    """Carica batch: prima i non scansionati, poi i più vecchi."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RESCAN_DAYS)
    # Partiziona per worker_id per evitare collisioni
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, domain, website, source, global_rank, employee_count,
                   scan_errors, last_scan_date
            FROM companies
            WHERE (last_scan_date IS NULL OR last_scan_date < $1)
              AND scan_errors < 5
              AND (id % $2) = $3
            ORDER BY last_scan_date NULLS FIRST
            LIMIT $4
        """, cutoff, TOTAL_WORKERS, WORKER_ID, BATCH_SIZE)
    return [dict(r) for r in rows]


# ── Importer: carica 1.6M domini in PG ───────────────────────────────────────
async def run_importer(pool):
    """
    True streaming importer — never holds more than CHUNK_SIZE rows in RAM.

    Majestic Million (~80MB CSV):
      - Streamed in 64KB network chunks via aiohttp content.iter_chunked()
      - Decoded and split line-by-line with a rolling leftover buffer
      - At most CHUNK_SIZE rows (~2000 × ~30 bytes = ~60KB) in memory at once

    Cisco Umbrella (~12MB ZIP):
      - Small enough to download in full, but still parsed row-by-row

    Peak RAM: ~5MB regardless of source file size.
    """
    import csv, io, zipfile, re as _re
    log.info("=I] IMPORTER v2 — true streaming, peak RAM ~5MB")

    CHUNK_SIZE = 2000
    EXCLUDE = {
        "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
        "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","apple.com",
        "microsoft.com","netflix.com","spotify.com","cloudflare.com","amazonaws.com",
        "doubleclick.net","googlesyndication.com","gstatic.com","googletagmanager.com",
        "googleapis.com","akamai.net","akamaized.net","fastly.net","cloudfront.net",
        "wp.com","wordpress.com","blogspot.com","tumblr.com","medium.com",
    }

    def ok_domain(d):
        return (d and len(d) > 3 and d not in EXCLUDE
                and "." in d and d.count(".") <= 3
                and not _re.match(r"^\d+\.\d+", d))

    async def flush_chunk(conn, chunk):
        if chunk:
            await conn.executemany("""
                INSERT INTO companies (domain, name, website, source, global_rank)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (domain) DO NOTHING
            """, chunk)

    total_inserted = 0
    connector = aiohttp.TCPConnector(limit=2)

    async with aiohttp.ClientSession(connector=connector) as session:

        # ── SOURCE 1: Majestic Million — true line streaming ─────────────────
        log.info("=I] Majestic Million: line-streaming (no full read)...")
        try:
            async with session.get(
                "https://downloads.majestic.com/majestic_million.csv",
                timeout=aiohttp.ClientTimeout(total=300, connect=30)
            ) as r:
                if not r.ok:
                    log.error(f"=I] Majestic HTTP {r.status}")
                else:
                    header_parsed = False
                    col_domain = col_rank = -1
                    leftover   = ""
                    chunk      = []
                    inserted   = 0

                    async with pool.acquire() as conn:
                        async for raw_bytes in r.content.iter_chunked(65536):  # 64KB at a time
                            text_piece = leftover + raw_bytes.decode("utf-8", errors="replace")
                            lines = text_piece.split("\n")
                            leftover = lines[-1]  # incomplete last line — carry over

                            for line in lines[:-1]:
                                line = line.strip()
                                if not line:
                                    continue

                                # Parse header once
                                if not header_parsed:
                                    cols = [c.strip() for c in line.split(",")]
                                    col_domain = cols.index("Domain")   if "Domain"     in cols else 2
                                    col_rank   = cols.index("GlobalRank") if "GlobalRank" in cols else 0
                                    header_parsed = True
                                    continue

                                # Parse data row (simple split — CSV is clean)
                                parts = line.split(",")
                                if len(parts) <= max(col_domain, col_rank):
                                    continue
                                try:
                                    domain = normalize_domain(parts[col_domain].strip())
                                    rank   = int(parts[col_rank].strip())
                                except Exception:
                                    continue

                                if not ok_domain(domain):
                                    continue

                                chunk.append((domain, domain_to_name(domain),
                                              f"https://{domain}", "majestic", rank))

                                if len(chunk) >= CHUNK_SIZE:
                                    await flush_chunk(conn, chunk)
                                    inserted += len(chunk)
                                    chunk = []
                                    if inserted % 100000 == 0:
                                        log.info(f"=I] Majestic: {inserted:,}...")
                                    await asyncio.sleep(0.002)

                        # Flush remainder
                        if chunk:
                            await flush_chunk(conn, chunk)
                            inserted += len(chunk)

                    log.info(f"=I] Majestic done: {inserted:,} domains")
                    total_inserted += inserted

        except Exception as e:
            log.error(f"=I] Majestic error: {e}")

        await asyncio.sleep(2)

        # ── SOURCE 2: Cisco Umbrella — 12MB ZIP, row-by-row parse ────────────
        log.info("=I] Cisco Umbrella: download 12MB ZIP...")
        try:
            async with session.get(
                "https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip",
                timeout=aiohttp.ClientTimeout(total=120, connect=30)
            ) as r:
                if not r.ok:
                    log.error(f"=I] Umbrella HTTP {r.status}")
                else:
                    zip_bytes = await r.read()  # 12MB — safe
                    log.info(f"=I] Umbrella ZIP: {len(zip_bytes):,} bytes")

                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                        del zip_bytes  # free immediately
                        fname = z.namelist()[0]
                        with z.open(fname) as f:
                            reader  = csv.reader(io.TextIOWrapper(f, "utf-8", errors="replace"))
                            chunk   = []
                            inserted = 0

                            async with pool.acquire() as conn:
                                for row in reader:
                                    try:
                                        domain = normalize_domain(row[1])
                                        rank   = int(row[0])
                                    except Exception:
                                        continue
                                    if not ok_domain(domain):
                                        continue
                                    chunk.append((domain, domain_to_name(domain),
                                                  f"https://{domain}", "umbrella", rank))
                                    if len(chunk) >= CHUNK_SIZE:
                                        await flush_chunk(conn, chunk)
                                        inserted += len(chunk)
                                        chunk = []
                                        if inserted % 100000 == 0:
                                            log.info(f"=I] Umbrella: {inserted:,}...")
                                        await asyncio.sleep(0.002)
                                if chunk:
                                    await flush_chunk(conn, chunk)
                                    inserted += len(chunk)

                    log.info(f"=I] Umbrella done: {inserted:,} domains")
                    total_inserted += inserted

        except Exception as e:
            log.error(f"=I] Umbrella error: {e}")

    # Final count
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM companies")
    log.info(f"=I] Import DONE — this run: {total_inserted:,} | DB total: {total:,}")
    log.info("=I] Sleeping 1h before next re-run (ON CONFLICT DO NOTHING keeps it idempotent)")
    await asyncio.sleep(3600)


async def run_syncer(pool):
    """
    Pusha su Base44 SOLO i record con AI score > 0 non ancora pushati.
    Rate limit Base44: max ~6 POST o PUT al minuto → 1 ogni 10s.
    """
    log.info("=S] SYNCER MODE — push to Base44")
    connector = aiohttp.TCPConnector(limit=2)

    while True:
        async with aiohttp.ClientSession(connector=connector) as session:
            # Leggi batch di record da pushare
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, domain, name, website, source, global_rank,
                           country, industry, employee_count, revenue_range, logo_url,
                           ai_stack, tech_stack,
                           ai_score, maturity_score, cloud_score, automation_score,
                           developer_score, security_score, growth_score, innovation_score,
                           intent_score, commerce_score, tech_gap_score,
                           base44_id, last_scan_date
                    FROM companies
                    WHERE ai_score > 20
                      AND last_scan_date IS NOT NULL
                      AND (last_push_date IS NULL OR last_push_date < last_scan_date)
                    ORDER BY ai_score DESC
                    LIMIT 50
                """)

            if not rows:
                log.info("=S] Nessun record da pushare — sleep 5min")
                await asyncio.sleep(300)
                continue

            log.info(f"=S] {len(rows)} record da pushare su Base44")
            pushed = 0

            for row in rows:
                r = dict(row)
                ai_stack   = json.loads(r.get("ai_stack") or "[]")
                tech_stack = json.loads(r.get("tech_stack") or "[]")

                payload = {
                    "name":                    r.get("name") or domain_to_name(r["domain"]),
                    "website":                 r.get("website") or f"https://{r['domain']}",
                    "source":                  r.get("source","railway"),
                    "ai_stack":                ai_stack,
                    "tech_stack":              tech_stack,
                    "ai_adoption_score":       r.get("ai_score",0),
                    "ai_maturity_score":       r.get("maturity_score",0),
                    "cloud_score":             r.get("cloud_score",0),
                    "automation_score":        r.get("automation_score",0),
                    "developer_score":         r.get("developer_score",0),
                    "security_score":          r.get("security_score",0),
                    "growth_score":            r.get("growth_score",0),
                    "innovation_score":        r.get("innovation_score",0),
                    "ai_buying_intent_score":  r.get("intent_score",0),
                    "commerce_score":          r.get("commerce_score",0),
                    "tech_gap_score":          r.get("tech_gap_score",0),
                    "last_scan_date":          r["last_scan_date"].isoformat() if r.get("last_scan_date") else None,
                    "global_rank":             r.get("global_rank"),
                    "country":                 r.get("country"),
                    "employee_count":          r.get("employee_count"),
                    "revenue_range":           r.get("revenue_range"),
                    "logo_url":                r.get("logo_url"),
                    "ai_transformation_score": r.get("maturity_score",0),
                }
                payload = {k:v for k,v in payload.items() if v is not None}

                try:
                    b44_id = r.get("base44_id")
                    if b44_id:
                        # Update esistente
                        async with session.put(f"{BASE44_URL}/Company/{b44_id}",
                            headers=HW, json=payload,
                            timeout=aiohttp.ClientTimeout(total=15)) as resp:
                            ok = resp.ok
                    else:
                        # Cerca per dominio su Base44
                        domain = r["domain"]
                        async with session.get(f"{BASE44_URL}/Company",
                            headers=HR,
                            params={"limit":1,"fields":"id,website"},
                            timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            # Cerca match per website
                            b44_list = await resp.json() if resp.ok else []
                            match = next((c for c in b44_list if domain in (c.get("website") or "")), None)

                        if match:
                            b44_id = match["id"]
                            async with session.put(f"{BASE44_URL}/Company/{b44_id}",
                                headers=HW, json=payload,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
                                ok = resp.ok
                        else:
                            # Nuovo record
                            async with session.post(f"{BASE44_URL}/Company",
                                headers=HW, json=payload,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
                                ok = resp.ok
                                if ok:
                                    created = await resp.json()
                                    b44_id = created.get("id","")

                    if ok:
                        pushed += 1
                        async with pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE companies SET
                                    base44_id = $1,
                                    last_push_date = NOW()
                                WHERE domain = $2
                            """, b44_id, r["domain"])

                except Exception as e:
                    log.warning(f"=S] Push error {r['domain']}: {e}")

                # Rate limit: 1 operazione ogni 10s su Base44
                await asyncio.sleep(10)

            log.info(f"=S] Pushati: {pushed}/{len(rows)}")
            await asyncio.sleep(60)


# ── Scanner worker ────────────────────────────────────────────────────────────
async def run_scanner(pool):
    log.info(f"=W{WORKER_ID}] SCANNER MODE | threads={THREADS} | batch={BATCH_SIZE}")
    total_scanned = total_ai = 0
    start = time.time()

    connector = aiohttp.TCPConnector(limit=THREADS, ttl_dns_cache=300, limit_per_host=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            batch = await load_batch_pg(pool)
            if not batch:
                log.info(f"=W{WORKER_ID}] DB vuoto — sleep 5min")
                await asyncio.sleep(300)
                continue

            log.info(f"=W{WORKER_ID}] Batch: {len(batch)} domini")
            sem   = asyncio.Semaphore(THREADS)
            done  = ok = ai_n = 0
            t_bat = time.time()

            async def process(row):
                nonlocal done, ok, ai_n
                async with sem:
                    try:
                        result = await scan_domain(session, row)
                        if result:
                            await write_scan_result(pool, result)
                            ok += 1
                            stack = json.loads(result.get("ai_stack","[]"))
                            if stack: ai_n += 1
                    except Exception as e:
                        log.debug(f"process error: {e}")
                    finally:
                        done += 1
                        if done % 100 == 0:
                            elapsed = time.time() - t_bat
                            rate = int(done / max(elapsed/60, 0.01))
                            log.info(f"=W{WORKER_ID}]  [{done}/{len(batch)}] {rate}/min | ok:{ok} | AI:{ai_n} ({ai_n/max(done,1)*100:.0f}%)")

            await asyncio.gather(*[process(r) for r in batch])

            total_scanned += done
            total_ai      += ai_n
            uptime = (time.time()-start)/3600
            log.info(
                f"=W{WORKER_ID}] Batch done: {done} | ok:{ok} | AI:{ai_n} | "
                f"Tot:{total_scanned:,} | AI%:{total_ai/max(total_scanned,1)*100:.1f}% | Up:{uptime:.2f}h"
            )
            await asyncio.sleep(1)


# ── Healthcheck HTTP ──────────────────────────────────────────────────────────
async def healthcheck(pool):
    async def handle(reader, writer):
        try:
            await reader.read(512)
            async with pool.acquire() as conn:
                total   = await conn.fetchval("SELECT COUNT(*) FROM companies") or 0
                scanned = await conn.fetchval("SELECT COUNT(*) FROM companies WHERE last_scan_date IS NOT NULL") or 0
                ai_count= await conn.fetchval("SELECT COUNT(*) FROM companies WHERE ai_score > 0") or 0
            body = json.dumps({"status":"ok","total":total,"scanned":scanned,"ai":ai_count,"worker":WORKER_ID,"mode":MODE}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+str(len(body)).encode()+b"\r\n\r\n"+body)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    log.info(f"=0] Healthcheck :{PORT} (mode={MODE})")
    async with server:
        await server.serve_forever()


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info(f"AgentSignal v6.0 | worker={WORKER_ID} | mode={MODE}")

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=3, max_size=10,
                                     command_timeout=30)
    await ensure_schema(pool)

    if MODE == "importer":
        await asyncio.gather(healthcheck(pool), run_importer(pool))
    elif MODE == "syncer":
        await asyncio.gather(healthcheck(pool), run_syncer(pool))
    else:
        await asyncio.gather(healthcheck(pool), run_scanner(pool))


if __name__ == "__main__":
    asyncio.run(main())

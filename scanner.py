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
RESCAN_DAYS   = int(os.environ.get("RESCAN_DAYS", "9999"))  # v10 full rescan
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
    linkedin_url    TEXT,
    twitter_url     TEXT,
    
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
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    -- v10 fields: Tech & Business Intelligence
    description     TEXT,
    org_chart       JSONB DEFAULT '[]',
    biz_stack       JSONB DEFAULT '{}',
    technology_dna  JSONB DEFAULT '{}',
    ats_documentation TEXT,
    ats_product_signals JSONB DEFAULT '[]',
    founded_year    INT,
    founded_date    TEXT
);

-- Aggiungi colonne se non esistono (ALTER per DB già esistente)
DO $$ BEGIN
    BEGIN ALTER TABLE companies ADD COLUMN description TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN org_chart JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN biz_stack JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN technology_dna JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN ats_documentation TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN ats_product_signals JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END;
    BEGIN ALTER TABLE companies ADD COLUMN founded_year INT; EXCEPTION WHEN duplicate_column THEN NULL; END;
END $$;

CREATE INDEX IF NOT EXISTS idx_companies_scan   ON companies(last_scan_date NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_companies_push   ON companies(last_push_date NULLS FIRST) WHERE ai_score > 0;
CREATE INDEX IF NOT EXISTS idx_companies_ai     ON companies(ai_score DESC) WHERE ai_score > 0;
CREATE INDEX IF NOT EXISTS idx_companies_worker ON companies(id) WHERE last_scan_date IS NULL;
"""


# ══════════════════════════════════════════════════════════════════════════════
# DETECTION ENGINE v9 — STRATEGIA REALISTICA
# ══════════════════════════════════════════════════════════════════════════════
#
# REALTÀ DEL WEB SCRAPING AI:
#   - I bundle JS frontend NON contengono mai SDK AI (OpenAI/LangChain vivono
#     nel backend server-side, invisibili al browser)
#   - I siti enterprise bloccano i crawler con WAF/CDN
#   - Le pagine /careers redirigono su ATS esterni (Greenhouse, Lever, Workday)
#
# STRATEGIA v9 — TRE LIVELLI:
#   L1. Codice sorgente (homepage + bundle JS):
#       - Endpoint API nelle chiamate fetch/XHR dello script frontend
#       - Dipendenze npm/pip nei manifest JSON embedded
#       - CDN imports con versione (unpkg, jsDelivr, esm.sh)
#       - Header HTTP riflessi (x-powered-by: Next.js, server: nginx)
#
#   L2. Pagine di riferimento tecnico (non editoriali):
#       - /stack, /tech, /engineering, /about/technology
#       - robots.txt, /.well-known/security.txt
#       - sitemap.xml (rivela struttura del sito)
#       Pattern: solo TOOL NAMES specifici (LangChain, Pinecone, ecc.)
#       — non acronimi generici come "LLM", "AI", "ML"
#
#   L3. ATS hiring pages — seguiamo redirect a Greenhouse/Lever/Workday:
#       - Cerca job role con tool tecnici specifici nel titolo/descrizione
#       - Solo tool inequivocabili (LangChain, Pinecone, Weaviate, ecc.)
#       - NON: "AI Engineer", "ML Engineer" (troppo generici)

PRODUCTIVITY_BLACKLIST = {
    "microsoftoffice","googledocs","googlesheets","googleslides","googledrive",
    "microsoftteams","slack","zoom","dropbox","box","notion","confluence",
    "jira","trello","asana","monday","clickup","airtable",
}

# ── L1: Endpoint API e CDN SDK — solo nel codice sorgente ────────────────────
# Questi pattern trovano CHIAMATE DIRETTE all'API nel codice JavaScript/Python
# eseguito dal browser o scritto esplicitamente in script inline/bundle.
AI_API_SIGNATURES = [
    # Endpoint REST univoci — non esistono false positive
    ("OpenAI",         r"api\.openai\.com/v\d+/(chat/completions|embeddings|completions|assistants)"),
    ("Anthropic",      r"api\.anthropic\.com/v\d+/messages"),
    ("Google AI",      r"generativelanguage\.googleapis\.com/v\d+/models"),
    ("Azure OpenAI",   r"openai\.azure\.com/openai/deployments/[^/]+/(chat/completions|completions)"),
    ("AWS Bedrock",    r"bedrock-runtime\.amazonaws\.com/model/"),
    ("Cohere",         r"api\.cohere\.(ai|com)/v\d+/(generate|embed|chat|summarize)"),
    ("Mistral",        r"api\.mistral\.ai/v\d+/(chat/completions|embeddings)"),
    ("Groq",           r"api\.groq\.com/openai/v\d+/chat/completions"),
    ("Perplexity",     r"api\.perplexity\.ai/chat/completions"),
    ("Together AI",    r"api\.together\.(xyz|ai)/v\d+/(chat/completions|completions|inference)"),
    ("Replicate",      r"api\.replicate\.com/v\d+/predictions"),
    ("xAI Grok",       r"api\.x\.ai/v\d+/(chat/completions|completions)"),
    ("Fireworks AI",   r"api\.fireworks\.ai/inference/v\d+/chat/completions"),
    ("Deepseek",       r"api\.deepseek\.com/v\d+/(chat/completions|completions)"),
    ("ElevenLabs",     r"api\.elevenlabs\.io/v\d+/text-to-speech"),
    ("Stability AI",   r"api\.stability\.ai/v\d+/(generation|engines)"),
    # CDN imports con versione — solo unpkg/jsDelivr/esm.sh (non testo libero)
    ("OpenAI SDK",     r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/openai@\d"),
    ("Anthropic SDK",  r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/@anthropic-ai/sdk@\d"),
    ("TensorFlow.js",  r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm)/@tensorflow/tfjs@\d"),
]

# ── L2: Package manager manifests — solo nei JSON embedded nel DOM ────────────
# webpack/vite/turbopack iniettano il manifest delle dipendenze nel DOM.
# Pattern: `"nome-package": "^versione"` — disambigua da nomi generici.
AI_PKG_SIGNATURES = [
    ("OpenAI SDK",     r'"openai"\s*:\s*"[\^~]?\d+\.\d'),
    ("Anthropic SDK",  r'"@anthropic-ai/sdk"\s*:\s*"[\^~]?\d+\.\d'),
    ("LangChain",      r'"(?:langchain|@langchain/core)"\s*:\s*"[\^~]?\d+\.\d'),
    ("LlamaIndex",     r'"llama-index(?:-core)?"\s*:\s*"[\^~]?\d+\.\d'),
    ("Vercel AI SDK",  r'"(?:@ai-sdk/openai|@ai-sdk/anthropic|@ai-sdk/google)"\s*:\s*"[\^~]?\d+\.\d'),
    ("Hugging Face",   r'"@huggingface/inference"\s*:\s*"[\^~]?\d+\.\d'),
    ("Pinecone",       r'"@pinecone-database/pinecone"\s*:\s*"[\^~]?\d+\.\d'),
    ("Weaviate",       r'"weaviate-client"\s*:\s*"[\^~]?\d+\.\d'),
    ("Qdrant",         r'"qdrant-client"\s*:\s*"[\^~]?\d+\.\d'),
    ("Chroma",         r'"chromadb"\s*:\s*"[\^~]?\d+\.\d'),
    ("Ollama",         r'"ollama"\s*:\s*"[\^~]?\d+\.\d'),
    ("LiteLLM",        r'"litellm"\s*:\s*"[\^~]?\d+\.\d'),
    ("Haystack",       r'"haystack-ai"\s*:\s*"[\^~]?\d+\.\d'),
    ("CrewAI",         r'"crewai"\s*:\s*"[\^~]?\d+\.\d'),
    ("AutoGen",        r'"pyautogen"\s*:\s*"[\^~]?\d+\.\d'),
    ("Langfuse",       r'"langfuse"\s*:\s*"[\^~]?\d+\.\d'),
    ("OpenAI Embed",   r'"openai-embeddings"\s*:\s*"[\^~]?\d+\.\d'),
]

# ── L2b: Costruttori SDK — solo nei JS bundle scaricati ──────────────────────
# new OpenAI(), new Anthropic() — inequivocabili anche in codice minificato
AI_SDK_CONSTRUCTORS = [
    ("OpenAI SDK",     r'new OpenAI\(\s*\{'),
    ("Anthropic SDK",  r'new Anthropic\(\s*\{'),
    ("Pinecone",       r'new Pinecone\(\s*\{'),
    ("Weaviate",       r'new WeaviateClient\('),
    ("Qdrant",         r'new QdrantClient\('),
    ("Chroma",         r'new ChromaClient\('),
    ("Groq SDK",       r'new Groq\(\s*\{'),
    ("Mistral SDK",    r'new MistralClient\('),
    ("ElevenLabs",     r'new ElevenLabsClient\('),
]

# ── Tech Stack: fingerprint CDN/framework inequivocabili ─────────────────────
TECH_SIGNATURES = [
    ("React",      [r"react\.production\.min\.js", r"/react@\d+\.\d", r"__reactFiber[A-Za-z0-9]", r"data-reactroot"]),
    ("Next.js",    [r"/_next/static/chunks/", r"__NEXT_DATA__", r"/next@\d+\.\d"]),
    ("Vue",        [r"vue\.global\.prod\.min\.js", r"/vue@\d+\.\d", r"__vue_app__", r"data-v-app"]),
    ("Angular",    [r'ng-version="\d', r"/zone\.js@\d+\.\d"]),
    ("Nuxt",       [r"__NUXT_DATA__", r"/_nuxt/builds/"]),
    ("Svelte",     [r"/svelte@\d+\.\d", r"__svelte[A-Za-z]"]),
    ("Remix",      [r"__remixContext", r"/build/root-[a-f0-9]+\.js"]),
    ("Vercel",     [r"\.vercel\.app", r"/_vercel/insights/", r'x-vercel-id']),
    ("Netlify",    [r"\.netlify\.app", r"netlify-identity-widget\.js"]),
    ("Cloudflare", [r"cdnjs\.cloudflare\.com/ajax/", r"__cf_bm=", r"cf-ray:"]),
    ("AWS",        [r"\.s3\.amazonaws\.com/", r"\.cloudfront\.net/"]),
    ("GCP",        [r"\.storage\.googleapis\.com/", r"\.googlecloud\.com/"]),
    ("Azure",      [r"\.azurewebsites\.net/", r"\.blob\.core\.windows\.net/"]),
    ("Shopify",    [r"cdn\.shopify\.com/s/files/", r"\.myshopify\.com", r"Shopify\.theme\b"]),
    ("Stripe",     [r"js\.stripe\.com/v\d/stripe\.js", r'Stripe\(["\']pk_']),
    ("WooCommerce",[r"/wp-content/plugins/woocommerce/", r"woocommerce-page"]),
    ("HubSpot",    [r"js\.hs-scripts\.com/\d+\.js", r"js\.hsforms\.net/"]),
    ("Intercom",   [r"widget\.intercom\.io/widget/[a-z0-9]+", r"app\.intercom\.io/auth/"]),
    ("Mixpanel",   [r"cdn4?\.mxpnl\.com/libs/"]),
    ("Amplitude",  [r"cdn\.amplitude\.com/libs/amplitude-\d"]),
    ("Sentry",     [r"browser\.sentry-cdn\.com/\d", r"@sentry/browser@\d"]),
    ("Datadog",    [r"datadoghq-browser-agent\.com/", r"browser-sdk\.datadoghq\.com/"]),
    ("Segment",    [r"cdn\.segment\.com/analytics\.js/v\d"]),
    ("PostHog",    [r"(?:app|eu)\.posthog\.com/static/array\.js"]),
    ("WordPress",  [r"/wp-content/themes/[a-zA-Z0-9\-_]+/", r"/wp-includes/js/wp-embed\.", r"wp-json/wp/v2"]),
    ("Webflow",    [r"assets\.website-files\.com/[a-f0-9]{24}/", r"\.webflow\.io/"]),
    ("Supabase",   [r"supabase\.co/rest/v1", r'"@supabase/supabase-js":\s*"[\^~]?\d']),
    ("Firebase",   [r"firebase\.googleapis\.com/v\d", r"firebaseapp\.com/__/auth"]),
    ("Tailwind",   [r"cdn\.tailwindcss\.com", r"tailwindcss@\d+\.\d"]),
    ("Prisma",     [r'"@prisma/client":\s*"[\^~]?\d']),
]

# ── L3: Tool names in pagine tecnico-editoriali NON-news ─────────────────────
# Cerca SOLO nomi di tool specifici (non acronimi generici).
# Pattern applicato SOLO a pagine /engineering, /stack, /tech-stack, /about/technology
# NON su /technology (pagina categoria notizie sui siti di news).
AI_TECH_PAGE_PATTERNS = [
    # Tool inequivocabili — il solo nome è sufficiente in una pagina /stack
    ("LangChain",        r"\bLangChain\b"),
    ("LlamaIndex",       r"\bLlamaIndex\b|\bllama[_\-]index\b"),
    ("AWS Bedrock",      r"\bAWS\s+Bedrock\b"),
    ("Azure OpenAI",     r"\bAzure\s+OpenAI\b"),
    ("Hugging Face",     r"\bHuggingFace\b|\bHugging\s+Face\s+(?:Hub|Transformers|Inference)\b"),
    ("Pinecone",         r"\bPinecone\b"),
    ("Weaviate",         r"\bWeaviate\b"),
    ("Qdrant",           r"\bQdrant\b"),
    ("Chroma",           r"\bChromaDB\b"),
    ("Ollama",           r"\bOllama\b"),
    ("MLflow",           r"\bMLflow\b"),
    ("Kubeflow",         r"\bKubeflow\b"),
    ("Ray Serve",        r"\bRay\s+Serve\b|\bAnyscale\b"),
    ("LiteLLM",          r"\bLiteLLM\b"),
    ("CrewAI",           r"\bCrewAI\b"),
    ("AutoGen",          r"\bAutoGen\b|\bpyautogen\b"),
]

EXCLUDE_DOMAINS = {
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
    "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","apple.com",
    "microsoft.com","netflix.com","spotify.com","cloudflare.com","amazonaws.com",
    "doubleclick.net","googlesyndication.com","gstatic.com","googletagmanager.com",
    "googleapis.com","akamai.net","akamaized.net","fastly.net","cloudfront.net",
    "wp.com","wordpress.com","blogspot.com","tumblr.com","medium.com",
}

# Pagine di news/media — su questi domini /technology è editoriale, non tecnico
NEWS_DOMAINS = re.compile(
    r'ibtimes|techcrunch|wired|verge|engadget|cnet|zdnet|mashable|'
    r'businessinsider|forbes|bloomberg|reuters|wsj|nytimes|theguardian|'
    r'bbc\.co|cnbc|huffpost|dailymail|newsweek|time\.com',
    re.IGNORECASE
)

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
    """Testo visibile — solo per hiring signals in calc_scores."""
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL|re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text).lower()

def _build_code_corpus(html: str, js_bundles: list) -> str:
    """
    Corpus di CODICE da analizzare per L1 (API) e L1b (CDN imports).
    Include: script inline, URL src/href, JSON embedded SPA, bundle JS.
    Esclude: testo visibile della pagina.
    """
    sections = []
    # Script inline
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL|re.IGNORECASE):
        sections.append(m.group(1))
    # URL negli attributi (CDN fingerprint)
    for m in re.finditer(r'(?:src|href|data-src)\s*=\s*["\']([^"\']{5,})["\']', html, re.IGNORECASE):
        sections.append(m.group(1))
    # JSON embedded SPA (webpack/vite manifest + app state)
    for pat in [
        r'__NEXT_DATA__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'__NUXT_DATA__\s*=\s*(\[.{20,}?\])\s*[;<]',
        r'__NUXT__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'__remixContext\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'window\.__APP_STATE__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'window\.__INITIAL_STATE__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'"dependencies"\s*:\s*(\{[^}]{10,}\})',   # package.json dependencies
    ]:
        for m in re.finditer(pat, html, re.DOTALL):
            sections.append(m.group(1)[:12000])
    # JS bundle
    for bundle in js_bundles:
        sections.append(bundle[:50000])
    return " ".join(sections)


def detect(html: str, js_bundles: list) -> tuple:
    """
    Detection v9 — tre livelli nel codice sorgente.
    Nessun falso positivo da testo editoriale.
    """
    corpus = _build_code_corpus(html, js_bundles)
    ai_found, tech_found = [], []

    # L1: endpoint API
    for name, pat in AI_API_SIGNATURES:
        try:
            if re.search(pat, corpus, re.IGNORECASE) and name not in ai_found:
                ai_found.append(name)
        except re.error:
            pass

    # L2: package manifest
    for name, pat in AI_PKG_SIGNATURES:
        try:
            if re.search(pat, corpus, re.IGNORECASE) and name not in ai_found:
                ai_found.append(name)
        except re.error:
            pass

    # L2b: costruttori SDK (solo nei bundle)
    bundle_text = " ".join(js_bundles[:5])
    for name, pat in AI_SDK_CONSTRUCTORS:
        try:
            if re.search(pat, bundle_text, re.IGNORECASE) and name not in ai_found:
                ai_found.append(name)
        except re.error:
            pass

    # Tech stack
    full = html + " " + bundle_text
    for name, patterns in TECH_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, full, re.IGNORECASE) and name not in tech_found:
                    tech_found.append(name)
                    break
            except re.error:
                pass

    return ai_found, tech_found


def detect_from_tech_page(text: str, domain: str) -> list:
    """
    Cerca tool AI specifici nel testo di pagine tecnico/engineering.
    NON usare su domini news/media o su pagine /technology.
    """
    if NEWS_DOMAINS.search(domain):
        return []
    found = []
    for name, pat in AI_TECH_PAGE_PATTERNS:
        try:
            if re.search(pat, text, re.IGNORECASE) and name not in found:
                found.append(name)
        except re.error:
            pass
    return found


def calc_scores(ai_stack: list, tech_stack: list, text: str) -> dict:
    ai_n   = len(ai_stack)
    cloud  = sum(1 for t in tech_stack if t in {"AWS","GCP","Azure","Cloudflare","Vercel"})
    dev    = sum(1 for t in tech_stack if t in {"React","Next.js","Vue","Angular","Nuxt","Svelte","Remix"})
    hiring = sum(1 for kw in ["machine learning engineer","ai engineer","llm engineer",
                               "ml engineer","data scientist","ai researcher"] if kw in text)
    def clamp(v): return min(100.0, max(0.0, float(v)))
    return {
        "ai_score":         clamp(ai_n * 15),
        "maturity_score":   clamp(ai_n * 12 + cloud * 8 + dev * 5 + len(tech_stack) * 2),
        "cloud_score":      clamp(cloud * 25),
        "automation_score": clamp(sum(1 for t in ai_stack if t in
                                   {"LangChain","LlamaIndex","CrewAI","AutoGen","Haystack","Ray Serve"}) * 25),
        "developer_score":  clamp(dev * 15 + cloud * 5),
        "security_score":   clamp(cloud * 20),
        "growth_score":     clamp(hiring * 20),
        "innovation_score": clamp(ai_n * 10 + dev * 5),
        "intent_score":     clamp(ai_n * 10 + hiring * 15),
        "commerce_score":   clamp(sum(1 for t in tech_stack if t in {"Shopify","Stripe","WooCommerce"}) * 30),
        "tech_gap_score":   clamp(max(0, 80 - ai_n * 15 - cloud * 10)),
    }


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
}


async def fetch(session, url: str, timeout: int = 12) -> str:
    try:
        async with session.get(url, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=timeout),
                               allow_redirects=True, max_redirects=5) as r:
            if r.status == 200:
                ct = r.headers.get("Content-Type", "")
                if "text" in ct or "javascript" in ct or "json" in ct:
                    return await r.text(errors="replace")
    except Exception:
        pass
    return ""


async def fetch_js_bundles(session, html: str, base_url: str) -> list:
    """
    Scarica bundle JS in parallelo — tutti tranne analytics/font/ads.
    Costruisce URL assoluti correttamente (path relativo → origin + path).
    """
    SKIP = re.compile(
        r'analytics|gtm|gtag|fbq|pixel|hotjar|clarity|mouseflow|'
        r'fonts?\.(?:google|gstatic)|typekit|font|icon|emoji|polyfill|'
        r'recaptcha|turnstile|consent|gdpr|adsbygoogle|adsense',
        re.IGNORECASE
    )
    try:
        p = urlparse(base_url)
        origin = f"{p.scheme}://{p.netloc}"
    except Exception:
        return []

    js_urls, seen = [], set()
    for m in re.finditer(
        r'<script[^>]+src=["\']([^"\']+\.js(?:[^"\']*)?)["\']',
        html, re.IGNORECASE
    ):
        raw = m.group(1)
        full = raw if raw.startswith("http") else origin + raw
        base_full = full.split("?")[0]  # dedup senza querystring
        if base_full not in seen and not SKIP.search(full):
            seen.add(base_full)
            js_urls.append(full)  # scarica con querystring per validità CDN

    # Scarica fino a 8 bundle in parallelo
    tasks = [fetch(session, u, timeout=7) for u in js_urls[:10]]
    raw = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in raw if isinstance(r, str) and r][:8]


async def fetch_tech_pages(session, base_url: str, domain: str) -> str:
    """
    Scarica pagine tecnico-editoriali NON-news per L3 detection.
    Pagine incluse: /engineering, /stack, /tech-stack, /about/technology,
                    /careers/engineering (ATS esterni seguiti via redirect)
    Pagine ESCLUSE: /technology (editoriale news), /blog, /news
    """
    if NEWS_DOMAINS.search(domain):
        return ""   # Nessuna extra page per siti di news

    p = urlparse(base_url)
    origin = f"{p.scheme}://{p.netloc}"
    pages = [
        f"{origin}/engineering",
        f"{origin}/stack",
        f"{origin}/tech-stack",
        f"{origin}/about/technology",
        f"{origin}/about/engineering",
        f"{origin}/careers",        # ATS con redirect consentito (max_redirects=5)
        f"{origin}/jobs",
    ]
    tasks = [fetch(session, url, timeout=7) for url in pages]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filtra solo pagine con contenuto tecnico reale (non 404 redirect su homepage)
    texts = []
    for r in results:
        if not isinstance(r, str) or not r:
            continue
        # Scarta se la pagina è chiaramente la homepage redirettata (stessa struttura)
        # o se è troppo corta (404 page)
        clean = re.sub(r'<[^>]+>', ' ', r)
        if len(clean.strip()) > 500:
            texts.append(clean[:20000])
    return " ".join(texts)


async def scan_domain(session, row: dict) -> dict | None:
    domain  = row["domain"]
    website = row.get("website") or f"https://{domain}"

    # 1. Homepage
    html = await fetch(session, website)
    if not html:
        html = await fetch(session, website.rstrip("/") + "/")
    if not html.strip():
        return {
            "domain": domain,
            "scan_errors": (row.get("scan_errors") or 0) + 1,
            "last_scan_date": datetime.now(timezone.utc),
        }

    # 2. Bundle JS + pagine tech in parallelo
    js_bundles, tech_page_text = await asyncio.gather(
        fetch_js_bundles(session, html, website),
        fetch_tech_pages(session, website, domain),
    )

    # 3. L1+L2: detection da codice (endpoint API, CDN imports, manifest, bundle)
    ai_from_code, tech_stack = detect(html, js_bundles)

    # 4. L3: detection da pagine tech/engineering (tool names specifici)
    ai_from_pages = detect_from_tech_page(tech_page_text, domain) if tech_page_text else []

    # Merge: codice prima (più affidabile), poi pagine tech
    ai_stack = list(dict.fromkeys(ai_from_code + [t for t in ai_from_pages if t not in ai_from_code]))

    # 5. Scores
    visible_text = extract_text(html)
    scores = calc_scores(ai_stack, tech_stack, visible_text)

    return {
        "domain":         domain,
        "ai_stack":       json.dumps(ai_stack),
        "tech_stack":     json.dumps(tech_stack),
        "last_scan_date": datetime.now(timezone.utc),
        **scores,
    }


# ── Detection Engine v10 ────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# AGENTSIGNAL DETECTION ENGINE v10 — DEEP CODE SCAN
# ══════════════════════════════════════════════════════════════════════════════
#
# PROBLEMA RISOLTO: wandb.ai mostrava LangChain/LlamaIndex/TensorFlow come
# "detected stack" ma sono solo integrazioni mostrate sul marketing site.
# Rilevare tool dal testo della homepage = falsi positivi al 100%.
#
# NUOVA STRATEGIA — solo evidenze tecniche inequivocabili:
#
#  FONTE 1: Bundle JS scaricati (codice minificato del frontend)
#    → package manifest JSON (dependencies: {"langchain": "^0.1.0"})
#    → costruttori SDK (new OpenAI({...}), new Stripe("pk_..."))
#    → endpoint API diretti (fetch("api.openai.com/v1/..."))
#    → CDN imports versionati (cdn.jsdelivr.net/npm/openai@4.x)
#
#  FONTE 2: File tecnici pubblici (non pagine marketing)
#    → /package.json — dipendenze npm reali del progetto
#    → /robots.txt, /sitemap.xml — struttura tecnica
#    → /.well-known/security.txt — tech stack dichiarato
#    → /api/health, /api/status — endpoint tecnici
#
#  FONTE 3: Pagine tecniche NON-marketing
#    → /engineering, /tech-stack, /stack (NON /technology su news sites)
#    → Solo tool names specifici in contesto tecnico ("we use X in production")
#    → NON homepage, NON /docs pubblici, NON /blog, NON /pricing
#
#  BIZ STACK: solo da CDN fingerprint nel DOM
#    → js.stripe.com/v3/stripe.js → Stripe
#    → cdn.shopify.com/s/files/ → Shopify
#    → widget.intercom.io/widget/XXX → Intercom
#    Questi sono inequivocabili: il CDN è presente SOLO se l'azienda usa il tool.
#
# REGOLE ANTI-FALSO-POSITIVO:
#  ✗ Mai rilevare da testo visibile della homepage
#  ✗ Mai rilevare da /docs, /learn, /tutorials (tutorial = non usano il tool)
#  ✗ Mai rilevare da code snippet della documentazione propria
#  ✗ Mai rilevare da "integrations" o "works with" marketing page
#  ✗ Mai rilevare nomi di tool da tag <img> alt, <a> href di partner
# ══════════════════════════════════════════════════════════════════════════════

import re, json, asyncio, aiohttp
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

# ── Siti da escludere completamente ──────────────────────────────────────────
EXCLUDE_DOMAINS = {
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
    "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","apple.com",
    "microsoft.com","netflix.com","spotify.com","cloudflare.com","amazonaws.com",
    "doubleclick.net","googlesyndication.com","gstatic.com","googletagmanager.com",
    "googleapis.com","akamai.net","akamaized.net","fastly.net","cloudfront.net",
    "wp.com","wordpress.com","blogspot.com","tumblr.com","medium.com",
}

NEWS_DOMAINS = re.compile(
    r'ibtimes|techcrunch|wired|theverge|engadget|cnet|zdnet|mashable|'
    r'businessinsider|forbes|bloomberg|reuters|wsj|nytimes|theguardian|'
    r'bbc\.co\.|cnbc|huffpost|dailymail|newsweek|time\.com|venturebeat|'
    r'techradar|tomshardware|arstechnica|gizmodo|techrepublic',
    re.IGNORECASE
)

# ── FONTE 1: AI STACK — solo da codice sorgente ───────────────────────────────

# 1a. Endpoint API REST — chiamate dirette nel codice JS
# Queste stringhe appaiono SOLO quando il frontend chiama direttamente l'API
# (raro ma inequivocabile: chatbot, voice, image gen nel browser)
AI_API_CALLS = [
    ("OpenAI",         r"api\.openai\.com/v\d+/(?:chat/completions|embeddings|completions|assistants)"),
    ("Anthropic",      r"api\.anthropic\.com/v\d+/messages"),
    ("Google Gemini",  r"generativelanguage\.googleapis\.com/v\d+/models"),
    ("Google Vertex",  r"aiplatform\.googleapis\.com/v\d+/projects/"),
    ("Azure OpenAI",   r"openai\.azure\.com/openai/deployments/[^/\"']{3,}/(?:chat/completions|embeddings)"),
    ("AWS Bedrock",    r"bedrock-runtime\.amazonaws\.com/model/"),
    ("Cohere",         r"api\.cohere\.(ai|com)/v\d+/(?:generate|embed|chat|rerank)"),
    ("Mistral",        r"api\.mistral\.ai/v\d+/(?:chat/completions|embeddings)"),
    ("Groq",           r"api\.groq\.com/openai/v\d+/chat/completions"),
    ("Together AI",    r"api\.together\.(xyz|ai)/v\d+/(?:chat/completions|inference)"),
    ("Replicate",      r"api\.replicate\.com/v\d+/predictions"),
    ("ElevenLabs",     r"api\.elevenlabs\.io/v\d+/text-to-speech"),
    ("Hugging Face",   r"api-inference\.huggingface\.co/models/[a-zA-Z0-9\-_/]{5,}"),
    ("Perplexity",     r"api\.perplexity\.ai/chat/completions"),
    ("xAI",            r"api\.x\.ai/v\d+/chat/completions"),
    ("Fireworks AI",   r"api\.fireworks\.ai/inference/v\d+/chat/completions"),
    ("Deepseek",       r"api\.deepseek\.com/v\d+/chat/completions"),
]

# 1b. CDN imports con versione — solo su unpkg/jsDelivr/esm.sh
# Questi appaiono quando si importa un SDK AI direttamente nel browser
AI_CDN_IMPORTS = [
    ("OpenAI SDK",     r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/openai@\d+\.\d"),
    ("Anthropic SDK",  r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/@anthropic-ai/sdk@\d"),
    ("TensorFlow.js",  r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm)/@tensorflow/tfjs@\d"),
    ("ONNX Runtime",   r"cdn\.jsdelivr\.net/npm/onnxruntime-web@\d"),
    ("Transformers.js",r"cdn\.jsdelivr\.net/npm/@xenova/transformers@\d"),
    ("LangChain.js",   r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/@langchain/core@\d"),
    ("Ollama JS",      r"(?:unpkg\.com|cdn\.jsdelivr\.net/npm|esm\.sh)/ollama@\d"),
]

# 1c. Package manifest nei JSON embedded nel DOM (webpack/vite inject)
# Pattern: "package-name": "^1.2.3" — SOLO in JSON reale, non in testo
AI_PACKAGE_MANIFEST = [
    ("OpenAI SDK",     r'"openai"\s*:\s*"[\^~]?\d+\.\d'),
    ("Anthropic SDK",  r'"@anthropic-ai/sdk"\s*:\s*"[\^~]?\d+\.\d'),
    ("LangChain",      r'"(?:langchain|@langchain/core|@langchain/openai|@langchain/anthropic)"\s*:\s*"[\^~]?\d+\.\d'),
    ("LlamaIndex",     r'"llama-index(?:-core)?"\s*:\s*"[\^~]?\d+\.\d'),
    ("Vercel AI SDK",  r'"(?:ai|@ai-sdk/openai|@ai-sdk/anthropic|@ai-sdk/google|@ai-sdk/mistral)"\s*:\s*"[\^~]?\d+\.\d'),
    ("Hugging Face",   r'"@huggingface/inference"\s*:\s*"[\^~]?\d+\.\d'),
    ("Pinecone",       r'"@pinecone-database/pinecone"\s*:\s*"[\^~]?\d+\.\d'),
    ("Weaviate",       r'"weaviate-client"\s*:\s*"[\^~]?\d+\.\d'),
    ("Qdrant",         r'"qdrant-client"\s*:\s*"[\^~]?\d+\.\d'),
    ("Chroma",         r'"chromadb"\s*:\s*"[\^~]?\d+\.\d'),
    ("Milvus",         r'"@zilliz/milvus2-sdk-node"\s*:\s*"[\^~]?\d+\.\d'),
    ("Ollama",         r'"ollama"\s*:\s*"[\^~]?\d+\.\d'),
    ("LiteLLM",        r'"litellm"\s*:\s*"[\^~]?\d+\.\d'),
    ("CrewAI",         r'"crewai"\s*:\s*"[\^~]?\d+\.\d'),
    ("AutoGen",        r'"pyautogen"\s*:\s*"[\^~]?\d+\.\d'),
    ("TensorFlow.js",  r'"@tensorflow/tfjs"\s*:\s*"[\^~]?\d+\.\d'),
    ("Langfuse",       r'"langfuse"\s*:\s*"[\^~]?\d+\.\d'),
    ("LangSmith",      r'"langsmith"\s*:\s*"[\^~]?\d+\.\d'),
    ("Cohere SDK",     r'"cohere-ai"\s*:\s*"[\^~]?\d+\.\d'),
    ("Mistral SDK",    r'"@mistralai/mistralai"\s*:\s*"[\^~]?\d+\.\d'),
    ("Groq SDK",       r'"groq-sdk"\s*:\s*"[\^~]?\d+\.\d'),
    ("Haystack",       r'"haystack-ai"\s*:\s*"[\^~]?\d+\.\d'),
]

# 1d. Costruttori SDK nel codice bundle minificato
# new OpenAI({apiKey:...}) → inequivocabile nel codice minificato
AI_SDK_CONSTRUCTORS = [
    ("OpenAI SDK",     r'new OpenAI\s*\(\s*\{'),
    ("Anthropic SDK",  r'new Anthropic\s*\(\s*\{'),
    ("Pinecone",       r'new Pinecone\s*\(\s*\{'),
    ("Weaviate",       r'new WeaviateClient\s*\('),
    ("Qdrant",         r'new QdrantClient\s*\('),
    ("Groq SDK",       r'new Groq\s*\(\s*\{'),
    ("Mistral SDK",    r'new MistralClient\s*\('),
    ("Cohere SDK",     r'new CohereClient\s*\('),
    ("ElevenLabs",     r'new ElevenLabsClient\s*\('),
]

# ── FONTE 2: /package.json pubblico ───────────────────────────────────────────
# Molti SaaS espongono /package.json — fonte di verità assoluta
PACKAGE_JSON_AI = [
    ("OpenAI SDK",     r'"openai"\s*:'),
    ("Anthropic SDK",  r'"@anthropic-ai/sdk"\s*:'),
    ("LangChain",      r'"(?:langchain|@langchain/core)"\s*:'),
    ("LlamaIndex",     r'"llama-index(?:-core)?"\s*:'),
    ("Vercel AI SDK",  r'"@ai-sdk/(?:openai|anthropic|google|mistral)"\s*:'),
    ("Hugging Face",   r'"@huggingface/inference"\s*:'),
    ("Pinecone",       r'"@pinecone-database/pinecone"\s*:'),
    ("Weaviate",       r'"weaviate-client"\s*:'),
    ("Qdrant",         r'"qdrant-client"\s*:'),
    ("Cohere SDK",     r'"cohere-ai"\s*:'),
    ("Mistral SDK",    r'"@mistralai/mistralai"\s*:'),
    ("Groq SDK",       r'"groq-sdk"\s*:'),
    ("Langfuse",       r'"langfuse"\s*:'),
    ("LangSmith",      r'"langsmith"\s*:'),
    ("Ollama",         r'"ollama"\s*:'),
    ("TensorFlow.js",  r'"@tensorflow/tfjs"\s*:'),
    ("ONNX Runtime",   r'"onnxruntime-web"\s*:'),
]

# ── FONTE 3: Pagine tech/engineering (anti-marketing) ─────────────────────────
# Tool names specifici in pagine /engineering, /stack, /tech-stack
# MAI su /docs, /learn, /tutorials, /integrations, /partners, homepage
# CONTESTO RICHIESTO: "we use", "built with", "powered by", "run on", "deploy"
AI_TECH_PAGE = [
    # Solo tool con nome non generico — non "AI" o "ML" da soli
    ("LangChain",      r"\bLangChain\b"),
    ("LlamaIndex",     r"\bLlamaIndex\b|\bllama[_\-]index\b"),
    ("AWS Bedrock",    r"\bAWS\s+Bedrock\b"),
    ("Azure OpenAI",   r"\bAzure\s+OpenAI\b"),
    ("Google Vertex",  r"\bVertex\s+AI\b"),
    ("Pinecone",       r"\bPinecone\b"),
    ("Weaviate",       r"\bWeaviate\b"),
    ("Qdrant",         r"\bQdrant\b"),
    ("ChromaDB",       r"\bChromaDB\b"),
    ("Milvus",         r"\bMilvus\b"),
    ("Ollama",         r"\bOllama\b"),
    ("MLflow",         r"\bMLflow\b"),
    ("Kubeflow",       r"\bKubeflow\b"),
    ("LiteLLM",        r"\bLiteLLM\b"),
    ("CrewAI",         r"\bCrewAI\b"),
    ("AutoGen",        r"\bAutoGen\b"),
    ("Langfuse",       r"\bLangfuse\b"),
    ("LangSmith",      r"\bLangSmith\b"),
    ("RAG pipeline",   r"\bRAG\b|\bRetrieval[- ]Augmented\s+Generation\b"),
    # Solo OpenAI/Anthropic se citati come stack tecnico usato (non partner)
    ("Anthropic Claude", r"\bClaude\s+(?:3|API|Opus|Sonnet|Haiku)\b"),
    ("Google Gemini",  r"\bGemini\s+(?:1\.5|API|Pro|Ultra)\b"),
    ("Mistral",        r"\bMistral\s+(?:7B|8x7B|Large|API)\b"),
]

# ── BIZ STACK — solo CDN fingerprint inequivocabili ──────────────────────────
# Questi CDN appaiono SOLO sui siti dei clienti (non sui siti dei vendor stessi)
BIZ_CDN_SIGNATURES = {
    # COMMERCE — CDN specifici dei client
    "Shopify":       [r"cdn\.shopify\.com/s/files/", r"\.myshopify\.com",
                      r"Shopify\.theme\b", r"shopify-section"],
    "WooCommerce":   [r"/wp-content/plugins/woocommerce/", r"woocommerce-page"],
    "Magento":       [r"mage/requirejs", r'"Magento_'],
    "BigCommerce":   [r"cdn\d*\.bigcommerce\.com", r"stencil\.js"],
    "PrestaShop":    [r"prestashop", r"/themes/classic/assets/"],
    "Squarespace":   [r"static\d*\.squarespace\.com"],
    "Wix":           [r"static\.wixstatic\.com", r"wix-thunderbolt"],
    # PAYMENTS
    "Stripe":        [r"js\.stripe\.com/v\d/stripe\.js",
                      r'Stripe\s*\(\s*["\']pk_(?:live|test)_'],
    "PayPal":        [r"paypalobjects\.com/api/checkout\.js",
                      r"paypal\.com/sdk/js\?client-id=",
                      r"paypal\.Buttons\s*\("],
    "Adyen":         [r"checkoutshopper-(?:live|test)\.adyen\.com"],
    "Braintree":     [r"js\.braintreegateway\.com/web/"],
    "Klarna":        [r"js\.klarna\.com/", r"klarna-payments"],
    "Mollie":        [r"js\.mollie\.com/v\d"],
    "Paddle":        [r"cdn\.paddle\.com/paddle/paddle\.js"],
    # CRM
    "HubSpot":       [r"js\.hs-scripts\.com/\d+\.js", r"js\.hsforms\.net/",
                      r"hs-analytics\.net", r"hsappstatic\.net"],
    "Salesforce":    [r"salesforceliveagent\.com/content/g/js/",
                      r"salesforce-chat"],
    "Pipedrive":     [r"pipedriveassets\.com"],
    "Zoho":          [r"salesiq\.zoho\.com/widget", r"zohopublic\.com"],
    "ActiveCampaign":[r"trackcmp\.net/"],
    # AUTOMATION
    "n8n":           [r"n8n-widget", r"app\.n8n\.io/embed"],
    "Make":          [r"integromat\.com", r"make\.com/oauth/api/embed"],
    "Zapier":        [r"zapier\.com/(?:partner|embed)/"],
    # SUPPORT
    "Intercom":      [r"widget\.intercom\.io/widget/",
                      r'"intercomSettings"\s*=',
                      r"intercom-container"],
    "Zendesk":       [r"static\.zdassets\.com/ekr/snippet\.js",
                      r'ze\s*\(\s*"webWidget"'],
    "Freshdesk":     [r"fw-cdn\.com/fresh(?:desk|chat)\.js"],
    "Crisp":         [r"client\.crisp\.chat/", r"CRISP_WEBSITE_ID\s*="],
    "Drift":         [r"js\.driftt\.com/include/", r'"driftt_aim"'],
    "Tidio":         [r"code\.tidio\.co/"],
    # MARKETING
    "Mailchimp":     [r"chimpstatic\.com/mcjs-connected"],
    "Klaviyo":       [r"static\.klaviyo\.com/onsite/js/", r"a\.klaviyo\.com"],
    "Brevo":         [r"sibforms\.com/serve/"],
    # ANALYTICS
    "Mixpanel":      [r"cdn4?\.mxpnl\.com/libs/"],
    "Amplitude":     [r"cdn\.amplitude\.com/libs/amplitude-\d"],
    "Segment":       [r"cdn\.segment\.com/analytics\.js/v\d"],
    "PostHog":       [r"(?:app|eu)\.posthog\.com/static/array\.js"],
    "Heap":          [r"heapanalytics\.com/js/heap-\d+\.js"],
    "FullStory":     [r"fullstory\.com/s/fs\.js"],
    "Hotjar":        [r"static\.hotjar\.com/c/hotjar-\d+\.js"],
    "Plausible":     [r"plausible\.io/js/(?:script|plausible)\.js"],
    # MONITORING
    "Sentry":        [r"browser\.sentry-cdn\.com/\d"],
    "Datadog":       [r"datadoghq-browser-agent\.com/", r"browser-sdk\.datadoghq\.com/"],
    "LogRocket":     [r"cdn\.lr-in\.com/LogRocket\.min\.js"],
    "Pendo":         [r"cdn\.pendo\.io/agent/static/"],
}

BIZ_CATEGORIES = {
    "Commerce":   ["Shopify","WooCommerce","Magento","BigCommerce","PrestaShop","Squarespace","Wix"],
    "Payments":   ["Stripe","PayPal","Adyen","Braintree","Klarna","Mollie","Paddle"],
    "CRM":        ["HubSpot","Salesforce","Pipedrive","Zoho","ActiveCampaign"],
    "Automation": ["n8n","Make","Zapier"],
    "Support":    ["Intercom","Zendesk","Freshdesk","Crisp","Drift","Tidio"],
    "Marketing":  ["Mailchimp","Klaviyo","Brevo"],
    "Analytics":  ["Mixpanel","Amplitude","Segment","PostHog","Heap","FullStory","Hotjar","Plausible"],
    "Monitoring": ["Sentry","Datadog","LogRocket","Pendo"],
}

# ── Tech/Framework fingerprint ────────────────────────────────────────────────
TECH_SIGNATURES = [
    ("React",      [r"react\.production\.min\.js", r"/react@\d+\.\d",
                    r"__reactFiber[A-Za-z0-9]", r"data-reactroot"]),
    ("Next.js",    [r"/_next/static/chunks/", r"__NEXT_DATA__", r"/next@\d+\.\d"]),
    ("Vue",        [r"vue\.global\.prod\.min\.js", r"/vue@\d+\.\d",
                    r"__vue_app__", r"data-v-app"]),
    ("Angular",    [r'ng-version="\d', r"/zone\.js@\d+\.\d"]),
    ("Nuxt",       [r"__NUXT_DATA__", r"/_nuxt/builds/"]),
    ("Svelte",     [r"/svelte@\d+\.\d", r"__svelte[A-Za-z]"]),
    ("Remix",      [r"__remixContext", r"/build/root-[a-f0-9]+\.js"]),
    ("Vercel",     [r"\.vercel\.app", r"/_vercel/insights/"]),
    ("Netlify",    [r"netlify-identity-widget\.js"]),
    ("Cloudflare", [r"cdnjs\.cloudflare\.com/ajax/", r"__cf_bm="]),
    ("AWS",        [r"\.s3\.amazonaws\.com/", r"\.cloudfront\.net/"]),
    ("GCP",        [r"\.storage\.googleapis\.com/", r"\.googlecloud\.com/"]),
    ("Azure",      [r"\.azurewebsites\.net/", r"\.blob\.core\.windows\.net/"]),
    ("Supabase",   [r"supabase\.co/rest/v1"]),
    ("Firebase",   [r"firebase\.googleapis\.com/v\d"]),
    ("WordPress",  [r"/wp-content/themes/[a-zA-Z0-9\-_]+/", r"wp-json/wp/v2"]),
    ("Webflow",    [r"assets\.website-files\.com/[a-f0-9]{24}/"]),
    ("Contentful", [r"cdn\.contentful\.com", r"ctfassets\.net"]),
    ("Sanity",     [r"cdn\.sanity\.io"]),
    ("Prisma",     [r'"@prisma/client"\s*:\s*"[\^~]?\d']),
    ("Tailwind",   [r"cdn\.tailwindcss\.com"]),
]

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
    return re.sub(r"[-_]", " ", domain.split(".")[0]).title()

def extract_text(html: str) -> str:
    t = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
    t = re.sub(r'<script[^>]*>.*?</script>', ' ', t, flags=re.DOTALL|re.IGNORECASE)
    t = re.sub(r'<[^>]+>', ' ', t)
    return re.sub(r'\s+', ' ', t).lower()

def _extract_code_corpus(html: str, bundles: list) -> str:
    """
    Estrae SOLO codice sorgente dal DOM — non testo visibile.
    Include: script inline, URL CDN negli attributi, JSON embedded, bundle.
    ESCLUDE: testo visibile, alt text, link text, paragrafi marketing.
    """
    parts = []

    # Script inline (il codice JS nella pagina)
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL|re.IGNORECASE):
        content = m.group(1).strip()
        # Salta script vuoti o < 20 chars
        if len(content) > 20:
            parts.append(content)

    # URL negli attributi src/href — CDN fingerprint
    for m in re.finditer(r'(?:src|href|data-src)\s*=\s*["\']([^"\']{8,})["\']', html, re.IGNORECASE):
        parts.append(m.group(1))

    # JSON embedded SPA (webpack/vite manifest — contiene dependencies)
    for pat in [
        r'__NEXT_DATA__\s*=\s*(\{.{20,100000}?\})\s*[;<]',
        r'__NUXT_DATA__\s*=\s*(\[.{20,}?\])\s*[;<]',
        r'__NUXT__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'__remixContext\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'window\.__APP_STATE__\s*=\s*(\{.{20,}?\})\s*[;<]',
        r'"dependencies"\s*:\s*(\{[^}]{20,}\})',
        r'"devDependencies"\s*:\s*(\{[^}]{20,}\})',
    ]:
        for m in re.finditer(pat, html, re.DOTALL):
            parts.append(m.group(1)[:20000])

    # Bundle JS scaricati
    for b in bundles:
        parts.append(b[:80000])

    return "\n".join(parts)


def detect_ai(html: str, bundles: list, pkg_json: str = "", tech_page_text: str = "", domain: str = "") -> list:
    """
    Rileva AI stack con 4 fonti ordinate per affidabilità decrescente.
    Nessuna fonte usa testo visibile della homepage.
    """
    code = _extract_code_corpus(html, bundles)
    bundle_text = " ".join(b[:80000] for b in bundles[:5])
    found = []

    def add(name):
        if name not in found:
            found.append(name)

    # Fonte 1a: endpoint API diretti nel codice
    for name, pat in AI_API_CALLS:
        try:
            if re.search(pat, code, re.IGNORECASE):
                add(name)
        except re.error: pass

    # Fonte 1b: CDN imports versionati
    for name, pat in AI_CDN_IMPORTS:
        try:
            if re.search(pat, code, re.IGNORECASE):
                add(name)
        except re.error: pass

    # Fonte 1c: package manifest nel DOM
    for name, pat in AI_PACKAGE_MANIFEST:
        try:
            if re.search(pat, code, re.IGNORECASE):
                add(name)
        except re.error: pass

    # Fonte 1d: costruttori SDK nei bundle
    for name, pat in AI_SDK_CONSTRUCTORS:
        try:
            if re.search(pat, bundle_text, re.IGNORECASE):
                add(name)
        except re.error: pass

    # Fonte 2: /package.json pubblico
    if pkg_json:
        for name, pat in PACKAGE_JSON_AI:
            try:
                if re.search(pat, pkg_json, re.IGNORECASE):
                    add(name)
            except re.error: pass

    # Fonte 3: pagine tech/engineering (NON homepage, NON docs, NON news)
    if tech_page_text and not NEWS_DOMAINS.search(domain):
        for name, pat in AI_TECH_PAGE:
            try:
                if re.search(pat, tech_page_text, re.IGNORECASE):
                    add(name)
            except re.error: pass

    # Normalizza: unifica varianti stesso provider, deduplicazione
    _NORM = {
        "Anthropic": "Anthropic Claude",
        "Groq SDK": "Groq", "Mistral SDK": "Mistral AI", "Mistral": "Mistral AI",
        "Cohere SDK": "Cohere", "LangChain.js": "LangChain",
    }
    seen, out = set(), []
    for _n in found:
        _c = _NORM.get(_n, _n)
        if _c not in seen:
            seen.add(_c)
            out.append(_c)
    return out


def detect_biz(html: str, bundles: list) -> dict:
    """
    Rileva BIZ stack solo da CDN fingerprint nel DOM completo.
    Il CDN è presente SOLO se il sito usa davvero il tool.
    """
    full = html + " " + " ".join(b[:30000] for b in bundles[:3])
    result = {}
    for cat, tools in BIZ_CATEGORIES.items():
        detected = []
        for tool in tools:
            for pat in BIZ_CDN_SIGNATURES.get(tool, []):
                try:
                    if re.search(pat, full, re.IGNORECASE):
                        detected.append(tool)
                        break
                except re.error: pass
        if detected:
            result[cat] = detected
    return result


def detect_tech(html: str, bundles: list) -> list:
    full = html + " " + " ".join(b[:30000] for b in bundles[:3])
    found = []
    for name, patterns in TECH_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, full, re.IGNORECASE) and name not in found:
                    found.append(name); break
            except re.error: pass
    return found


def calc_scores(ai_stack: list, biz_stack: dict, tech_stack: list, text: str) -> dict:
    ai_n     = len(ai_stack)
    cloud    = sum(1 for t in tech_stack if t in {"AWS","GCP","Azure","Cloudflare","Vercel"})
    dev      = sum(1 for t in tech_stack if t in {"React","Next.js","Vue","Angular","Nuxt","Svelte","Remix"})
    commerce = len(biz_stack.get("Commerce", []))
    payments = len(biz_stack.get("Payments", []))
    crm      = len(biz_stack.get("CRM", []))
    automation = len(biz_stack.get("Automation", []))
    monitoring = len(biz_stack.get("Monitoring", []))
    hiring   = sum(1 for kw in [
        "machine learning engineer","ai engineer","llm engineer",
        "ml engineer","data scientist","ai researcher",
        "prompt engineer","mlops engineer",
    ] if kw in text)
    def c(v): return min(100.0, max(0.0, float(v)))
    return {
        "ai_score":          c(ai_n * 15),
        "maturity_score":    c(ai_n * 10 + cloud * 6 + dev * 4 + len(tech_stack) * 2),
        "cloud_score":       c(cloud * 25),
        "automation_score":  c(automation * 25 + sum(1 for t in ai_stack if t in
                               {"LangChain","LlamaIndex","CrewAI","AutoGen","Haystack"}) * 15),
        "developer_score":   c(dev * 12 + cloud * 5),
        "security_score":    c(cloud * 15 + monitoring * 10),
        "growth_score":      c(hiring * 20 + crm * 5),
        "innovation_score":  c(ai_n * 10 + dev * 4),
        "intent_score":      c(ai_n * 10 + hiring * 15),
        "commerce_score":    c(commerce * 20 + payments * 15),
        "tech_gap_score":    c(max(0, 80 - ai_n * 15 - cloud * 10)),
    }


# ── HTTP Layer ─────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    # NO Accept-Encoding br — evita brotli errors su aiohttp senza Brotli lib
}

async def fetch(session, url: str, timeout: int = 12) -> str:
    try:
        async with session.get(url, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=timeout),
                               allow_redirects=True, max_redirects=5,
                               ssl=False) as r:
            if r.status == 200:
                ct = r.headers.get("Content-Type", "")
                if "text" in ct or "javascript" in ct or "json" in ct:
                    return (await r.read()).decode("utf-8", errors="replace")
    except Exception:
        pass
    return ""

async def fetch_bundles(session, html: str, base_url: str) -> list:
    """Scarica bundle JS (NON analytics/font/ads) — fino a 8 in parallelo."""
    SKIP = re.compile(
        r'analytics|gtm|gtag|fbq|pixel|hotjar|clarity|mouseflow|'
        r'fonts?\.(?:google|gstatic)|typekit|font|icon|emoji|polyfill|'
        r'recaptcha|turnstile|consent|gdpr|adsbygoogle|adsense',
        re.IGNORECASE
    )
    try:
        p = urlparse(base_url)
        origin = f"{p.scheme}://{p.netloc}"
    except Exception:
        return []

    urls, seen = [], set()
    for m in re.finditer(r'<script[^>]+src=["\']([^"\']+\.js(?:[^"\']*)?)["\']',
                          html, re.IGNORECASE):
        raw  = m.group(1)
        full = raw if raw.startswith("http") else origin + raw
        key  = full.split("?")[0]
        if key not in seen and not SKIP.search(full):
            seen.add(key)
            urls.append(full)

    tasks   = [fetch(session, u, 8) for u in urls[:12]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if isinstance(r, str) and len(r) > 200][:8]


async def fetch_package_json(session, base_url: str) -> str:
    """Tenta di recuperare /package.json pubblico — fonte di verità assoluta."""
    try:
        p = urlparse(base_url)
        url = f"{p.scheme}://{p.netloc}/package.json"
        content = await fetch(session, url, timeout=6)
        # Valida che sia davvero un package.json (deve avere "name" e "dependencies")
        if content and '"dependencies"' in content and '"name"' in content:
            return content[:50000]
    except Exception:
        pass
    return ""


async def fetch_tech_pages(session, base_url: str, domain: str) -> str:
    """
    Recupera pagine tecniche (non marketing/docs) per L3 detection.
    INCLUDE: /engineering, /stack, /tech-stack, /about/technology, /careers
    ESCLUDE: /docs, /learn, /tutorials, /integrations, /partners, /blog
    """
    if NEWS_DOMAINS.search(domain):
        return ""
    try:
        p = urlparse(base_url)
        origin = f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""

    pages = [
        f"{origin}/engineering",
        f"{origin}/stack",
        f"{origin}/tech-stack",
        f"{origin}/about/technology",
        f"{origin}/about/engineering",
        f"{origin}/careers",
        f"{origin}/jobs",
        f"{origin}/about/infrastructure",
    ]

    tasks   = [fetch(session, u, 7) for u in pages]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filtra pagine con contenuto reale (non 404 redirect sulla homepage)
    # e ESCLUDE pagine che sono chiaramente marketing/docs
    texts = []
    for r in results:
        if not isinstance(r, str) or not r:
            continue
        clean = re.sub(r'<[^>]+>', ' ', r)
        clean = re.sub(r'\s+', ' ', clean).strip()
        # Minimo 600 caratteri
        if len(clean) < 600:
            continue
        # Segnale: se la pagina parla di "integrations" o "works with" → skip
        # (potrebbe essere una pagina marketing con tool di altri come wandb.ai)
        if re.search(r'works with|integrat(?:e|es|ion|ions)|partner(?:s|ship)', clean[:500], re.I):
            continue
        texts.append(clean[:30000])

    return " ".join(texts)


async def scan_domain(session, row: dict) -> dict | None:
    domain  = row["domain"]
    website = row.get("website") or f"https://{domain}"

    # Step 1: Fetch homepage
    html = await fetch(session, website)
    if not html:
        html = await fetch(session, f"https://{domain}")
    if not html.strip():
        return {
            "domain":         domain,
            "scan_errors":    (row.get("scan_errors") or 0) + 1,
            "last_scan_date": datetime.now(timezone.utc),
        }

    # Step 2: Tutto in parallelo — bundle JS + package.json + pagine tech
    bundles, pkg_json, tech_text = await asyncio.gather(
        fetch_bundles(session, html, website),
        fetch_package_json(session, website),
        fetch_tech_pages(session, website, domain),
    )

    # ── Step 3: Detection — Digital Maturity Intelligence v11 ──────────────
    from engine_dmi_v11 import (
        detect_biz_stack, detect_tech_stack, detect_ai_signals,
        calc_digital_maturity_scores, build_tech_dna, build_flat_tech_list,
        build_buying_intent_signals, build_gap_signals,
        build_dna_summary, map_scores_to_base44
    )

    # 3a: Business Stack (CDN fingerprint — alta affidabilità)
    biz_stack = detect_biz_stack(html, bundles)

    # 3b: Tech/Framework Stack (CDN fingerprint)
    tech_stack_list = detect_tech_stack(html, bundles)

    # 3c: AI Signals (testo pubblico: careers, blog, product, docs)
    page_texts = {
        "careers":   (await _fetch(session, website + "/careers",  6) or ""),
        "jobs":      (await _fetch(session, website + "/jobs",      6) or ""),
        "blog":      (await _fetch(session, website + "/blog",      6) or ""),
        "product":   (await _fetch(session, website + "/product",   6) or ""),
        "features":  (await _fetch(session, website + "/features",  6) or ""),
        "docs":      (await _fetch(session, website + "/docs",      6) or ""),
        "changelog": (await _fetch(session, website + "/changelog", 6) or ""),
        "homepage":  html[:50000],
    }
    ai_signals = detect_ai_signals(page_texts)

    # 3d: Derivati per campi Base44
    flat_tech   = build_flat_tech_list(biz_stack, tech_stack_list)
    ai_list     = build_ai_signals_list(ai_signals)
    tech_dna    = build_tech_dna(biz_stack, tech_stack_list)

    # Step 4: Digital Maturity Scores
    emp_count = 0
    try: emp_count = int(row.get("employee_count") or 0)
    except: pass
    dmi_scores  = calc_digital_maturity_scores(biz_stack, tech_stack_list, ai_signals, emp_count)
    b44_scores  = map_scores_to_base44(dmi_scores)
    dna_summary = build_dna_summary(biz_stack, tech_stack_list, ai_signals, dmi_scores)

    # Campi Base44:
    # ai_stack             = lista tool tecnologici (Shopify, Stripe, React, AWS...)
    # buying_intent_signals = segnali AI leggibili (AI Hiring Signal · careers page)
    # acquisition_signals  = gap tecnologici (No CRM detected, No Automation...)
    ai_stack              = flat_tech                                    # tool list
    tech_stack            = flat_tech                                    # stesso (compatibilità)
    buying_intent_signals = build_buying_intent_signals(ai_signals)      # AI signals leggibili
    acquisition_signals   = build_gap_signals(dmi_scores)                # gap tecnologici
    scores                = b44_scores

    # Step 5: Company enrichment (description, CEO, revenue, founded)
    try:
        company_name = row.get("name") or domain_to_name(domain)
        enrichment   = await enrich_company(session, domain, company_name)
    except Exception:
        enrichment = {}

    return {
        "domain":                domain,
        "ai_stack":              json.dumps(ai_stack),             # lista tool (Shopify, Stripe...)
        "tech_stack":            json.dumps(tech_stack),
        "buying_intent_signals": json.dumps(buying_intent_signals),# segnali AI leggibili
        "acquisition_signals":   json.dumps(acquisition_signals),  # gap tecnologici
        "technology_dna":        json.dumps(tech_dna),
        "biz_stack":             json.dumps(biz_stack),
        "ats_documentation":     dna_summary,
        "description":           enrichment.get("description") or None,
        "industry":              enrichment.get("industry") or None,
        "employee_count":        enrichment.get("employee_count") or None,
        "revenue_range":         enrichment.get("revenue_range") or None,
        "country":               enrichment.get("country") or None,
        "founded_year":          enrichment.get("founded_year") or None,
        "org_chart":             json.dumps(enrichment.get("org_chart") or []),
        "logo_url":              enrichment.get("logo_url") or None,
        "linkedin_url":          enrichment.get("linkedin_url") or None,
        "last_scan_date":        datetime.now(timezone.utc),
        **scores,
    }


# ── Enricher v2 ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# AGENTSIGNAL ENRICHER v2 — Company Intelligence Enrichment
# Fonti (tutte gratuite):
#   1. Schema.org JSON-LD (homepage)   → description, logo, linkedin, founded
#   2. Meta tags (homepage)            → description fallback
#   3. DuckDuckGo Instant API          → abstract, revenue, founded, key people
#   4. /about /team /leadership pages  → people + titles
#   5. Clearbit Logo API               → logo fallback
# ══════════════════════════════════════════════════════════════════════════════

import re, json, asyncio, aiohttp
from urllib.parse import urlparse

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,*/*;q=0.8",
}
HEADERS_DDG = {"User-Agent": "Mozilla/5.0 (compatible; AgentSignal/1.0)"}

EXEC_TITLE_RE = re.compile(
    r'\b(CEO|Chief Executive Officer|Co-Founder|Founder|CTO|Chief Technology Officer|'
    r'COO|Chief Operating Officer|CFO|Chief Financial Officer|President|'
    r'VP of (?:Engineering|Product|Sales|Marketing)|'
    r'Head of (?:Engineering|Product|Design|Sales)|'
    r'Managing Director|General Manager)\b',
    re.IGNORECASE
)
STOP_WORDS = re.compile(
    r'\b(About|Our|The|We|Join|Meet|Leadership|Team|Company|Products?|Services?|'
    r'See|View|Read|Learn|Contact|Home|Back|Next|More|Get|New|All|By|For|At|In|On)\b',
    re.IGNORECASE
)
NAME_RE = re.compile(r'\b([A-Z][a-z]{1,20}\s+(?:[A-Z][a-z]{0,3}\.\s+)?[A-Z][a-z]{1,25})\b')


async def _fetch(session, url, timeout=9):
    try:
        async with session.get(
            url, headers=HEADERS_WEB,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True, max_redirects=3, ssl=False
        ) as r:
            if r.status == 200:
                return (await r.read()).decode("utf-8", errors="replace")
    except Exception:
        pass
    return ""


def _schema_org(html):
    result = {}
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    ):
        try:
            d = json.loads(m.group(1).strip())
            nodes = d if isinstance(d, list) else d.get("@graph", [d])
            for node in nodes:
                t = str(node.get("@type", ""))
                if not any(x in t for x in ("Organization","Corporation","LocalBusiness","Company")):
                    continue
                if not result.get("description"):
                    desc = node.get("description") or node.get("slogan","")
                    if desc: result["description"] = str(desc)[:500]
                if not result.get("founded_year"):
                    fd = node.get("foundingDate","")
                    if fd: result["founded_year"] = str(fd)[:4]
                emp = node.get("numberOfEmployees")
                if isinstance(emp, dict) and not result.get("employee_count"):
                    result["employee_count"] = emp.get("value")
                elif emp and not result.get("employee_count"):
                    result["employee_count"] = emp
                logo = node.get("logo")
                if not result.get("logo_url"):
                    if isinstance(logo, dict): result["logo_url"] = logo.get("url","")
                    elif isinstance(logo, str): result["logo_url"] = logo
                same_as = node.get("sameAs") or []
                if isinstance(same_as, str): same_as = [same_as]
                for url in same_as:
                    if "linkedin.com/company" in url and not result.get("linkedin_url"):
                        result["linkedin_url"] = url
                    if ("twitter.com" in url or "x.com" in url) and not result.get("twitter_url"):
                        result["twitter_url"] = url
                # Founder
                founder = node.get("founder") or node.get("founders")
                if founder:
                    fl = founder if isinstance(founder, list) else [founder]
                    for f in fl[:2]:
                        name = (f.get("name","") if isinstance(f,dict) else str(f)).strip()
                        if name and len(name.split()) >= 2:
                            result.setdefault("people",[]).append(
                                {"name": name, "title": "Founder"}
                            )
        except Exception:
            pass
    return result


def _meta_description(html):
    best = ""
    for m in re.finditer(
        r'<meta[^>]+(?:property|name)\s*=\s*["\']([^"\']+)["\'][^>]+content\s*=\s*["\']([^"\']{15,500})["\']',
        html, re.IGNORECASE
    ):
        prop, content = m.group(1).lower(), m.group(2).strip()
        if prop in ("og:description","twitter:description","description"):
            if len(content) > len(best):
                best = content
    return best


async def _duckduckgo(session, company_name):
    """DuckDuckGo Instant Answer API — gratuita, no auth."""
    try:
        async with session.get(
            "https://api.duckduckgo.com/",
            params={"q": company_name, "format": "json",
                    "no_html": "1", "skip_disambig": "1"},
            headers=HEADERS_DDG,
            timeout=aiohttp.ClientTimeout(total=8), ssl=False
        ) as r:
            if r.status != 200: return {}
            d = await r.json(content_type=None)
    except Exception:
        return {}

    result = {}

    abstract = str(d.get("Abstract","")).strip()
    if abstract and len(abstract) > 30:
        result["ddg_abstract"] = abstract[:600]

    infobox = d.get("Infobox") or {}
    items   = infobox.get("content") or [] if isinstance(infobox, dict) else []
    for item in items:
        if not isinstance(item, dict): continue
        label = str(item.get("label","")).lower()
        value = str(item.get("value","")).strip()
        if not value: continue
        if "founded" in label:
            y = re.search(r'\d{4}', value)
            if y: result["founded_year"] = y.group()
        elif "revenue" in label:
            result["revenue_range"] = value[:60]
        elif "employee" in label or "headcount" in label:
            n = re.search(r'[\d,]+', value.replace(",",""))
            if n: result["employee_count_ddg"] = int(n.group().replace(",",""))
        elif "key people" in label or "ceo" in label or "founder" in label:
            result.setdefault("key_people_raw", []).append(value[:100])
        elif "industry" in label:
            result["industry"] = value[:80]
        elif "headquarters" in label or "location" in label:
            result["hq"] = value[:80]

    return result


def _parse_people(html, limit=8):
    """Estrae people con titoli dalla pagina /about o /team."""
    people = []
    seen   = set()

    # JSON-LD Person (più affidabile)
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    ):
        try:
            d = json.loads(m.group(1))
            nodes = d if isinstance(d, list) else d.get("@graph", [d])
            for node in nodes:
                if str(node.get("@type","")) != "Person": continue
                name  = str(node.get("name","")).strip()
                title = str(node.get("jobTitle","")).strip()
                if len(name.split()) < 2 or name.lower() in seen: continue
                seen.add(name.lower())
                entry = {"name": name, "title": title}
                li = node.get("sameAs","")
                if isinstance(li, list):
                    li = next((x for x in li if "linkedin" in str(x)),"")
                if "linkedin" in str(li): entry["linkedin"] = li
                people.append(entry)
        except: pass

    if len(people) >= 3:
        return people[:limit]

    # HTML fallback: trova titolo exec + nome vicino
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL|re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '|', text)
    segs = [s.strip() for s in re.split(r'\|+', text) if s.strip() and len(s.strip()) > 1]

    for i, seg in enumerate(segs):
        if not EXEC_TITLE_RE.search(seg) or len(seg) > 100: continue
        title = seg.strip()[:80]
        window = segs[max(0,i-10):i+10]
        for w in window:
            if len(w) < 4 or len(w) > 50: continue
            if STOP_WORDS.match(w.strip()): continue
            names = NAME_RE.findall(w)
            for name in names:
                if len(name.split()) < 2: continue
                if STOP_WORDS.search(name): continue
                key = name.lower()
                if key in seen: continue
                seen.add(key)
                people.append({"name": name, "title": title})
                if len(people) >= limit: return people
    return people


def _infer_range(n):
    if not n: return None
    try:
        n = int(str(n).replace(",","").split("-")[0])
        if n < 11:    return "1-10"
        if n < 51:    return "11-50"
        if n < 201:   return "51-200"
        if n < 501:   return "201-500"
        if n < 1001:  return "501-1K"
        if n < 5001:  return "1K-5K"
        if n < 10001: return "5K-10K"
        if n < 50001: return "10K-50K"
        return "50K+"
    except:
        return None


async def enrich_company(session, domain, company_name=None):
    """
    Arricchisce azienda con description, people, logo, revenue, founded, ecc.
    100% gratuito — Schema.org + DuckDuckGo + page scraping.
    """
    if not company_name:
        company_name = re.sub(r"[-_]"," ", domain.split(".")[0]).title()

    base = f"https://{domain}"

    # Fetch tutto in parallelo
    homepage, about_pg, team_pg, leadership_pg, ddg = await asyncio.gather(
        _fetch(session, base, 10),
        _fetch(session, f"{base}/about", 7),
        _fetch(session, f"{base}/team", 7),
        _fetch(session, f"{base}/leadership", 7),
        _duckduckgo(session, company_name),
    )

    # Aggiorna nome da og:site_name della homepage (più affidabile di domain.split)
    if homepage:
        import re as _re
        _og_pats = [
            r'property="og:site_name"\s+content="([^"]{2,50})"',
            r'content="([^"]{2,50})"\s+property="og:site_name"',
            r"property='og:site_name'\s+content='([^']{2,50})'",
            r"content='([^']{2,50})'\s+property='og:site_name'",
        ]
        for _p in _og_pats:
            _m = _re.search(_p, homepage, _re.IGNORECASE)
            if _m and _m.group(1).strip():
                company_name = _m.group(1).strip()
                break
    schema = _schema_org(homepage)
    meta   = _meta_description(homepage)

    # People da tutte le pagine
    people, seen_keys = [], set()
    for html_src in [about_pg, team_pg, leadership_pg]:
        for p in _parse_people(html_src):
            k = p["name"].lower()
            if k not in seen_keys:
                seen_keys.add(k); people.append(p)
    people = people[:8]

    # Key people da DuckDuckGo
    for raw in ddg.get("key_people_raw",[]):
        m = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', raw)
        title_m = re.search(r'\(([^)]+)\)', raw)
        if m:
            name  = m.group(1)
            title = title_m.group(1) if title_m else ""
            k = name.lower()
            if k not in seen_keys:
                seen_keys.add(k)
                people.append({"name": name, "title": title})

    # Description finale (priorità)
    description = (
        schema.get("description") or
        ddg.get("ddg_abstract") or
        meta or
        ""
    )[:500]

    # Employee count
    emp = schema.get("employee_count") or ddg.get("employee_count_ddg")
    emp_range = _infer_range(emp)

    # Revenue
    revenue = ddg.get("revenue_range","")

    # Founded
    founded = schema.get("founded_year") or ddg.get("founded_year","")

    # Logo
    logo = schema.get("logo_url") or f"https://logo.clearbit.com/{domain}"

    # Org chart (CEO + exec) come JSON per il campo org_chart
    org_chart = json.dumps(people) if people else "[]"

    return {
        "description":    description,
        "employee_count": int(str(emp).replace(",","")) if emp else None,
        "revenue_range":  revenue or None,
        "founded_year":   founded or None,
        "logo_url":       logo,
        "linkedin_url":   schema.get("linkedin_url",""),
        "twitter_url":    schema.get("twitter_url",""),
        "industry":       ddg.get("industry",""),
        "country":        ddg.get("hq","").split(",")[-1].strip() if ddg.get("hq") else None,
        "people":         people,
        "org_chart":      org_chart,
    }


# ── PostgreSQL helpers ─────────────────────────────────────────────────────────
async def ensure_schema(pool):
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        # Aggiungi colonne nuove se mancanti (tabella già esistente)
        migrations = [
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS biz_stack JSONB DEFAULT '{}'",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_push_date TIMESTAMPTZ",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS base44_id TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS description TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS founded_year TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS org_chart JSONB DEFAULT '[]'",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS linkedin_url TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS twitter_url TEXT",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS tech_stack JSONB DEFAULT '[]'",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS tech_gap_score INT DEFAULT 0",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS innovation_score INT DEFAULT 0",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS commerce_score INT DEFAULT 0",
            "CREATE INDEX IF NOT EXISTS idx_companies_push ON companies(last_push_date NULLS FIRST) WHERE ai_score > 0",
        ]
        for sql in migrations:
            try:
                await conn.execute(sql)
            except Exception as e:
                log.warning(f"Migration skip: {e}")
    log.info("=0] Schema DB OK")


async def write_scan_result(pool, result: dict):
    if not result: return
    # Serializza JSONB come stringa — asyncpg usa cast ::text::jsonb nel SQL
    def to_json_str(v, fallback="{}"):
        if v is None: return fallback
        if isinstance(v, (dict, list)): return json.dumps(v)
        try: json.loads(v); return v  # già stringa JSON valida
        except: return fallback
    biz = to_json_str(result.get("biz_stack"), "{}")
    orc = to_json_str(result.get("org_chart"), "[]")
    ais = to_json_str(result.get("ai_stack"), "[]")
    tec = to_json_str(result.get("tech_stack"), "[]")
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE companies SET
                ai_stack        = $1::text::jsonb,
                tech_stack      = $2::text::jsonb,
                biz_stack       = $3::text::jsonb,
                ai_score        = $4,
                maturity_score  = $5,
                cloud_score     = $6,
                automation_score= $7,
                developer_score = $8,
                security_score  = $9,
                growth_score    = $10,
                innovation_score= $11,
                intent_score    = $12,
                commerce_score  = $13,
                tech_gap_score  = $14,
                last_scan_date  = $15,
                scan_errors     = COALESCE($16, scan_errors),
                description     = COALESCE($18, description),
                industry        = COALESCE($19, industry),
                founded_year    = COALESCE($20, founded_year),
                org_chart       = COALESCE($21::jsonb, org_chart),
                logo_url        = COALESCE($22, logo_url),
                linkedin_url    = COALESCE($23, linkedin_url),
                updated_at      = NOW()
            WHERE domain = $17
        """,
            ais,
            tec,
            biz,
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
            result.get("description") or None,
            result.get("industry") or None,
            result.get("founded_year") or None,
            orc,
            result.get("logo_url") or None,
            result.get("linkedin_url") or None,
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
                biz_stack  = json.loads(r.get("biz_stack") or "{}")

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
                    "ats_technology_adoption":  json.dumps(json.loads(r.get("biz_stack") or "{}")),
                    "description":              r.get("description") or None,
                    "industry":                 r.get("industry") or None,
                    "org_chart":                r.get("org_chart") or None,
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
            done  = ok = ai_n = biz_n = 0
            t_bat = time.time()

            async def process(row):
                nonlocal done, ok, ai_n, biz_n
                async with sem:
                    try:
                        result = await scan_domain(session, row)
                        if result:
                            await write_scan_result(pool, result)
                            ok += 1
                            ai_stack  = json.loads(result.get("ai_stack","[]"))
                            biz_stack = json.loads(result.get("biz_stack","{}"))
                            if ai_stack:  ai_n  += 1
                            if biz_stack: biz_n += 1
                    except Exception as e:
                        log.warning(f"=W{WORKER_ID}] process error: {type(e).__name__}: {e}")
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
                f"=W{WORKER_ID}] Batch done: {done} | ok:{ok} | AI:{ai_n} | Biz:{biz_n} | "
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
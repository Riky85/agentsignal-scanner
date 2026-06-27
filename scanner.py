#!/usr/bin/env python3
"""
AgentSignal Scanner v5.0 — Railway Production Worker
=========================================================
Fix rispetto a v4.x:
  1. Healthcheck HTTP su $PORT (richiesto da Railway per non killare il processo)
  2. ssl=True (default corretto per HTTPS)
  3. Batch vuoto → sleep 2min invece di 10min (keep-alive)
  4. load_batch con skip stabile (sort=last_scan_date, avanza sempre)
  5. Gestione eccezioni granulare per evitare crash silenzioso
  6. Log strutturato per Railway
"""

import asyncio
import aiohttp
import time
import os
import logging
import json
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [W%(message)s",
    force=True
)
log = logging.getLogger(__name__)

# ── Config da env ─────────────────────────────────────────────────────────────
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
PORT          = int(os.environ.get("PORT", "8080"))

log.info(f"={WORKER_ID}/{TOTAL_WORKERS}] v5.0 | threads={THREADS} | batch={BATCH_SIZE} | apollo={'YES' if APOLLO_KEY else 'NO'} | port={PORT}")

# ── Blacklist produttività ─────────────────────────────────────────────────────
PRODUCTIVITY_BLACKLIST = {
    "microsoft office","google docs","google sheets","google slides",
    "excel","word","powerpoint","notion","confluence","jira","trello",
    "asana","monday.com","basecamp","slack","teams","zoom","gmail",
    "outlook","dropbox","box.com","sharepoint","onedrive",
}

# ── AI Signatures (L1-L4) ─────────────────────────────────────────────────────
AI_SIGNATURES = [
    # L1 — API endpoints diretti (peso 40)
    ("OpenAI",       [r"api\.openai\.com",r"openai\.com/v1/",r"OPENAI_API_KEY",r"sk-[a-zA-Z0-9]{20}"], 1, 40),
    ("Anthropic",    [r"api\.anthropic\.com",r"anthropic\.com/v1",r"ANTHROPIC_API_KEY",r"sk-ant-"], 1, 40),
    ("Google AI",    [r"generativelanguage\.googleapis\.com",r"aiplatform\.googleapis\.com",r"vertexai"], 1, 38),
    ("Azure OpenAI", [r"openai\.azure\.com",r"\.openai\.azure\.com/openai/deployments"], 1, 38),
    ("AWS Bedrock",  [r"bedrock-runtime\.amazonaws\.com",r"bedrock\.amazonaws\.com"], 1, 38),
    ("Cohere",       [r"api\.cohere\.ai",r"api\.cohere\.com",r"cohere-python"], 1, 35),
    ("Mistral",      [r"api\.mistral\.ai",r"mistral-client"], 1, 35),
    ("Groq",         [r"api\.groq\.com",r"groq-sdk",r"groq\.com/openai/v1"], 1, 35),
    ("Perplexity",   [r"api\.perplexity\.ai"], 1, 33),
    ("Together AI",  [r"api\.together\.xyz",r"together\.ai/inference"], 1, 33),
    ("Replicate",    [r"api\.replicate\.com",r"replicate\.com/predictions"], 1, 33),
    ("xAI Grok",     [r"api\.x\.ai",r"x\.ai/api/v1"], 1, 33),
    ("Fireworks AI", [r"api\.fireworks\.ai"], 1, 32),
    ("Deepseek",     [r"api\.deepseek\.com",r"deepseek-chat"], 1, 32),

    # L2 — SDK/librerie Python (peso 25)
    ("LangChain",    [r"langchain",r"from langchain",r"langchain-core"], 2, 25),
    ("LlamaIndex",   [r"llama.?index",r"llama_index",r"from llama_index"], 2, 25),
    ("Hugging Face", [r"huggingface\.co",r"transformers",r"from transformers"], 2, 22),
    ("Pinecone",     [r"pinecone\.io",r"pinecone-client",r"pinecone\.init"], 2, 22),
    ("Weaviate",     [r"weaviate\.io",r"weaviate-client"], 2, 20),
    ("Qdrant",       [r"qdrant\.tech",r"qdrant-client"], 2, 20),
    ("Chroma",       [r"chromadb",r"chroma-db"], 2, 18),
    ("OpenCV",       [r"opencv",r"cv2"], 2, 15),
    ("PyTorch",      [r"pytorch\.org",r"torch\.nn",r"import torch"], 2, 15),
    ("TensorFlow",   [r"tensorflow",r"import tensorflow"], 2, 15),
    ("scikit-learn", [r"scikit.learn",r"sklearn"], 2, 12),
    ("Weights & Biases",[r"wandb",r"weights-and-biases",r"wandb\.init"], 2, 12),
    ("Ray",          [r"ray\.io",r"import ray"], 2, 12),
    ("Ollama",       [r"ollama\.ai",r"ollama-python",r"ollama\.chat"], 2, 20),
    ("Midjourney",   [r"midjourney",r"mid-journey"], 2, 15),
    ("Stable Diffusion",[r"stable.diffusion",r"stabilityai",r"stability\.ai"], 2, 18),

    # L3 — Menzioni esplicite (peso 15)
    ("ChatGPT",      [r"chatgpt",r"chat\.openai\.com"], 3, 15),
    ("Claude",       [r"claude\.ai",r"anthropic claude",r"claude-3"], 3, 15),
    ("Gemini",       [r"gemini\.google\.com",r"google gemini",r"gemini-pro"], 3, 15),
    ("Copilot",      [r"github\.com/features/copilot",r"github copilot",r"ms copilot"], 3, 12),
    ("Cursor",       [r"cursor\.sh",r"cursor\.so",r"cursor ai"], 3, 12),
    ("Vercel AI SDK",[r"vercel\.com/ai",r"@vercel/ai",r"ai-sdk"], 3, 15),
    ("Langfuse",     [r"langfuse\.com"], 3, 12),
    ("Helicone",     [r"helicone\.ai"], 3, 12),
    ("Flowise",      [r"flowiseai\.com",r"flowise"], 3, 10),

    # L4 — Segnali contestuali (peso 8)
    ("AI Platform",  [r"/ai\b",r"/artificial-intelligence\b",r"/machine-learning\b"], 4, 8),
    ("ML Hiring",    [r"machine learning engineer",r"ai researcher",r"llm engineer"], 4, 8),
    ("AI Features",  [r"powered by ai",r"ai-powered",r"artificial intelligence platform"], 4, 6),
]

# ── Tech Stack ────────────────────────────────────────────────────────────────
TECH_SIGNATURES = [
    ("React",     [r"react\.js",r"reactjs",r"_react",r"__REACT"]),
    ("Next.js",   [r"next\.js",r"_next/static",r"__NEXT_DATA__"]),
    ("Vue",       [r"vue\.js",r"vuejs",r"__vue"]),
    ("Angular",   [r"angular\.js",r"angularjs",r"ng-version"]),
    ("Vercel",    [r"vercel\.app",r"vercel\.com",r"_vercel"]),
    ("Netlify",   [r"netlify\.app",r"netlify\.com"]),
    ("Cloudflare",[r"cloudflare\.com",r"cf-ray",r"__cf_bm"]),
    ("AWS",       [r"amazonaws\.com",r"cloudfront\.net",r"s3\.amazonaws"]),
    ("GCP",       [r"googleapis\.com",r"googlecloud\.com"]),
    ("Azure",     [r"azure\.com",r"azurewebsites\.net"]),
    ("Shopify",   [r"shopify\.com",r"cdn\.shopify\.com",r"myshopify"]),
    ("Stripe",    [r"stripe\.com",r"js\.stripe\.com"]),
    ("Segment",   [r"segment\.com",r"segment\.io",r"analytics\.js"]),
    ("HubSpot",   [r"hubspot\.com",r"hs-scripts",r"hubspot"]),
    ("Intercom",  [r"intercom\.io",r"widget\.intercom\.io"]),
    ("Mixpanel",  [r"mixpanel\.com",r"mixpanel"]),
    ("Amplitude", [r"amplitude\.com",r"amplitude"]),
    ("Sentry",    [r"sentry\.io",r"browser\.sentry-cdn"]),
    ("Datadog",   [r"datadoghq\.com",r"datadog-rum"]),
    ("Webflow",   [r"webflow\.com",r"webflow\.io"]),
    ("WordPress", [r"wp-content",r"wp-includes",r"wordpress"]),
    ("Docker",    [r"docker\.com",r"dockerfile",r"docker-compose"]),
    ("Kubernetes",[r"kubernetes\.io",r"k8s",r"kubectl"]),
    ("Terraform", [r"terraform\.io",r"hashicorp"]),
]


# ── Healthcheck HTTP server ───────────────────────────────────────────────────
async def healthcheck_server():
    """
    Mini HTTP server su $PORT per soddisfare Railway.
    Risponde 200 OK a qualsiasi richiesta GET.
    """
    async def handle(reader, writer):
        try:
            await reader.read(1024)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    log.info(f"={WORKER_ID}] Healthcheck HTTP su :{PORT}")
    async with server:
        await server.serve_forever()


# ── Utilities ─────────────────────────────────────────────────────────────────
def normalize_domain(url):
    if not url:
        return ""
    try:
        if not url.startswith("http"):
            url = "https://" + url
        d = urlparse(url).netloc.lower()
        return d.replace("www.", "").strip()
    except Exception:
        return url.lower().strip()


def extract_text(html: str) -> str:
    """Estrae testo pulito + JSON embedded da HTML."""
    # JSON embedded (Next.js __NEXT_DATA__, ecc.)
    json_texts = []
    for m in re.finditer(r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE):
        json_texts.append(m.group(1))
    for m in re.finditer(r'__NEXT_DATA__\s*=\s*({.*?})\s*[;<]', html, re.DOTALL):
        json_texts.append(m.group(1))

    # Rimuovi tag HTML
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).lower()

    # Aggiungi JSON embedded
    for jt in json_texts:
        text += " " + jt.lower()

    return text


def detect_tech(text: str, html: str, url: str) -> tuple[list, list, list]:
    """
    Rileva tecnologie AI e stack tecnico.
    Ritorna: (ai_stack, tech_stack, evidence_list)
    """
    combined = (text + " " + html).lower()
    ai_found = []
    ai_evidence = []
    tech_found = []

    for name, patterns, level, weight in AI_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, combined, re.IGNORECASE):
                    clean = name.lower().replace(" ", "")
                    if clean not in PRODUCTIVITY_BLACKLIST and name not in ai_found:
                        ai_found.append(name)
                        ai_evidence.append({
                            "tech": name,
                            "pattern": pat,
                            "level": f"L{level}",
                            "weight": weight,
                            "source": url
                        })
                    break
            except re.error:
                continue

    for name, patterns in TECH_SIGNATURES:
        for pat in patterns:
            try:
                if re.search(pat, combined, re.IGNORECASE) and name not in tech_found:
                    tech_found.append(name)
                    break
            except re.error:
                continue

    return ai_found, tech_found, ai_evidence


def calculate_scores(ai_stack, tech_stack, html_text, company):
    """Calcola i 10 score proprietari."""
    ai_count = len(ai_stack)
    tech_count = len(tech_stack)

    # Segnali di crescita
    growth_signals = sum(1 for kw in ["hiring", "careers", "open position", "we're growing", "join us"]
                         if kw in html_text)
    intent_signals = sum(1 for kw in ["powered by ai", "ai-powered", "machine learning", "llm", "gpt"]
                         if kw in html_text)
    cloud_tech = sum(1 for t in tech_stack if t in ["AWS", "GCP", "Azure", "Cloudflare", "Vercel"])
    dev_tech   = sum(1 for t in tech_stack if t in ["React", "Next.js", "Vue", "Angular", "Docker", "Kubernetes"])

    def clamp(v): return min(100.0, max(0.0, float(v)))

    ai_score         = clamp(ai_count * 12 + intent_signals * 5)
    maturity_score   = clamp((ai_count * 10 + cloud_tech * 8 + dev_tech * 5 + tech_count * 3))
    cloud_score      = clamp(cloud_tech * 25)
    automation_score = clamp(sum(1 for t in ai_stack if t in ["LangChain","LlamaIndex","Ray","Flowise"]) * 20)
    developer_score  = clamp(dev_tech * 15 + sum(1 for t in tech_stack if t in ["Docker","Kubernetes","Terraform"]) * 20)
    security_score   = clamp(sum(1 for t in tech_stack if t in ["Cloudflare","AWS","Azure","GCP"]) * 20)
    growth_score     = clamp(growth_signals * 15 + (company.get("employee_count") or 0) / 100)
    innovation_score = clamp(ai_count * 8 + intent_signals * 6 + dev_tech * 4)
    intent_score     = clamp(intent_signals * 20 + ai_count * 8)
    commerce_score   = clamp(sum(1 for t in tech_stack if t in ["Shopify","Stripe"]) * 40)
    tech_gap_score   = clamp(100 - maturity_score) if maturity_score > 0 else 50.0

    return {
        "ai_adoption_score":      ai_score,
        "ai_maturity_score":      maturity_score,
        "cloud_score":            cloud_score,
        "automation_score":       automation_score,
        "developer_score":        developer_score,
        "security_score":         security_score,
        "growth_score":           growth_score,
        "innovation_score":       innovation_score,
        "ai_buying_intent_score": intent_score,
        "commerce_score":         commerce_score,
        "tech_gap_score":         tech_gap_score,
    }


# ── Apollo Enrichment ─────────────────────────────────────────────────────────
async def enrich_apollo(session, domain: str) -> dict:
    if not APOLLO_KEY or not domain:
        return {}
    try:
        async with session.post(
            "https://api.apollo.io/v1/organizations/enrich",
            json={"domain": domain},
            headers={"Cache-Control": "no-cache", "Content-Type": "application/json", "x-api-key": APOLLO_KEY},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.ok:
                data = await r.json()
                org = data.get("organization") or {}
                out = {}
                if org.get("estimated_num_employees"):
                    out["employee_count"] = org["estimated_num_employees"]
                if org.get("annual_revenue_printed"):
                    out["revenue_range"] = org["annual_revenue_printed"]
                if org.get("founded_year"):
                    out["ats_hiring_signals"] = f"Founded: {org['founded_year']}"
                if org.get("logo_url"):
                    out["logo_url"] = org["logo_url"]
                if org.get("country"):
                    out["country"] = org["country"]
                if org.get("primary_domain"):
                    out["website"] = "https://" + org["primary_domain"]
                return out
    except Exception:
        pass
    return {}


# ── Web scraping ──────────────────────────────────────────────────────────────
async def fetch_page(session, url: str, timeout: int = 12) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; AgentSignalBot/5.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=timeout),
                               allow_redirects=True, max_redirects=5) as r:
            if r.status in (200, 203):
                ct = r.headers.get("Content-Type", "")
                if "text" in ct or "json" in ct:
                    return await r.text(errors="replace")
    except Exception:
        pass
    return ""


async def scan_company(session, company: dict) -> tuple:
    """Scansiona un'azienda e ritorna (id, payload, evidence)."""
    cid     = company.get("id", "")
    website = company.get("website", "") or ""
    name    = company.get("name", "?")

    if not website:
        return cid, None, []

    domain = normalize_domain(website)
    if not domain:
        return cid, None, []

    # Pagine da scansionare
    pages_to_scan = [website]
    for path in ["/about", "/technology", "/ai", "/careers", "/blog"]:
        pages_to_scan.append(website.rstrip("/") + path)

    all_html = ""
    for url in pages_to_scan[:3]:  # max 3 pagine per velocità
        html = await fetch_page(session, url)
        if html:
            all_html += " " + html

    if not all_html.strip():
        # Sito irraggiungibile — segna come scansionato senza dati
        return cid, {
            "last_scan_date": datetime.now(timezone.utc).isoformat(),
            "ai_adoption_score": 0.0,
        }, []

    text = extract_text(all_html)
    ai_stack, tech_stack, evidence = detect_tech(text, all_html, website)

    # Apollo enrichment
    apollo_data = await enrich_apollo(session, domain)

    scores = calculate_scores(ai_stack, tech_stack, text, company)

    tech_dna = {
        "ai":         ai_stack,
        "tech":       tech_stack,
        "domain":     domain,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
    }

    payload = {
        "last_scan_date":       datetime.now(timezone.utc).isoformat(),
        "ai_stack":             ai_stack if ai_stack else [],
        "tech_stack":           tech_stack if tech_stack else [],
        "technology_dna":       json.dumps(tech_dna),
        "ats_technology_adoption": scores["ai_adoption_score"],
        "ai_transformation_score": scores["ai_maturity_score"],
        **scores,
        **apollo_data,
    }

    return cid, payload, evidence


# ── Write to Base44 ───────────────────────────────────────────────────────────
async def write_to_base44(session, company_id: str, payload: dict) -> bool:
    """Aggiorna record via PUT (merge atomico lato server)."""
    if not company_id:
        return False
    try:
        async with session.put(
            f"{BASE}/Company/{company_id}",
            headers=HW,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            return r.ok
    except Exception as e:
        log.debug(f"write error {company_id}: {e}")
        return False


# ── Load batch (skip stabile) ─────────────────────────────────────────────────
_w_skip = -1

async def load_batch(session) -> list:
    """
    Carica batch con skip deterministico.
    sort=last_scan_date: NULL prima → mai scansionate vengono prime.
    Skip avanza sempre → non torna mai indietro.
    Fine DB → pausa 5min e ricomincia.
    """
    global _w_skip

    if _w_skip == -1:
        _w_skip = WORKER_ID * BATCH_SIZE
        log.info(f"={WORKER_ID}] Init skip={_w_skip}")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RESCAN_DAYS)).isoformat()

    for attempt in range(5):
        try:
            async with session.get(
                f"{BASE}/Company",
                headers=HR,
                params={
                    "limit": BATCH_SIZE,
                    "skip":  _w_skip,
                    "sort":  "last_scan_date",
                    "fields": "id,name,website,last_scan_date,employee_count,industry,description,country",
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if not r.ok:
                    log.warning(f"={WORKER_ID}] HTTP {r.status} load_batch")
                    await asyncio.sleep(10)
                    continue

                data = await r.json()
                if not isinstance(data, list):
                    data = []

                if not data:
                    log.info(f"={WORKER_ID}] Fine DB — pausa 5min e restart")
                    _w_skip = WORKER_ID * BATCH_SIZE
                    await asyncio.sleep(300)
                    return []

                # Fine DB reale (meno di BATCH_SIZE record)
                if len(data) < BATCH_SIZE:
                    log.info(f"={WORKER_ID}] Fine DB (got {len(data)}) — pausa 5min e restart")
                    _w_skip = WORKER_ID * BATCH_SIZE
                    await asyncio.sleep(300)
                    return data  # processa comunque l'ultimo batch parziale

                pending = [c for c in data
                           if not c.get("last_scan_date") or c["last_scan_date"] < cutoff]

                _w_skip += BATCH_SIZE  # avanza sempre
                log.info(f"={WORKER_ID}] skip→{_w_skip} pending={len(pending)}/{len(data)}")

                return pending if pending else []

        except Exception as e:
            log.warning(f"={WORKER_ID}] load_batch attempt {attempt+1}: {e}")
            await asyncio.sleep(5)

    return []


# ── Main worker loop ──────────────────────────────────────────────────────────
async def run_worker():
    total_scanned = total_ai = total_written = 0
    start = time.time()

    connector = aiohttp.TCPConnector(limit=THREADS, ttl_dns_cache=300, limit_per_host=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            batch = await load_batch(session)
            if not batch:
                await asyncio.sleep(120)  # 2 min — non 10 min (keep-alive per Railway)
                continue

            log.info(f"={WORKER_ID}] Batch: {len(batch)} aziende")
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
                        log.debug(f"process error {company.get('name','?')}: {e}")
                    finally:
                        done += 1
                        if done % 50 == 0:
                            elapsed = time.time() - batch_start
                            rate = int(done / max(elapsed / 60, 0.01))
                            pct_ai = ai_hits / max(done, 1) * 100
                            log.info(f"={WORKER_ID}]  [{done}/{len(batch)}] {rate}/min | written:{ok} | AI:{ai_hits} ({pct_ai:.1f}%)")

            await asyncio.gather(*[process(c) for c in batch])

            total_scanned += done
            total_ai      += ai_hits
            total_written += ok
            uptime_h       = (time.time() - start) / 3600

            log.info(
                f"={WORKER_ID}] Batch done: {done} scan | {ok} written | {ai_hits} AI | "
                f"Tot: {total_scanned} | AI%: {total_ai/max(total_scanned,1)*100:.1f}% | "
                f"Up: {uptime_h:.2f}h"
            )
            await asyncio.sleep(1)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    # Avvia healthcheck e worker in parallelo
    await asyncio.gather(
        healthcheck_server(),
        run_worker(),
    )

if __name__ == "__main__":
    asyncio.run(main())

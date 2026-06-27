#!/usr/bin/env python3
"""
AgentSignal Mega Domain Importer
=================================
Scarica Majestic Million + Cisco Umbrella Top-1M,
deduplicano i domini, filtrano quelli già nel DB,
e inseriscono in bulk su Base44.

Throughput stimato: ~500 inserimenti/min → 1M in ~33h
"""

import asyncio
import aiohttp
import csv
import gzip
import io
import json
import logging
import os
import re
import time
import zipfile
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

TOKEN  = os.environ.get("BASE44_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID = os.environ.get("APP_ID",       "6a3a284ab0b87dfa27558bb6")
BASE   = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HR     = {"api-key": TOKEN}
HW     = {"api-key": TOKEN, "Content-Type": "application/json"}

CONCURRENT_WRITES = 20   # scritture parallele su Base44
INSERT_BATCH      = 50   # record per batch insert
SKIP_EXISTING     = True  # salta domini già nel DB

SOURCES = [
    {
        "name": "Majestic Million",
        "url":  "https://downloads.majestic.com/majestic_million.csv",
        "type": "csv",
        "domain_col": "Domain",   # colonna con il dominio
        "rank_col":   "GlobalRank",
    },
    {
        "name": "Cisco Umbrella",
        "url":  "https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip",
        "type": "csv_zip",
        "domain_col": 1,    # colonna 1 (0-based: rank, domain)
        "rank_col":   0,
    },
]

# Domini da escludere (CDN, social, motori di ricerca, ecc.)
EXCLUDE_DOMAINS = {
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com",
    "x.com","tiktok.com","linkedin.com","reddit.com","wikipedia.org",
    "amazon.com","apple.com","microsoft.com","netflix.com","spotify.com",
    "cloudflare.com","amazonaws.com","doubleclick.net","googlesyndication.com",
    "gstatic.com","googletagmanager.com","googleapis.com","akamai.net",
    "akamaized.net","fastly.net","cloudfront.net","wp.com","wordpress.com",
    "blogspot.com","tumblr.com","medium.com","substack.com","ghost.io",
}

# TLD da escludere (gov, edu di basso valore commerciale)
EXCLUDE_TLD = {".gov", ".mil", ".edu"}


def normalize_domain(d: str) -> str:
    d = d.lower().strip()
    if d.startswith("http"):
        d = urlparse(d).netloc
    d = d.replace("www.", "")
    return d.strip()


def domain_to_name(domain: str) -> str:
    """Estrae un nome leggibile dal dominio."""
    name = domain.split(".")[0]
    name = re.sub(r"[-_]", " ", name)
    return name.title()


def should_skip(domain: str) -> bool:
    if not domain or len(domain) < 4:
        return True
    if domain in EXCLUDE_DOMAINS:
        return True
    for tld in EXCLUDE_TLD:
        if domain.endswith(tld):
            return True
    # Escludi IP e domini con troppi punti (CDN)
    if re.match(r"^\d+\.\d+\.\d+", domain):
        return True
    if domain.count(".") > 3:
        return True
    return False


async def download_source(session, source: dict) -> list[dict]:
    """Scarica e parsa una sorgente di domini."""
    name = source["name"]
    url  = source["url"]
    log.info(f"[{name}] Download: {url}")

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
            if not r.ok:
                log.error(f"[{name}] HTTP {r.status}")
                return []
            raw = await r.read()
    except Exception as e:
        log.error(f"[{name}] Download error: {e}")
        return []

    log.info(f"[{name}] Downloaded {len(raw):,} bytes — parsing...")

    records = []
    try:
        if source["type"] == "csv_zip":
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                fname = z.namelist()[0]
                with z.open(fname) as f:
                    reader = csv.reader(io.TextIOWrapper(f, "utf-8", errors="replace"))
                    for row in reader:
                        if len(row) < 2:
                            continue
                        try:
                            rank   = int(row[source["rank_col"]])
                            domain = normalize_domain(row[source["domain_col"]])
                        except (ValueError, IndexError):
                            continue
                        if not should_skip(domain):
                            records.append({"domain": domain, "rank": rank, "source": name})

        elif source["type"] == "csv":
            text = raw.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                try:
                    domain = normalize_domain(row[source["domain_col"]])
                    rank   = int(row.get(source["rank_col"], 0))
                except (KeyError, ValueError):
                    continue
                if not should_skip(domain):
                    records.append({"domain": domain, "rank": rank, "source": name})

    except Exception as e:
        log.error(f"[{name}] Parse error: {e}")

    log.info(f"[{name}] Parsed {len(records):,} valid domains")
    return records


async def get_existing_domains(session) -> set:
    """Legge tutti i domini già nel DB (campo website normalizzato)."""
    log.info("Caricamento domini esistenti dal DB...")
    existing = set()
    skip = 0
    while True:
        try:
            async with session.get(f"{BASE}/Company",
                headers=HR,
                params={"limit": 500, "skip": skip, "fields": "id,website,name"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if not r.ok:
                    log.warning(f"DB load HTTP {r.status}")
                    await asyncio.sleep(5)
                    continue
                data = await r.json()
                if not isinstance(data, list) or not data:
                    break
                for c in data:
                    w = c.get("website") or ""
                    existing.add(normalize_domain(w))
                    existing.add((c.get("name") or "").lower().strip())
                if len(data) < 500:
                    break
                skip += 500
                if skip % 5000 == 0:
                    log.info(f"  Caricati {skip:,} record esistenti...")
                await asyncio.sleep(0.05)
        except Exception as e:
            log.warning(f"DB load error: {e}")
            await asyncio.sleep(5)

    log.info(f"Domini esistenti: {len(existing):,}")
    return existing


async def insert_company(session, record: dict) -> bool:
    """Inserisce una singola azienda (POST se nuova)."""
    domain = record["domain"]
    payload = {
        "name":    domain_to_name(domain),
        "website": f"https://{domain}",
        "source":  record.get("source", "bulk_import"),
        "global_rank": record.get("rank"),
        # Campi vuoti da popolare dallo scanner
        "ai_stack":   [],
        "tech_stack": [],
    }
    try:
        async with session.post(f"{BASE}/Company",
            headers=HW,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            return r.ok
    except Exception:
        return False


async def run_importer():
    """Pipeline principale: scarica → deduplica → inserisce."""
    connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        # 1. Scarica tutte le sorgenti in parallelo
        log.info("=== FASE 1: Download sorgenti ===")
        results = await asyncio.gather(*[download_source(session, s) for s in SOURCES])
        all_records = [r for batch in results for r in batch]
        log.info(f"Totale domini scaricati: {len(all_records):,}")

        # 2. Deduplica per dominio (mantieni rank più basso = più importante)
        log.info("=== FASE 2: Deduplicazione ===")
        seen = {}
        for r in all_records:
            d = r["domain"]
            if d not in seen or r["rank"] < seen[d]["rank"]:
                seen[d] = r
        unique = list(seen.values())
        # Ordina per rank (i più importanti prima)
        unique.sort(key=lambda x: x["rank"])
        log.info(f"Domini unici dopo dedup: {len(unique):,}")

        # 3. Filtra quelli già nel DB
        if SKIP_EXISTING:
            log.info("=== FASE 3: Confronto con DB ===")
            existing = await get_existing_domains(session)
            to_insert = [r for r in unique if r["domain"] not in existing]
            log.info(f"Nuovi da inserire: {len(to_insert):,} (già presenti: {len(unique)-len(to_insert):,})")
        else:
            to_insert = unique

        # 4. Inserimento in bulk con parallelismo controllato
        log.info(f"=== FASE 4: Inserimento {len(to_insert):,} aziende ===")
        sem = asyncio.Semaphore(CONCURRENT_WRITES)
        inserted = 0
        errors   = 0
        start    = time.time()

        async def insert_one(rec):
            nonlocal inserted, errors
            async with sem:
                ok = await insert_company(session, rec)
                if ok:
                    inserted += 1
                else:
                    errors += 1
                if (inserted + errors) % 500 == 0:
                    elapsed = time.time() - start
                    rate = inserted / max(elapsed / 60, 0.01)
                    pct  = inserted / max(len(to_insert), 1) * 100
                    eta_min = (len(to_insert) - inserted) / max(rate, 1)
                    log.info(
                        f"  Inseriti: {inserted:,}/{len(to_insert):,} ({pct:.1f}%) | "
                        f"{rate:.0f}/min | ETA: {eta_min:.0f}min | Err: {errors}"
                    )

        # Processa in chunk per evitare OOM
        CHUNK = 5000
        for i in range(0, len(to_insert), CHUNK):
            chunk = to_insert[i:i+CHUNK]
            await asyncio.gather(*[insert_one(r) for r in chunk])
            log.info(f"  Chunk {i//CHUNK+1}/{(len(to_insert)+CHUNK-1)//CHUNK} completato")
            await asyncio.sleep(1)  # pausa tra chunk

        elapsed = time.time() - start
        log.info(f"\n=== COMPLETATO ===")
        log.info(f"Inseriti: {inserted:,} | Errori: {errors:,} | Tempo: {elapsed/60:.1f}min")
        log.info(f"I worker Railway inizieranno a scansionare i nuovi record automaticamente.")


if __name__ == "__main__":
    asyncio.run(run_importer())

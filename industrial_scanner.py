#!/usr/bin/env python3
"""
Industrial Opportunity Intelligence — Scanner Engine v1.0
Scansiona siti di aziende manifatturiere e rileva segnali per:
  - Robotica (cobot, robot industriale, palletizing, welding...)
  - CNC / Machine Tending
  - AMR / AGV / Warehouse Automation
  - MES / SCADA / PLC
  - Computer Vision / Quality Inspection
  - Buying Intent (nuova fabbrica, espansione, assunzioni)

Output → Base44 entities: IndustrialCompany, IndustrialSignal, IndustrialOpportunity
"""

import asyncio
import aiohttp
import asyncpg
import os
import json
import re
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [IND] %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
B44_TOKEN = (os.environ.get("B44_SERVICE_TOKEN") or
             os.environ.get("BASE44_SERVICE_TOKEN") or "")
APP_ID    = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
B44_BASE  = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW        = {"api-key": B44_TOKEN, "Content-Type": "application/json"}

WORKER_ID = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
BATCH_SIZE  = int(os.environ.get("BATCH_SIZE", "50"))
REQUEST_TIMEOUT = 15
DELAY_BETWEEN_DOMAINS = 1.5

USER_AGENT = "IndustrialOpportunityBot/1.0 (+https://agentsignal.io/bot)"

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# ──────────────────────────────────────────────
# SIGNAL PATTERNS — solo evidenza testuale reale
# Ogni pattern ha: keywords, category, signal_type, confidence, deal_range
# ──────────────────────────────────────────────

SIGNAL_DB = [
    # ── ROBOTICS ──
    {"category": "robotics", "type": "palletizing",
     "patterns": [r"\bpalletiz\w+\b", r"\bdepalletiz\w+\b", r"\bpallet\s+stacking\b"],
     "confidence": 85, "deal_min": 60000, "deal_max": 150000},
    {"category": "robotics", "type": "pick_and_place",
     "patterns": [r"\bpick[\s\-]and[\s\-]place\b", r"\bpick\s+&\s+place\b"],
     "confidence": 80, "deal_min": 40000, "deal_max": 120000},
    {"category": "robotics", "type": "welding_robot",
     "patterns": [r"\brobotic\s+welding\b", r"\bwelding\s+robot\b", r"\barc\s+welding\b", r"\bspot\s+welding\b"],
     "confidence": 90, "deal_min": 80000, "deal_max": 200000},
    {"category": "robotics", "type": "assembly_robot",
     "patterns": [r"\brobotic\s+assembl\w+\b", r"\bautomat\w+\s+assembl\w+\b"],
     "confidence": 75, "deal_min": 60000, "deal_max": 180000},
    {"category": "robotics", "type": "collaborative_robot",
     "patterns": [r"\bcobot\b", r"\bcollaborative\s+robot\b", r"\bhuman[\s\-]robot\s+collaboration\b"],
     "confidence": 90, "deal_min": 30000, "deal_max": 90000},
    {"category": "robotics", "type": "end_of_line",
     "patterns": [r"\bend[\s\-]of[\s\-]line\b", r"\bcase\s+packing\b", r"\bshrink\s+wrapping\b"],
     "confidence": 75, "deal_min": 50000, "deal_max": 130000},
    {"category": "robotics", "type": "heavy_lifting",
     "patterns": [r"\bheavy\s+lifting\b", r"\bmanual\s+lifting\b", r"\brepetitive\s+lifting\b"],
     "confidence": 70, "deal_min": 40000, "deal_max": 100000},

    # ── CNC / MACHINE TENDING ──
    {"category": "cnc_machine_tending", "type": "cnc_operator",
     "patterns": [r"\bcnc\s+operator\b", r"\bcnc\s+machinist\b", r"\bmachine\s+operator\b"],
     "confidence": 80, "deal_min": 50000, "deal_max": 150000},
    {"category": "cnc_machine_tending", "type": "cnc_machine",
     "patterns": [r"\bcnc\s+machin\w+\b", r"\bmazak\b", r"\bdmg\s+mori\b", r"\bhaas\s+cnc\b",
                  r"\bokuma\b", r"\bfanuc\s+cnc\b", r"\bturning\s+center\b", r"\bmachining\s+center\b"],
     "confidence": 85, "deal_min": 60000, "deal_max": 180000},
    {"category": "cnc_machine_tending", "type": "machine_tending",
     "patterns": [r"\bmachine\s+tending\b", r"\bloading\s+unloading\b", r"\bload\s+unload\b",
                  r"\bautomatic\s+loading\b"],
     "confidence": 90, "deal_min": 70000, "deal_max": 180000},
    {"category": "cnc_machine_tending", "type": "shift_production",
     "patterns": [r"\b3[\s\-]shift\b", r"\bthree[\s\-]shift\b", r"\bnight\s+shift\b",
                  r"\b24[\s\/]7\s+production\b", r"\bcontinuous\s+production\b"],
     "confidence": 70, "deal_min": 40000, "deal_max": 120000},

    # ── AMR / AGV ──
    {"category": "amr_agv", "type": "warehouse_logistics",
     "patterns": [r"\bwarehousing\b", r"\bwarehouse\s+operations\b", r"\bintralogistics\b",
                  r"\binternal\s+logistics\b", r"\bmaterial\s+handling\b"],
     "confidence": 70, "deal_min": 80000, "deal_max": 250000},
    {"category": "amr_agv", "type": "forklift_operations",
     "patterns": [r"\bforklift\b", r"\bfork\s+lift\b", r"\bforklifts\b"],
     "confidence": 75, "deal_min": 100000, "deal_max": 300000},
    {"category": "amr_agv", "type": "agv_amr_mention",
     "patterns": [r"\b\bAGV\b", r"\b\bAMR\b", r"\bautonomous\s+mobile\s+robot\b",
                  r"\bautonome\s+fahrzeuge\b", r"\bguided\s+vehicle\b"],
     "confidence": 95, "deal_min": 100000, "deal_max": 400000},
    {"category": "amr_agv", "type": "warehouse_expansion",
     "patterns": [r"\bnew\s+warehouse\b", r"\bwarehouse\s+expansion\b", r"\bnew\s+distribution\s+center\b",
                  r"\blogistics\s+center\b"],
     "confidence": 80, "deal_min": 150000, "deal_max": 500000},
    {"category": "amr_agv", "type": "picking_operations",
     "patterns": [r"\border\s+picking\b", r"\bpick\s+path\b", r"\bgoods[\s\-]to[\s\-]person\b",
                  r"\bpicking\s+efficiency\b"],
     "confidence": 75, "deal_min": 80000, "deal_max": 250000},

    # ── MES / SCADA / PLC ──
    {"category": "mes_scada", "type": "mes_mention",
     "patterns": [r"\bMES\b", r"\bmanufacturing\s+execution\s+system\b"],
     "confidence": 90, "deal_min": 50000, "deal_max": 200000},
    {"category": "mes_scada", "type": "scada_mention",
     "patterns": [r"\bSCADA\b", r"\bsupervisory\s+control\b"],
     "confidence": 90, "deal_min": 40000, "deal_max": 180000},
    {"category": "mes_scada", "type": "plc_systems",
     "patterns": [r"\bPLC\b", r"\bprogrammable\s+logic\b", r"\bsiemens\s+s7\b",
                  r"\ballen[\s\-]bradley\b", r"\brockwell\s+automation\b", r"\bschneider\s+electric\b"],
     "confidence": 85, "deal_min": 30000, "deal_max": 150000},
    {"category": "mes_scada", "type": "oee_monitoring",
     "patterns": [r"\bOEE\b", r"\boverall\s+equipment\s+effectiveness\b", r"\bproduction\s+monitoring\b",
                  r"\bdowntime\s+monitoring\b", r"\bshop\s+floor\b"],
     "confidence": 85, "deal_min": 30000, "deal_max": 120000},
    {"category": "mes_scada", "type": "industry40",
     "patterns": [r"\bindustry\s+4\.0\b", r"\bindustrie\s+4\.0\b", r"\bsmart\s+factory\b",
                  r"\bdigital\s+twin\b", r"\biiot\b", r"\bindustrial\s+iot\b", r"\bopc[\s\-]ua\b"],
     "confidence": 80, "deal_min": 50000, "deal_max": 250000},
    {"category": "mes_scada", "type": "traceability",
     "patterns": [r"\btraceability\b", r"\bproduct\s+tracing\b", r"\bbatch\s+tracking\b",
                  r"\bserial\s+number\s+tracking\b"],
     "confidence": 80, "deal_min": 30000, "deal_max": 100000},

    # ── MACHINE VISION ──
    {"category": "machine_vision", "type": "quality_inspection",
     "patterns": [r"\bquality\s+inspection\b", r"\bvisual\s+inspection\b", r"\bdefect\s+detection\b",
                  r"\binspection\s+line\b", r"\bquality\s+control\s+system\b"],
     "confidence": 85, "deal_min": 30000, "deal_max": 150000},
    {"category": "machine_vision", "type": "computer_vision",
     "patterns": [r"\bcomputer\s+vision\b", r"\bmachine\s+vision\b", r"\bvision\s+system\b",
                  r"\bcamera\s+inspection\b", r"\bimage\s+processing\s+inspection\b"],
     "confidence": 90, "deal_min": 40000, "deal_max": 200000},
    {"category": "machine_vision", "type": "nonconformity",
     "patterns": [r"\bnon[\s\-]conformit\w+\b", r"\bdefect\s+rate\b", r"\bscrap\s+rate\b",
                  r"\bquality\s+reject\w*\b", r"\bzero\s+defect\b"],
     "confidence": 75, "deal_min": 25000, "deal_max": 100000},

    # ── GROWTH / BUYING INTENT ──
    {"category": "growth_buying_intent", "type": "new_factory",
     "patterns": [r"\bnew\s+factory\b", r"\bnew\s+plant\b", r"\bnew\s+facility\b",
                  r"\bgreen[\s\-]field\b", r"\bnuovo\s+stabilimento\b"],
     "confidence": 90, "deal_min": 200000, "deal_max": 1000000},
    {"category": "growth_buying_intent", "type": "production_expansion",
     "patterns": [r"\bproduction\s+expansion\b", r"\bexpanding\s+production\b",
                  r"\bincreasing\s+capacity\b", r"\bcapacity\s+expansion\b",
                  r"\bnew\s+production\s+line\b", r"\bespansione\s+produttiva\b"],
     "confidence": 85, "deal_min": 100000, "deal_max": 500000},
    {"category": "growth_buying_intent", "type": "automation_project",
     "patterns": [r"\bautomation\s+project\b", r"\bautomation\s+initiative\b",
                  r"\bautomation\s+investment\b", r"\bdigital\s+transformation\b",
                  r"\boperational\s+efficiency\b", r"\blabor\s+shortage\b"],
     "confidence": 80, "deal_min": 80000, "deal_max": 400000},
    {"category": "growth_buying_intent", "type": "investment_announcement",
     "patterns": [r"\bmillion\s+investment\b", r"\binvestment\s+plan\b",
                  r"\bfunding\s+round\b", r"\bseries\s+[abc]\b",
                  r"\bcapital\s+expenditure\b", r"\bcapex\b"],
     "confidence": 75, "deal_min": 100000, "deal_max": 500000},

    # ── HIRING SIGNALS ──
    {"category": "hiring", "type": "automation_engineer_hiring",
     "patterns": [r"\bautomation\s+engineer\b", r"\brobotic\w*\s+engineer\b",
                  r"\bprocess\s+automation\s+engineer\b"],
     "confidence": 90, "deal_min": 60000, "deal_max": 200000},
    {"category": "hiring", "type": "plc_programmer_hiring",
     "patterns": [r"\bplc\s+programm\w+\b", r"\bsiemens\s+programm\w+\b",
                  r"\bscada\s+engineer\b", r"\bcontrol\s+systems\s+engineer\b"],
     "confidence": 85, "deal_min": 40000, "deal_max": 150000},
    {"category": "hiring", "type": "maintenance_technician_hiring",
     "patterns": [r"\bmaintenance\s+technician\b", r"\bindustrial\s+electrician\b",
                  r"\bpredictive\s+maintenance\b", r"\bpreventive\s+maintenance\b"],
     "confidence": 80, "deal_min": 30000, "deal_max": 100000},
    {"category": "hiring", "type": "manufacturing_engineer_hiring",
     "patterns": [r"\bmanufacturing\s+engineer\b", r"\bproduction\s+engineer\b",
                  r"\bindustrial\s+engineer\b", r"\blean\s+engineer\b",
                  r"\bcontinuous\s+improvement\s+engineer\b"],
     "confidence": 75, "deal_min": 50000, "deal_max": 180000},
    {"category": "hiring", "type": "mes_specialist_hiring",
     "patterns": [r"\bmes\s+specialist\b", r"\bmes\s+engineer\b",
                  r"\bscada\s+specialist\b", r"\bmanufacturing\s+it\b"],
     "confidence": 90, "deal_min": 50000, "deal_max": 200000},
]

# Pagine da scansionare per ogni azienda (path relativi)
PAGES_TO_SCAN = [
    "/", "/about", "/about-us", "/chi-siamo",
    "/products", "/prodotti", "/services", "/servizi",
    "/manufacturing", "/produzione", "/production",
    "/industries", "/settori", "/solutions",
    "/warehouse", "/magazzino", "/logistics",
    "/quality", "/qualita", "/quality-control",
    "/careers", "/lavora-con-noi", "/jobs", "/job-openings",
    "/news", "/notizie", "/blog", "/press",
    "/technology", "/tecnologia", "/innovation",
]

# ──────────────────────────────────────────────
# SCORING ENGINE
# ──────────────────────────────────────────────

OPPORTUNITY_MAP = {
    "collaborative_robot":      {"categories": ["robotics", "cnc_machine_tending"], "base_score": 70},
    "industrial_robot":         {"categories": ["robotics"], "base_score": 75},
    "amr_agv":                  {"categories": ["amr_agv"], "base_score": 70},
    "machine_tending":          {"categories": ["cnc_machine_tending"], "base_score": 80},
    "palletizing":              {"categories": ["robotics"], "types": ["palletizing"], "base_score": 90},
    "packaging_automation":     {"categories": ["robotics"], "types": ["end_of_line"], "base_score": 75},
    "mes_scada":                {"categories": ["mes_scada"], "base_score": 75},
    "plc_upgrade":              {"categories": ["mes_scada"], "types": ["plc_systems"], "base_score": 70},
    "computer_vision":          {"categories": ["machine_vision"], "base_score": 80},
    "predictive_maintenance":   {"categories": ["hiring"], "types": ["maintenance_technician_hiring"], "base_score": 65},
    "warehouse_automation":     {"categories": ["amr_agv"], "base_score": 75},
    "industrial_ai":            {"categories": ["mes_scada", "machine_vision"], "base_score": 70},
}

DEAL_VALUES = {
    "collaborative_robot":      (30000,  90000),
    "industrial_robot":         (80000,  250000),
    "amr_agv":                  (100000, 400000),
    "machine_tending":          (60000,  180000),
    "palletizing":              (60000,  150000),
    "packaging_automation":     (50000,  130000),
    "mes_scada":                (50000,  200000),
    "plc_upgrade":              (20000,  80000),
    "computer_vision":          (30000,  150000),
    "predictive_maintenance":   (20000,  80000),
    "warehouse_automation":     (100000, 400000),
    "industrial_ai":            (50000,  200000),
}

SOLUTION_NAMES = {
    "collaborative_robot":      "Collaborative Robot Cell (Cobot)",
    "industrial_robot":         "Industrial Robot System",
    "amr_agv":                  "AMR / AGV Fleet",
    "machine_tending":          "CNC Machine Tending Robot",
    "palletizing":              "End-of-Line Palletizing Robot",
    "packaging_automation":     "Packaging Automation System",
    "mes_scada":                "MES / SCADA Platform",
    "plc_upgrade":              "PLC Retrofit & Upgrade",
    "computer_vision":          "Computer Vision Inspection System",
    "predictive_maintenance":   "Predictive Maintenance Platform",
    "warehouse_automation":     "Warehouse Automation System",
    "industrial_ai":            "Industrial AI Platform",
}


def compute_scores(signals: list) -> dict:
    """Calcola i 6 score proprietari dai segnali rilevati."""
    cat_counts = {}
    cat_confidence = {}
    for s in signals:
        cat = s["signal_category"]
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        cat_confidence[cat] = max(cat_confidence.get(cat, 0), s["confidence_score"])

    def score(cats, weight=1.0):
        total = sum(cat_counts.get(c, 0) * cat_confidence.get(c, 0) for c in cats)
        base  = sum(cat_confidence.get(c, 0) for c in cats if c in cat_counts)
        if not base: return 0
        raw = min(100, int((total / max(1, len(cats) * 100)) * 100 * weight))
        return min(100, max(0, raw + (10 if sum(cat_counts.get(c,0) for c in cats) > 2 else 0)))

    robotics    = score(["robotics", "cnc_machine_tending"], 1.2)
    amr         = score(["amr_agv"], 1.3)
    mes         = score(["mes_scada"], 1.2)
    vision      = score(["machine_vision"], 1.2)
    intent      = score(["growth_buying_intent", "hiring"], 1.4)
    readiness   = min(100, int((robotics + amr + mes + intent) / 4))

    return {
        "automation_readiness_score":      readiness,
        "robotics_opportunity_score":      robotics,
        "amr_agv_opportunity_score":       amr,
        "mes_opportunity_score":           mes,
        "machine_vision_opportunity_score": vision,
        "buying_intent_score":             intent,
    }


def generate_opportunities(company_id: str, company_name: str, domain: str,
                            signals: list, scores: dict) -> list:
    """Genera le opportunity raccomandate con stima deal value."""
    opps = []
    cat_signals = {}
    for s in signals:
        cat_signals.setdefault(s["signal_category"], []).append(s)

    for opp_type, cfg in OPPORTUNITY_MAP.items():
        cats = cfg["categories"]
        relevant = [s for s in signals if s["signal_category"] in cats]
        if not relevant:
            continue

        # Score specifico per questa opportunity
        opp_score = min(100, int(
            sum(s["confidence_score"] for s in relevant) / max(1, len(relevant)) +
            len(relevant) * 5
        ))
        if opp_score < 40:
            continue

        intent_boost = scores.get("buying_intent_score", 0) // 10
        opp_score = min(100, opp_score + intent_boost)

        dmin, dmax = DEAL_VALUES.get(opp_type, (50000, 200000))
        # Scala il deal value in base allo score
        factor = opp_score / 100
        dmin_adj = int(dmin * (0.7 + 0.3 * factor))
        dmax_adj = int(dmax * (0.7 + 0.3 * factor))

        top_signals = [s["signal_type"] for s in sorted(relevant, key=lambda x: -x["confidence_score"])[:3]]
        reason = _build_reason(opp_type, relevant, scores)

        opps.append({
            "company_id":           company_id,
            "company_name":         company_name,
            "company_domain":       domain,
            "opportunity_type":     opp_type,
            "recommended_solution": SOLUTION_NAMES[opp_type],
            "opportunity_score":    opp_score,
            "buying_intent_score":  scores.get("buying_intent_score", 0),
            "estimated_deal_value_min": dmin_adj,
            "estimated_deal_value_max": dmax_adj,
            "reason_summary":       reason,
            "signals_count":        len(relevant),
            "top_signals":          top_signals,
        })

    # Ordina per score decrescente
    opps.sort(key=lambda x: -x["opportunity_score"])
    return opps


def _build_reason(opp_type: str, signals: list, scores: dict) -> str:
    signal_descs = list({s["signal_type"].replace("_", " ") for s in signals[:4]})
    intent = scores.get("buying_intent_score", 0)
    parts = []
    if signal_descs:
        parts.append(f"Detected signals: {', '.join(signal_descs)}")
    if intent >= 70:
        parts.append("strong buying intent indicators")
    elif intent >= 40:
        parts.append("moderate buying intent")
    solution = SOLUTION_NAMES.get(opp_type, opp_type.replace("_", " ").title())
    parts.append(f"→ recommended: {solution}")
    return ". ".join(parts).capitalize() + "."


# ──────────────────────────────────────────────
# SCRAPER
# ──────────────────────────────────────────────

def clean_text(html: str) -> str:
    """Estrae testo pulito dall'HTML."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "head"]):
            tag.decompose()
        return " ".join(soup.get_text(separator=" ").split())[:50000]
    except Exception:
        return html[:20000]


def detect_signals(text: str, url: str, company_id: str,
                   company_name: str, domain: str) -> list:
    """Applica tutti i pattern al testo e ritorna i segnali trovati."""
    text_lower = text.lower()
    found = []
    seen_types = set()

    for pat_cfg in SIGNAL_DB:
        signal_type = pat_cfg["type"]
        if (pat_cfg["category"], signal_type) in seen_types:
            continue
        for pattern in pat_cfg["patterns"]:
            m = re.search(pattern, text_lower)
            if m:
                # Estrai contesto: 150 char attorno al match
                start = max(0, m.start() - 80)
                end   = min(len(text), m.end() + 80)
                evidence = text[start:end].strip()

                found.append({
                    "company_id":       company_id,
                    "company_name":     company_name,
                    "company_domain":   domain,
                    "signal_category":  pat_cfg["category"],
                    "signal_type":      signal_type,
                    "source_url":       url,
                    "evidence_text":    evidence[:400],
                    "confidence_score": pat_cfg["confidence"],
                    "detected_at":      datetime.now(timezone.utc).isoformat(),
                })
                seen_types.add((pat_cfg["category"], signal_type))
                break  # un match per pattern è sufficiente

    return found


async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    """Scarica una pagina con timeout e retry."""
    for attempt in range(2):
        try:
            async with session.get(
                url, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True, ssl=False
            ) as resp:
                if resp.status == 200:
                    ct = resp.headers.get("content-type", "")
                    if "text" in ct or "html" in ct:
                        return await resp.text(errors="replace")
                return ""
        except asyncio.TimeoutError:
            log.debug(f"Timeout: {url}")
            return ""
        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(1)
            else:
                log.debug(f"ERR {url}: {e}")
                return ""
    return ""


async def scan_domain(session: aiohttp.ClientSession, company: dict) -> dict:
    """Scansiona tutte le pagine di un'azienda e ritorna i segnali."""
    domain  = company["domain"]
    base    = company.get("website_url") or f"https://{domain}"
    cid     = company["id"]
    cname   = company.get("name", domain)

    all_signals = []
    pages_scanned = 0

    for path in PAGES_TO_SCAN:
        url = urljoin(base.rstrip("/") + "/", path.lstrip("/"))
        html = await fetch_page(session, url)
        if not html:
            # Prova HTTP se HTTPS fallisce
            if url.startswith("https://"):
                url2 = url.replace("https://", "http://", 1)
                html = await fetch_page(session, url2)
        if not html:
            continue

        pages_scanned += 1
        text = clean_text(html)
        sigs = detect_signals(text, url, cid, cname, domain)
        all_signals.extend(sigs)
        await asyncio.sleep(0.3)  # gentile con il server

    # Dedup: tieni 1 segnale per (category, type) — quello con confidence più alta
    deduped = {}
    for s in all_signals:
        key = (s["signal_category"], s["signal_type"])
        if key not in deduped or s["confidence_score"] > deduped[key]["confidence_score"]:
            deduped[key] = s
    signals = list(deduped.values())

    return {
        "company":       company,
        "signals":       signals,
        "pages_scanned": pages_scanned,
    }


# ──────────────────────────────────────────────
# BASE44 API
# ──────────────────────────────────────────────

async def b44_post(session: aiohttp.ClientSession, entity: str, data: dict) -> str:
    """Crea un record su Base44, ritorna l'ID."""
    url = f"{B44_BASE}/{entity}"
    try:
        async with session.post(url, headers=HW, json=data,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status in (200, 201):
                r = await resp.json(content_type=None)
                return r.get("id", "")
            else:
                t = await resp.text()
                log.warning(f"POST {entity} {resp.status}: {t[:100]}")
                return ""
    except Exception as e:
        log.warning(f"POST {entity} ERR: {e}")
        return ""


async def b44_put(session: aiohttp.ClientSession, entity: str, eid: str, data: dict) -> bool:
    """Aggiorna un record su Base44."""
    url = f"{B44_BASE}/{entity}/{eid}"
    try:
        async with session.put(url, headers=HW, json=data,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return resp.status in (200, 201)
    except Exception as e:
        log.warning(f"PUT {entity} ERR: {e}")
        return False


async def b44_get_or_create_company(session: aiohttp.ClientSession,
                                    domain: str, name: str, website: str = "",
                                    country: str = "", industry: str = "") -> str:
    """Ritorna l'ID di IndustrialCompany per questo dominio, creandola se non esiste."""
    # Cerca per dominio
    url = f"{B44_BASE}/IndustrialCompany?domain={domain}&limit=1&fields=id"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                if data:
                    return data[0]["id"]
    except Exception:
        pass

    # Crea
    payload = {
        "name":        name,
        "domain":      domain,
        "website_url": website or f"https://{domain}",
        "country":     country,
        "industry":    industry,
        "scan_status": "pending",
        "source":      "industrial_scanner",
    }
    return await b44_post(session, "IndustrialCompany", payload)


async def save_results(session: aiohttp.ClientSession, result: dict):
    """Salva segnali, score e opportunity su Base44."""
    company  = result["company"]
    signals  = result["signals"]
    pages    = result["pages_scanned"]
    domain   = company["domain"]
    cname    = company.get("name", domain)

    # Ottieni/crea IndustrialCompany
    cid = await b44_get_or_create_company(
        session, domain, cname,
        company.get("website_url", ""),
        company.get("country", ""),
        company.get("industry", ""),
    )
    if not cid:
        log.warning(f"No company ID for {domain}")
        return

    # Salva segnali (bulk se molti)
    for sig in signals:
        sig["company_id"] = cid
        await b44_post(session, "IndustrialSignal", sig)
        await asyncio.sleep(0.1)

    # Calcola score
    scores = compute_scores(signals)
    top_score_key = max(scores, key=scores.get) if scores else "automation_readiness_score"

    # Aggiorna IndustrialCompany con i score
    score_update = {
        **scores,
        "last_scan_date": datetime.now(timezone.utc).isoformat(),
        "scan_status":    "done",
    }
    await b44_put(session, "IndustrialCompany", cid, score_update)

    # Genera e salva opportunity
    opps = generate_opportunities(cid, cname, domain, signals, scores)
    for opp in opps[:5]:  # max 5 opportunity per azienda
        await b44_post(session, "IndustrialOpportunity", opp)
        await asyncio.sleep(0.1)

    # Salva ScanJob
    await b44_post(session, "IndustrialScanJob", {
        "company_id":              cid,
        "company_domain":          domain,
        "status":                  "done",
        "started_at":              datetime.now(timezone.utc).isoformat(),
        "completed_at":            datetime.now(timezone.utc).isoformat(),
        "pages_scanned":           pages,
        "signals_found":           len(signals),
        "opportunities_generated": len(opps),
    })

    log.info(f"✅ {domain}: pages={pages} signals={len(signals)} opps={len(opps)} "
             f"robotics={scores.get('robotics_opportunity_score',0)} "
             f"intent={scores.get('buying_intent_score',0)}")


# ──────────────────────────────────────────────
# COMPANY SEED — Dataset industriale di partenza
# ──────────────────────────────────────────────

# Seed di aziende manifatturiere / industriali per settore
INDUSTRIAL_SEED = [
    # ── Automotive / Tier 1 ──
    {"domain": "magneti-marelli.com",   "name": "Magneti Marelli",    "industry": "automotive", "country": "IT"},
    {"domain": "brembo.com",            "name": "Brembo",             "industry": "automotive", "country": "IT"},
    {"domain": "piaggio.com",           "name": "Piaggio",            "industry": "automotive", "country": "IT"},
    {"domain": "comau.com",             "name": "Comau",              "industry": "robotics",   "country": "IT"},
    {"domain": "salvagnini.com",        "name": "Salvagnini",         "industry": "metalworking","country": "IT"},
    {"domain": "prima-industrie.com",   "name": "Prima Industrie",    "industry": "metalworking","country": "IT"},
    {"domain": "ficep.com",             "name": "Ficep",              "industry": "metalworking","country": "IT"},
    {"domain": "marposs.com",           "name": "Marposs",            "industry": "metrology",  "country": "IT"},
    {"domain": "datalogic.com",         "name": "Datalogic",          "industry": "automation", "country": "IT"},
    {"domain": "cama-group.com",        "name": "Cama Group",         "industry": "packaging",  "country": "IT"},
    {"domain": "ima.it",                "name": "IMA Group",          "industry": "packaging",  "country": "IT"},
    {"domain": "marchesini.com",        "name": "Marchesini Group",   "industry": "packaging",  "country": "IT"},
    {"domain": "coesia.com",            "name": "Coesia",             "industry": "packaging",  "country": "IT"},
    {"domain": "sacmi.com",             "name": "Sacmi",              "industry": "machinery",  "country": "IT"},
    {"domain": "cefla.com",             "name": "Cefla",              "industry": "machinery",  "country": "IT"},
    {"domain": "loccioni.com",          "name": "Loccioni",           "industry": "automation", "country": "IT"},
    {"domain": "interpump.com",         "name": "Interpump",          "industry": "hydraulics", "country": "IT"},
    {"domain": "comer-industries.com",  "name": "Comer Industries",   "industry": "machinery",  "country": "IT"},
    {"domain": "bonfiglioli.com",       "name": "Bonfiglioli",        "industry": "automation", "country": "IT"},
    {"domain": "elica.com",             "name": "Elica",              "industry": "appliances", "country": "IT"},

    # ── German Mittelstand ──
    {"domain": "trumpf.com",            "name": "Trumpf",             "industry": "metalworking","country": "DE"},
    {"domain": "kuka.com",              "name": "KUKA",               "industry": "robotics",   "country": "DE"},
    {"domain": "festo.com",             "name": "Festo",              "industry": "automation", "country": "DE"},
    {"domain": "sew-eurodrive.com",     "name": "SEW-Eurodrive",      "industry": "automation", "country": "DE"},
    {"domain": "belden.com",            "name": "Belden",             "industry": "automation", "country": "DE"},
    {"domain": "weinig.com",            "name": "Weinig Group",       "industry": "woodworking","country": "DE"},
    {"domain": "homag.com",             "name": "Homag",              "industry": "woodworking","country": "DE"},
    {"domain": "duerr.com",             "name": "Dürr",               "industry": "automotive", "country": "DE"},
    {"domain": "grob.de",               "name": "Grob",               "industry": "metalworking","country": "DE"},
    {"domain": "zf.com",                "name": "ZF Friedrichshafen", "industry": "automotive", "country": "DE"},

    # ── Food & Beverage Manufacturing ──
    {"domain": "tetra.pak.com",         "name": "Tetra Pak",          "industry": "food_packaging","country": "SE"},
    {"domain": "gea.com",               "name": "GEA Group",          "industry": "food_machinery","country": "DE"},
    {"domain": "alfa-laval.com",        "name": "Alfa Laval",         "industry": "food_processing","country": "SE"},
    {"domain": "sidel.com",             "name": "Sidel",              "industry": "food_packaging","country": "FR"},
    {"domain": "krones.com",            "name": "Krones",             "industry": "food_packaging","country": "DE"},

    # ── Logistics / Warehouse ──
    {"domain": "jungheinrich.com",      "name": "Jungheinrich",       "industry": "warehouse",  "country": "DE"},
    {"domain": "dematic.com",           "name": "Dematic",            "industry": "warehouse",  "country": "US"},
    {"domain": "vanderlande.com",       "name": "Vanderlande",        "industry": "warehouse",  "country": "NL"},
    {"domain": "swisslog.com",          "name": "Swisslog",           "industry": "warehouse",  "country": "CH"},
    {"domain": "knapp.com",             "name": "Knapp",              "industry": "warehouse",  "country": "AT"},
    {"domain": "ssi-schaefer.com",      "name": "SSI Schäfer",        "industry": "warehouse",  "country": "DE"},
    {"domain": "mecalux.com",           "name": "Mecalux",            "industry": "warehouse",  "country": "ES"},

    # ── Electronics / PCB Manufacturing ──
    {"domain": "jabil.com",             "name": "Jabil",              "industry": "electronics","country": "US"},
    {"domain": "flex.com",              "name": "Flex",               "industry": "electronics","country": "US"},
    {"domain": "celestica.com",         "name": "Celestica",          "industry": "electronics","country": "CA"},
    {"domain": "sanmina.com",           "name": "Sanmina",            "industry": "electronics","country": "US"},
    {"domain": "plexus.com",            "name": "Plexus",             "industry": "electronics","country": "US"},

    # ── Pharma / MedTech Manufacturing ──
    {"domain": "getinge.com",           "name": "Getinge",            "industry": "medical",    "country": "SE"},
    {"domain": "sartorius.com",         "name": "Sartorius",          "industry": "pharma",     "country": "DE"},
    {"domain": "grifols.com",           "name": "Grifols",            "industry": "pharma",     "country": "ES"},

    # ── Metal / Steel ──
    {"domain": "voestalpine.com",       "name": "voestalpine",        "industry": "steel",      "country": "AT"},
    {"domain": "outokumpu.com",         "name": "Outokumpu",          "industry": "steel",      "country": "FI"},
    {"domain": "ssab.com",              "name": "SSAB",               "industry": "steel",      "country": "SE"},

    # ── Plastics / Rubber ──
    {"domain": "arburg.com",            "name": "Arburg",             "industry": "plastics",   "country": "DE"},
    {"domain": "engel.at",              "name": "Engel Austria",      "industry": "plastics",   "country": "AT"},
    {"domain": "krauss-maffei.com",     "name": "KraussMaffei",       "industry": "plastics",   "country": "DE"},
    {"domain": "sumitomo-shi.com",      "name": "Sumitomo Heavy Industries","industry":"plastics","country":"JP"},

    # ── Small/Mid Italian Manufacturers ──
    {"domain": "camozzi.com",           "name": "Camozzi",            "industry": "pneumatics", "country": "IT"},
    {"domain": "gimatic.it",            "name": "Gimatic",            "industry": "robotics",   "country": "IT"},
    {"domain": "pneumax.it",            "name": "Pneumax",            "industry": "pneumatics", "country": "IT"},
    {"domain": "univer.it",             "name": "Univer",             "industry": "pneumatics", "country": "IT"},
    {"domain": "reer.it",               "name": "Reer",               "industry": "safety",     "country": "IT"},
    {"domain": "pizzato.net",           "name": "Pizzato Elettrica",  "industry": "safety",     "country": "IT"},
    {"domain": "gefran.com",            "name": "Gefran",             "industry": "automation", "country": "IT"},
    {"domain": "givi-misure.it",        "name": "GIVI Misure",        "industry": "metrology",  "country": "IT"},
    {"domain": "renishaw.com",          "name": "Renishaw",           "industry": "metrology",  "country": "GB"},
    {"domain": "hexagonmi.com",         "name": "Hexagon MI",         "industry": "metrology",  "country": "SE"},
]


# ──────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────

async def main():
    log.info(f"=== Industrial Scanner v1.0 — Worker {WORKER_ID}/{TOTAL_WORKERS} ===")
    log.info(f"Seed aziende: {len(INDUSTRIAL_SEED)} | Concurrency: {CONCURRENCY}")

    # Segmenta il seed tra i worker
    my_companies = [c for i, c in enumerate(INDUSTRIAL_SEED) if i % TOTAL_WORKERS == WORKER_ID]
    log.info(f"My batch: {len(my_companies)} aziende")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def scan_one(company):
            async with sem:
                result = await scan_domain(session, company)
                if result["pages_scanned"] > 0 or True:  # salva sempre
                    await save_results(session, result)
                await asyncio.sleep(DELAY_BETWEEN_DOMAINS)

        tasks = [scan_one(c) for c in my_companies]
        await asyncio.gather(*tasks, return_exceptions=True)

    log.info("=== Scan completato ===")


if __name__ == "__main__":
    asyncio.run(main())

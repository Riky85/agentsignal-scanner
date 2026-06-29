#!/usr/bin/env python3
"""
Industrial Opportunity Intelligence — Scanner Engine v3.0
- Multi-page scraping: homepage + auto-detect + fetch fino a 10 sottopagine
  (careers, about, products, news, jobs, quality, logistics, press, blog…)
- LLM Analysis: Anthropic Claude analizza il testo combinato e genera
  un summary strutturato con segnali, opportunità e buying intent
- ScanJob tracking: ogni scansione tracciata con status, pages_scanned,
  signals_found, error_message, duration_seconds
- 147 aziende seed manifatturiere EU + globali
- Pattern matching multilingue (IT / DE / EN)
"""

import asyncio
import aiohttp
import os
import json
import re
import logging
import time
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin, quote_plus, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [IND] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
B44_TOKEN     = (os.environ.get("B44_SERVICE_TOKEN") or
                 os.environ.get("BASE44_SERVICE_TOKEN") or "")
APP_ID        = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
B44_BASE      = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW            = {"api-key": B44_TOKEN, "Content-Type": "application/json"}

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
USE_LLM       = bool(ANTHROPIC_KEY)

WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
CONCURRENCY   = int(os.environ.get("CONCURRENCY", "5"))
PORT          = int(os.environ.get("PORT", 8080))
MAX_SUBPAGES  = int(os.environ.get("MAX_SUBPAGES", "10"))  # max sottopagine per azienda
REQUEST_TIMEOUT = 14
PAGE_DELAY    = 0.5

HEADERS = {
    "User-Agent": "IndustrialOpportunityBot/3.0 (+https://agentsignal.io/bot)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}

stats = {
    "scanned": 0, "signals": 0, "opportunities": 0,
    "errors": 0, "llm_calls": 0, "status": "starting",
}


# ─── HEALTHCHECK ──────────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass


threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True
).start()
log.info(f"Healthcheck su :{PORT}")


# ─── SIGNAL PATTERNS ─────────────────────────────────────────────────────────
SIGNALS = [
    # ROBOTICS
    {"cat": "robotics", "type": "palletizing", "conf": 88, "dmin": 60000, "dmax": 160000,
     "p": [r"\bpalletiz\w+\b", r"\bdepalletiz\w+\b", r"\bpallet\s+(robot|system|cell|automat)\b"]},
    {"cat": "robotics", "type": "pick_and_place", "conf": 80, "dmin": 40000, "dmax": 120000,
     "p": [r"\bpick[\s\-]and[\s\-]place\b", r"\bpick\s+&\s+place\b"]},
    {"cat": "robotics", "type": "welding_robot", "conf": 90, "dmin": 80000, "dmax": 220000,
     "p": [r"\brobotic\s+welding\b", r"\bwelding\s+robot\b", r"\barc\s+welding\b",
           r"\bspot\s+welding\b", r"\bsaldatura\s+robot\w*\b"]},
    {"cat": "robotics", "type": "assembly_robot", "conf": 75, "dmin": 60000, "dmax": 180000,
     "p": [r"\brobotic\s+assembl\w+\b", r"\bautomat\w+\s+assembl\w+\b",
           r"\bassemblaggio\s+automat\w+\b"]},
    {"cat": "robotics", "type": "collaborative_robot", "conf": 92, "dmin": 25000, "dmax": 90000,
     "p": [r"\bcobot\b", r"\bcollaborative\s+robot\b", r"\bhuman[\s\-]robot\s+collab\w+\b",
           r"\brobot\s+collaborat\w+\b"]},
    {"cat": "robotics", "type": "end_of_line", "conf": 78, "dmin": 50000, "dmax": 140000,
     "p": [r"\bend[\s\-]of[\s\-]line\b", r"\bcase\s+packing\b", r"\bfine\s+linea\b",
           r"\bshrink\s+wrap\w+\b"]},
    {"cat": "robotics", "type": "heavy_lifting", "conf": 70, "dmin": 40000, "dmax": 100000,
     "p": [r"\bheavy\s+lifting\b", r"\bmanual\s+lifting\b", r"\bsollev\w+\s+manual\w+\b"]},

    # CNC / MACHINE TENDING
    {"cat": "cnc_machine_tending", "type": "cnc_machine", "conf": 85, "dmin": 55000, "dmax": 180000,
     "p": [r"\bcnc\s+machin\w+\b", r"\bmazak\b", r"\bdmg\s+mori\b", r"\bhaas\b",
           r"\bokuma\b", r"\bfanuc\s+cnc\b", r"\bturning\s+cent\w+\b",
           r"\bmachining\s+cent\w+\b", r"\btorni\s+cnc\b"]},
    {"cat": "cnc_machine_tending", "type": "machine_tending", "conf": 92, "dmin": 65000, "dmax": 190000,
     "p": [r"\bmachine\s+tending\b", r"\bloading[/\s]unloading\b", r"\bload\s+unload\b",
           r"\bautomatic\s+loading\b", r"\bcaricamento\s+automat\w+\b"]},
    {"cat": "cnc_machine_tending", "type": "shift_production", "conf": 72, "dmin": 40000, "dmax": 120000,
     "p": [r"\b3[\s\-]shift\b", r"\bthree[\s\-]shift\b", r"\bnight\s+shift\s+production\b",
           r"\b24[\s/]7\s+production\b", r"\bturni\s+di\s+produzione\b", r"\b3\s+turni\b"]},
    {"cat": "cnc_machine_tending", "type": "lathe_milling", "conf": 80, "dmin": 50000, "dmax": 150000,
     "p": [r"\blathe\s+operat\w+\b", r"\bmilling\s+operat\w+\b", r"\bturning\s+operat\w+\b",
           r"\btornitura\b", r"\bfresatura\b", r"\brettifica\b"]},

    # AMR / AGV
    {"cat": "amr_agv", "type": "warehouse_logistics", "conf": 70, "dmin": 80000, "dmax": 300000,
     "p": [r"\bwarehousing\b", r"\bintralogistics\b", r"\binternal\s+logistics\b",
           r"\bmaterial\s+handling\b", r"\blogistica\s+interna\b"]},
    {"cat": "amr_agv", "type": "forklift_operations", "conf": 78, "dmin": 100000, "dmax": 350000,
     "p": [r"\bforklifts?\b", r"\bcarrelli\s+elevatori\b"]},
    {"cat": "amr_agv", "type": "agv_amr_mention", "conf": 96, "dmin": 120000, "dmax": 450000,
     "p": [r"\bAGV\b", r"\bAMR\b", r"\bautonomous\s+mobile\s+robot\b",
           r"\bguided\s+vehicle\b", r"\bveicoli\s+automat\w+\b"]},
    {"cat": "amr_agv", "type": "warehouse_expansion", "conf": 82, "dmin": 150000, "dmax": 600000,
     "p": [r"\bnew\s+warehouse\b", r"\bwarehouse\s+expansion\b",
           r"\bnew\s+distribution\s+cent\w+\b", r"\bnuovo\s+magazzino\b",
           r"\bampliamento\s+magazzino\b", r"\bneues\s+lager\b"]},
    {"cat": "amr_agv", "type": "picking_operations", "conf": 75, "dmin": 80000, "dmax": 260000,
     "p": [r"\border\s+picking\b", r"\bgoods[\s\-]to[\s\-]person\b",
           r"\bpicking\s+effic\w+\b", r"\bprelievo\s+automat\w+\b"]},

    # MES / SCADA
    {"cat": "mes_scada", "type": "mes_mention", "conf": 92, "dmin": 50000, "dmax": 220000,
     "p": [r"\bMES\b", r"\bmanufacturing\s+execution\s+system\b", r"\bsistema\s+MES\b"]},
    {"cat": "mes_scada", "type": "scada_mention", "conf": 92, "dmin": 40000, "dmax": 190000,
     "p": [r"\bSCADA\b", r"\bsupervisory\s+control\b", r"\bsistema\s+SCADA\b"]},
    {"cat": "mes_scada", "type": "plc_systems", "conf": 85, "dmin": 25000, "dmax": 160000,
     "p": [r"\bPLC\b", r"\bprogrammable\s+logic\b", r"\bsiemens\s+s7\b",
           r"\ballen[\s\-]bradley\b", r"\brockwell\s+automat\w+\b",
           r"\bschneider\s+electric\b", r"\bomron\b", r"\bmitsubishi\s+electric\b"]},
    {"cat": "mes_scada", "type": "oee_monitoring", "conf": 86, "dmin": 30000, "dmax": 130000,
     "p": [r"\bOEE\b", r"\boverall\s+equipment\s+effect\w+\b",
           r"\bproduction\s+monitoring\b", r"\bdowntime\s+monitor\w+\b",
           r"\bshop\s+floor\b", r"\bmonitoraggio\s+produz\w+\b"]},
    {"cat": "mes_scada", "type": "industry40", "conf": 80, "dmin": 50000, "dmax": 280000,
     "p": [r"\bindustry\s+4\.0\b", r"\bindustrie\s+4\.0\b", r"\bsmart\s+factory\b",
           r"\bdigital\s+twin\b", r"\biiot\b", r"\bopc[\s\-]ua\b", r"\bfabbrica\s+4\.0\b"]},
    {"cat": "mes_scada", "type": "traceability", "conf": 80, "dmin": 30000, "dmax": 110000,
     "p": [r"\btraceability\b", r"\bbatch\s+tracking\b", r"\brintracciabilit\w+\b",
           r"\bserial\s+number\s+tracking\b"]},

    # MACHINE VISION
    {"cat": "machine_vision", "type": "quality_inspection", "conf": 85, "dmin": 30000, "dmax": 160000,
     "p": [r"\bquality\s+inspection\b", r"\bvisual\s+inspection\b", r"\bdefect\s+detection\b",
           r"\binspection\s+line\b", r"\bcontrollo\s+qualit\w+\b", r"\bispezione\s+visiva\b"]},
    {"cat": "machine_vision", "type": "computer_vision", "conf": 92, "dmin": 40000, "dmax": 210000,
     "p": [r"\bcomputer\s+vision\b", r"\bmachine\s+vision\b", r"\bvision\s+system\b",
           r"\bcamera\s+inspection\b", r"\bvisione\s+artificiale\b"]},
    {"cat": "machine_vision", "type": "nonconformity", "conf": 76, "dmin": 25000, "dmax": 110000,
     "p": [r"\bnon[\s\-]conformit\w+\b", r"\bdefect\s+rate\b", r"\bscrap\s+rate\b",
           r"\bzero\s+defect\b", r"\bscarti\s+di\s+produzione\b"]},
    {"cat": "machine_vision", "type": "metrology", "conf": 80, "dmin": 30000, "dmax": 120000,
     "p": [r"\bmetrolog\w+\b", r"\bdimensional\s+inspection\b",
           r"\bcoordinate\s+measuring\b", r"\bCMM\b"]},

    # GROWTH / BUYING INTENT
    {"cat": "growth_buying_intent", "type": "new_factory", "conf": 92, "dmin": 200000, "dmax": 1200000,
     "p": [r"\bnew\s+(factory|plant|facility|site)\b", r"\bgreen[\s\-]field\b",
           r"\bnuovo\s+stabilimento\b", r"\bneues\s+werk\b",
           r"\bopening\s+(new|a)\s+(plant|facility|factory)\b"]},
    {"cat": "growth_buying_intent", "type": "production_expansion", "conf": 86, "dmin": 100000, "dmax": 600000,
     "p": [r"\bproduction\s+expansion\b", r"\bexpanding\s+capacity\b",
           r"\bcapacity\s+increase\b", r"\bnew\s+production\s+line\b",
           r"\bespansione\s+produttiv\w+\b", r"\bampliamento\s+stabilimento\b",
           r"\bincrease\s+production\s+capacity\b"]},
    {"cat": "growth_buying_intent", "type": "automation_project", "conf": 82, "dmin": 80000, "dmax": 450000,
     "p": [r"\bautomation\s+(project|initiative|investment|program)\b",
           r"\bdigital\s+transformation\b", r"\boperational\s+efficiency\b",
           r"\blabor\s+shortage\b", r"\bprogetto\s+automazion\w+\b",
           r"\btrasformazione\s+digitale\b"]},
    {"cat": "growth_buying_intent", "type": "investment_announcement", "conf": 78, "dmin": 150000, "dmax": 700000,
     "p": [r"\b\d+\s*m\w*\s+investment\b", r"\binvestment\s+plan\b",
           r"\bcapital\s+expenditure\b", r"\bcapex\b",
           r"\binvestimento\s+di\s+\d+\b"]},
    {"cat": "growth_buying_intent", "type": "hiring_production_staff", "conf": 74, "dmin": 60000, "dmax": 300000,
     "p": [r"\bhiring\s+(production|manufacturing|assembly|warehouse)\s+\w+\b",
           r"\bricerchiamo\s+operatori\b", r"\bopen\s+position\w*.*production\b"]},

    # HIRING
    {"cat": "hiring", "type": "automation_engineer_hiring", "conf": 92, "dmin": 60000, "dmax": 220000,
     "p": [r"\bautomation\s+engineer\b", r"\brobotic\w*\s+engineer\b",
           r"\bprocess\s+automation\s+engineer\b", r"\bingegnere\s+di\s+automazione\b",
           r"\bAutomatisierungsingenieur\b"]},
    {"cat": "hiring", "type": "plc_programmer_hiring", "conf": 87, "dmin": 35000, "dmax": 160000,
     "p": [r"\bplc\s+programm\w+\b", r"\bsiemens\s+programm\w+\b",
           r"\bscada\s+engineer\b", r"\bcontrol\s+systems\s+engineer\b",
           r"\bprogrammatore\s+plc\b"]},
    {"cat": "hiring", "type": "maintenance_technician_hiring", "conf": 80, "dmin": 25000, "dmax": 100000,
     "p": [r"\bmaintenance\s+technician\b", r"\bindustrial\s+electrician\b",
           r"\bpredictive\s+maintenance\b", r"\btecnico\s+manutentore\b"]},
    {"cat": "hiring", "type": "manufacturing_engineer_hiring", "conf": 76, "dmin": 50000, "dmax": 190000,
     "p": [r"\bmanufacturing\s+engineer\b", r"\bproduction\s+engineer\b",
           r"\blean\s+engineer\b", r"\bcontinuous\s+improvement\b",
           r"\bingegnere\s+di\s+produzione\b"]},
    {"cat": "hiring", "type": "mes_specialist_hiring", "conf": 92, "dmin": 50000, "dmax": 220000,
     "p": [r"\bmes\s+specialist\b", r"\bmes\s+engineer\b", r"\bscada\s+specialist\b",
           r"\bmanufacturing\s+it\b", r"\bspecialista\s+mes\b"]},
    {"cat": "hiring", "type": "warehouse_operator_hiring", "conf": 72, "dmin": 80000, "dmax": 300000,
     "p": [r"\bwarehouse\s+operator\b", r"\blogistics\s+operator\b",
           r"\bfork\s*lift\s+(driver|operator)\b", r"\baddetto\s+magazzino\b"]},
    {"cat": "hiring", "type": "quality_technician_hiring", "conf": 78, "dmin": 30000, "dmax": 140000,
     "p": [r"\bquality\s+control\s+technician\b", r"\bquality\s+inspector\b",
           r"\bquality\s+assurance\s+engineer\b", r"\btecnico\s+qualit\w+\b"]},
]

# ─── PRIORITY SUBPAGES ────────────────────────────────────────────────────────
# Path da scansionare — ordinati per priorità segnali
PRIORITY_PAGES = [
    "/careers", "/jobs", "/lavora-con-noi", "/stellenangebote",
    "/job-openings", "/open-positions", "/vacancies",
    "/news", "/notizie", "/press", "/blog",
    "/about", "/about-us", "/chi-siamo", "/uber-uns",
    "/manufacturing", "/produzione", "/production",
    "/quality", "/qualita", "/quality-assurance",
    "/warehouse", "/magazzino", "/logistics",
    "/automation", "/automazione", "/technology",
    "/solutions", "/industries", "/products",
]

# Link anchor keyword che indicano sottopagine ad alto valore
HIGH_VALUE_ANCHORS = re.compile(
    r"career|job|vacan|hiring|recruit|employ|"
    r"news|press|blog|article|announc|"
    r"lavora|posizioni|offerte|notizie|stampa|"
    r"karriere|stellen|aktuell|presse|"
    r"about|about.us|who.we|chi.siam|"
    r"expansion|invest|growth|project|initiative|"
    r"nuov|ampliamento|stabilimento",
    re.I,
)


# ─── SCORING ──────────────────────────────────────────────────────────────────
OPPS = {
    "collaborative_robot":    {"cats": ["robotics","cnc_machine_tending"], "dmin": 25000,  "dmax": 90000,  "sol": "Collaborative Robot Cell (Cobot)"},
    "industrial_robot":       {"cats": ["robotics"],                       "dmin": 80000,  "dmax": 260000, "sol": "Industrial Robot System"},
    "amr_agv":                {"cats": ["amr_agv"],                        "dmin": 100000, "dmax": 450000, "sol": "AMR / AGV Fleet"},
    "machine_tending":        {"cats": ["cnc_machine_tending"],            "dmin": 60000,  "dmax": 200000, "sol": "CNC Machine Tending Robot"},
    "palletizing":            {"cats": ["robotics"],                       "dmin": 55000,  "dmax": 160000, "sol": "End-of-Line Palletizing Robot"},
    "packaging_automation":   {"cats": ["robotics"],                       "dmin": 45000,  "dmax": 140000, "sol": "Packaging Automation System"},
    "mes_scada":              {"cats": ["mes_scada"],                      "dmin": 45000,  "dmax": 230000, "sol": "MES / SCADA Platform"},
    "plc_upgrade":            {"cats": ["mes_scada"],                      "dmin": 20000,  "dmax": 90000,  "sol": "PLC Retrofit & Upgrade"},
    "computer_vision":        {"cats": ["machine_vision"],                 "dmin": 30000,  "dmax": 220000, "sol": "Computer Vision Inspection System"},
    "predictive_maintenance": {"cats": ["hiring"],                         "dmin": 20000,  "dmax": 90000,  "sol": "Predictive Maintenance Platform"},
    "warehouse_automation":   {"cats": ["amr_agv"],                        "dmin": 100000, "dmax": 450000, "sol": "Warehouse Automation System"},
    "industrial_ai":          {"cats": ["mes_scada","machine_vision"],     "dmin": 45000,  "dmax": 220000, "sol": "Industrial AI Platform"},
}


def compute_scores(signals):
    cat_conf, cat_cnt = {}, {}
    for s in signals:
        c = s["signal_category"]
        cat_conf[c] = max(cat_conf.get(c, 0), s["confidence_score"])
        cat_cnt[c]  = cat_cnt.get(c, 0) + 1

    def sc(cats, w=1.0):
        hits = sum(cat_cnt.get(c, 0) for c in cats)
        if not hits: return 0
        base = sum(cat_conf.get(c, 0) for c in cats if c in cat_conf)
        raw  = min(100, int(base / len(cats) * w))
        return min(100, raw + min(20, hits * 5))

    return {
        "automation_readiness_score":       sc(["robotics","cnc_machine_tending","amr_agv","mes_scada"], 0.9),
        "robotics_opportunity_score":       sc(["robotics","cnc_machine_tending"], 1.2),
        "amr_agv_opportunity_score":        sc(["amr_agv"], 1.3),
        "mes_opportunity_score":            sc(["mes_scada"], 1.2),
        "machine_vision_opportunity_score": sc(["machine_vision"], 1.2),
        "buying_intent_score":              sc(["growth_buying_intent","hiring"], 1.4),
    }


def generate_opps(cid, cname, domain, signals, scores):
    opps = []
    for otype, cfg in OPPS.items():
        relevant = [s for s in signals if s["signal_category"] in cfg["cats"]]
        if not relevant: continue
        avg_conf = sum(s["confidence_score"] for s in relevant) / len(relevant)
        osc = min(100, int(avg_conf + len(relevant) * 5 + scores.get("buying_intent_score", 0) // 10))
        if osc < 40: continue
        f = osc / 100
        opps.append({
            "company_id": cid, "company_name": cname, "company_domain": domain,
            "opportunity_type": otype,
            "recommended_solution": cfg["sol"],
            "opportunity_score": osc,
            "buying_intent_score": scores.get("buying_intent_score", 0),
            "estimated_deal_value_min": int(cfg["dmin"] * (0.7 + 0.3 * f)),
            "estimated_deal_value_max": int(cfg["dmax"] * (0.7 + 0.3 * f)),
            "reason_summary": f"Signals: {', '.join(s['signal_type'] for s in relevant[:3])}. → {cfg['sol']}.",
            "signals_count": len(relevant),
            "top_signals": [s["signal_type"] for s in sorted(relevant, key=lambda x: -x["confidence_score"])[:3]],
        })
    opps.sort(key=lambda x: -x["opportunity_score"])
    return opps[:5]


# ─── LLM ANALYSIS ────────────────────────────────────────────────────────────
LLM_PROMPT = """You are an industrial automation sales intelligence expert.
Analyze the following combined text from a manufacturing company's website
(homepage + careers + news + about pages) and extract buying signals.

Company: {name} ({domain})

Text:
{text}

Return a JSON object with:
{{
  "company_summary": "2-3 sentence description of what they manufacture",
  "employees_estimate": <integer or null>,
  "key_signals": ["signal1", "signal2", ...],  // max 5 specific automation buying signals
  "new_facility": true/false,          // opening new factory/warehouse/facility?
  "expansion_evidence": "quote or null", // direct quote showing expansion
  "hiring_automation": true/false,     // hiring automation/robotics/PLC engineers?
  "hiring_evidence": "quote or null",
  "manual_processes": ["process1", ...], // manual processes mentioned (loading, inspection, etc.)
  "technology_gaps": ["gap1", ...],    // missing technologies (no MES, manual QC, etc.)
  "urgency_score": <0-100>,           // how urgent is their automation need?
  "recommended_pitch": "1 sentence sales pitch for automation solution"
}}

Return ONLY valid JSON, no markdown."""


async def llm_analyze(session, name, domain, combined_text):
    """Chiama Claude Haiku per analisi LLM del testo combinato."""
    if not ANTHROPIC_KEY or not combined_text:
        return {}

    # Tronca a 8000 chars per non sforare il context
    text_chunk = combined_text[:8000]

    payload = {
        "model": "claude-haiku-4-5",
        "max_tokens": 800,
        "messages": [{
            "role": "user",
            "content": LLM_PROMPT.format(
                name=name, domain=domain, text=text_chunk
            )
        }]
    }

    try:
        async with session.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                raw = d.get("content", [{}])[0].get("text", "")
                stats["llm_calls"] += 1
                # Parse JSON dal response
                raw = re.sub(r"^```json\s*|```\s*$", "", raw.strip())
                return json.loads(raw)
            else:
                log.debug(f"LLM {r.status}: {await r.text()}")
                return {}
    except Exception as e:
        log.debug(f"LLM ERR {domain}: {e}")
        return {}


def boost_signals_with_llm(llm_result, cid, cname, domain, existing_signals):
    """Aggiunge segnali extra trovati dall'LLM non catturati dai pattern."""
    extra = []
    now = datetime.now(timezone.utc).isoformat()

    if llm_result.get("new_facility") and llm_result.get("expansion_evidence"):
        extra.append({
            "company_id": cid, "company_name": cname, "company_domain": domain,
            "signal_category": "growth_buying_intent", "signal_type": "new_factory",
            "source_url": f"https://{domain}/",
            "evidence_text": llm_result["expansion_evidence"][:400],
            "confidence_score": 88, "detected_at": now,
        })

    if llm_result.get("hiring_automation") and llm_result.get("hiring_evidence"):
        extra.append({
            "company_id": cid, "company_name": cname, "company_domain": domain,
            "signal_category": "hiring", "signal_type": "automation_engineer_hiring",
            "source_url": f"https://{domain}/careers",
            "evidence_text": llm_result["hiring_evidence"][:400],
            "confidence_score": 85, "detected_at": now,
        })

    for process in (llm_result.get("manual_processes") or [])[:3]:
        proc_lower = process.lower()
        if "loading" in proc_lower or "tending" in proc_lower:
            cat, stype = "cnc_machine_tending", "machine_tending"
        elif "palletiz" in proc_lower or "pallet" in proc_lower:
            cat, stype = "robotics", "palletizing"
        elif "inspection" in proc_lower or "quality" in proc_lower:
            cat, stype = "machine_vision", "quality_inspection"
        elif "lifting" in proc_lower:
            cat, stype = "robotics", "heavy_lifting"
        else:
            continue

        existing_types = {s["signal_type"] for s in existing_signals + extra}
        if stype not in existing_types:
            extra.append({
                "company_id": cid, "company_name": cname, "company_domain": domain,
                "signal_category": cat, "signal_type": stype,
                "source_url": f"https://{domain}/",
                "evidence_text": f"LLM detected manual process: {process}",
                "confidence_score": 72, "detected_at": now,
            })

    return extra


# ─── MULTI-PAGE SCRAPER ───────────────────────────────────────────────────────
def clean(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style", "noscript", "head"]):
            t.decompose()
        return " ".join(soup.get_text(" ").split())[:60000]
    except Exception:
        return html[:20000]


def extract_internal_links(html, base_url):
    """Estrae link interni dalla pagina, prioritizzando quelli ad alto valore."""
    try:
        base_domain = urlparse(base_url).netloc
        soup = BeautifulSoup(html, "html.parser")
        high, normal = [], []

        for a in soup.find_all("a", href=True):
            href   = a.get("href", "").strip()
            anchor = a.get_text(strip=True)

            # Normalizza URL
            if href.startswith("/"):
                full_url = f"{urlparse(base_url).scheme}://{base_domain}{href}"
            elif href.startswith("http"):
                # Solo link allo stesso dominio
                if base_domain not in href:
                    continue
                full_url = href
            else:
                continue

            # Escludi pattern non utili
            if any(x in full_url.lower() for x in
                   ["#", "mailto:", "tel:", ".pdf", ".jpg", ".png", ".zip",
                    "login", "register", "cart", "checkout", "privacy", "cookie"]):
                continue

            # Categorizza per priorità
            if HIGH_VALUE_ANCHORS.search(anchor) or HIGH_VALUE_ANCHORS.search(href):
                high.append(full_url)
            else:
                normal.append(full_url)

        # Dedup preservando ordine
        seen, result = set(), []
        for url in high + normal:
            if url not in seen:
                seen.add(url)
                result.append(url)

        return result

    except Exception:
        return []


def detect(text, url, cid, cname, domain):
    tl = text.lower()
    found, seen = [], set()
    for p in SIGNALS:
        k = (p["cat"], p["type"])
        if k in seen:
            continue
        for pat in p["p"]:
            try:
                m = re.search(pat, tl)
            except Exception:
                continue
            if m:
                s, e = max(0, m.start() - 100), min(len(text), m.end() + 100)
                found.append({
                    "company_id": cid, "company_name": cname, "company_domain": domain,
                    "signal_category": p["cat"], "signal_type": p["type"],
                    "source_url": url, "evidence_text": text[s:e].strip()[:400],
                    "confidence_score": p["conf"],
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                seen.add(k)
                break
    return found


async def fetch(session, url, timeout=None):
    t = timeout or REQUEST_TIMEOUT
    for att in range(2):
        try:
            async with session.get(
                url, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=t),
                allow_redirects=True, ssl=False
            ) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    if "text" in ct or "html" in ct:
                        return await r.text(errors="replace")
                return ""
        except Exception:
            if att == 0:
                await asyncio.sleep(1)
    return ""


# ─── BASE44 API ───────────────────────────────────────────────────────────────
async def b44_post(session, entity, data):
    url = f"{B44_BASE}/{entity}"
    try:
        async with session.post(url, headers=HW, json=data,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status in (200, 201):
                d = await r.json(content_type=None)
                return d.get("id", "") if isinstance(d, dict) else ""
    except Exception as e:
        log.debug(f"POST {entity}: {e}")
    return ""


async def b44_put(session, entity, eid, data):
    url = f"{B44_BASE}/{entity}/{eid}"
    try:
        async with session.put(url, headers=HW, json=data,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            return r.status in (200, 201)
    except Exception:
        return False

async def fetch_revenue(session, domain, name=""):
    """Fetch fatturato da Schema.org JSON-LD + Apollo (se disponibile)."""
    result = {"revenue_range": "", "employee_count": None, "description": ""}

    # --- Apollo.io (priorità massima) ---
    apollo_key = os.environ.get("APOLLO_KEY", "")
    if apollo_key:
        try:
            async with session.get(
                "https://api.apollo.io/api/v1/organizations/enrich",
                params={"domain": domain},
                headers={"x-api-key": apollo_key, "accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    org = d.get("organization") or {}
                    if org:
                        rev = org.get("annual_revenue_printed") or ""
                        emp = org.get("estimated_num_employees") or org.get("num_employees")
                        desc = (org.get("short_description") or org.get("seo_description",""))[:500]
                        if rev or emp:
                            result["revenue_range"]  = rev[:80] if rev else ""
                            result["employee_count"] = int(emp) if emp else None
                            result["description"]    = desc
                            return result
        except Exception:
            pass

    # --- Schema.org JSON-LD dalla homepage ---
    try:
        async with session.get(f"https://{domain}",
                               timeout=aiohttp.ClientTimeout(total=8),
                               headers={"User-Agent": "Mozilla/5.0 (compatible; IndustrialBot/1.0)"}) as r:
            if r.status == 200:
                html = await r.text(errors="replace")
                scripts = re.findall(
                    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                    html, re.S | re.I)
                for raw in scripts:
                    try:
                        item = json.loads(raw.strip())
                        if isinstance(item, list): item = item[0] if item else {}
                        t = item.get("@type", "")
                        if isinstance(t, list): t = t[0] if t else ""
                        if t in ("Organization","LocalBusiness","Corporation","Company","NGO"):
                            emp_node = item.get("numberOfEmployees")
                            emp = None
                            if isinstance(emp_node, dict): emp = emp_node.get("value")
                            elif isinstance(emp_node, (int,float)): emp = int(emp_node)
                            desc = item.get("description","")[:500]
                            if emp or desc:
                                result["employee_count"] = emp
                                result["description"]    = desc
                                break
                    except: pass
    except Exception:
        pass

    return result



async def get_or_create_company(session, c):
    domain = c["domain"]
    url = f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if isinstance(d, list) and d:
                    return d[0]["id"]
    except Exception:
        pass
    return await b44_post(session, "IndustrialCompany", {
        "name": c.get("name", domain), "domain": domain,
        "website_url": f"https://{domain}",
        "country": c.get("country", ""), "city": c.get("city", ""),
        "industry": c.get("industry", ""),
        "scan_status": "pending", "source": "seed_v3",
    })


# ─── SCAN JOB TRACKING ────────────────────────────────────────────────────────
async def create_scan_job(session, cid, domain):
    """Crea un ScanJob in stato 'running' e ritorna il suo ID."""
    return await b44_post(session, "IndustrialScanJob", {
        "company_id":    cid,
        "company_domain": domain,
        "status":        "running",
        "started_at":    datetime.now(timezone.utc).isoformat(),
    })


async def complete_scan_job(session, job_id, pages, signals, opps, error=None):
    """Aggiorna il ScanJob al completamento."""
    await b44_put(session, "IndustrialScanJob", job_id, {
        "status":                  "failed" if error else "done",
        "completed_at":            datetime.now(timezone.utc).isoformat(),
        "pages_scanned":           pages,
        "signals_found":           signals,
        "opportunities_generated": opps,
        "error_message":           error or "",
    })


# ─── MULTI-PAGE SCAN ─────────────────────────────────────────────────────────
async def scan_company(session, c):
    """
    Scansione multi-page:
    1. Fetch homepage
    2. Estrai link interni (priorità a careers/news/about)
    3. Fetch fino a MAX_SUBPAGES sottopagine
    4. Pattern matching su ogni pagina
    5. LLM analysis sul testo combinato (se ANTHROPIC_KEY disponibile)
    6. Salva segnali + opportunity + scan job su Base44
    """
    domain   = c["domain"]
    base_url = f"https://{domain}"
    cname    = c.get("name", domain)
    start_ts = time.time()

    # Usa ID esistente se già presente (dal feeder), altrimenti cerca/crea
    cid = c.get("id") or await get_or_create_company(session, c)
    if not cid:
        stats["errors"] += 1
        return

    # Crea ScanJob in stato running
    job_id = await create_scan_job(session, cid, domain)

    try:
        # ── Step 1: Homepage ──────────────────────────────────────────────────
        homepage_html = await fetch(session, base_url)
        if not homepage_html:
            homepage_html = await fetch(session, base_url.replace("https://", "http://"))

        if not homepage_html:
            await complete_scan_job(session, job_id, 0, 0, 0, "Homepage unreachable")
            return

        # ── Step 2: Discover subpages ─────────────────────────────────────────
        # Prima prova i PRIORITY_PAGES fissi, poi aggiungi link dal crawl
        candidate_urls = []

        # Aggiungi priority paths
        for path in PRIORITY_PAGES:
            candidate_urls.append(base_url.rstrip("/") + path)

        # Aggiungi link estratti dalla homepage (high-value first)
        discovered = extract_internal_links(homepage_html, base_url)
        for url in discovered:
            if url not in candidate_urls:
                candidate_urls.append(url)

        # ── Step 3: Fetch subpages ────────────────────────────────────────────
        all_pages = [("homepage", base_url, clean(homepage_html))]
        fetched_urls = {base_url}
        pages_done = 1

        for candidate in candidate_urls:
            if pages_done >= MAX_SUBPAGES + 1:  # +1 per homepage
                break
            if candidate in fetched_urls:
                continue

            html = await fetch(session, candidate)
            if html:
                page_text = clean(html)
                if len(page_text) > 200:  # pagina con contenuto reale
                    # Determina tipo pagina dal path
                    path_lower = candidate.lower()
                    if any(k in path_lower for k in ["career","job","vacan","recruit","posizioni"]):
                        page_type = "careers"
                    elif any(k in path_lower for k in ["news","blog","press","stampa","notizie"]):
                        page_type = "news"
                    elif any(k in path_lower for k in ["about","chi","uber"]):
                        page_type = "about"
                    elif any(k in path_lower for k in ["product","prodott"]):
                        page_type = "products"
                    else:
                        page_type = "other"

                    all_pages.append((page_type, candidate, page_text))
                    fetched_urls.add(candidate)
                    pages_done += 1

            await asyncio.sleep(PAGE_DELAY)

        # ── Step 4: Pattern matching su tutte le pagine ───────────────────────
        all_sigs = []
        for page_type, url, text in all_pages:
            sigs = detect(text, url, cid, cname, domain)
            # Boost confidence per segnali trovati su pagine specifiche
            for s in sigs:
                if page_type == "careers" and s["signal_category"] == "hiring":
                    s["confidence_score"] = min(100, s["confidence_score"] + 8)
                elif page_type == "news" and s["signal_category"] == "growth_buying_intent":
                    s["confidence_score"] = min(100, s["confidence_score"] + 10)
            all_sigs.extend(sigs)

        # ── Step 5: LLM Analysis (se API key disponibile) ────────────────────
        llm_result = {}
        if USE_LLM:
            # Combina testo da pagine prioritarie
            combined = " | ".join([
                t for ptype, _, t in all_pages
                if ptype in ("homepage", "careers", "news", "about")
            ])[:10000]
            if combined:
                llm_result = await llm_analyze(session, cname, domain, combined)
                if llm_result:
                    extra_sigs = boost_signals_with_llm(llm_result, cid, cname, domain, all_sigs)
                    all_sigs.extend(extra_sigs)

        # Dedup segnali: tieni il più confident per ogni (cat, type)
        dedup = {}
        for s in all_sigs:
            k = (s["signal_category"], s["signal_type"])
            if k not in dedup or s["confidence_score"] > dedup[k]["confidence_score"]:
                dedup[k] = s
        sigs = list(dedup.values())

        # ── Step 6: Scoring & Opportunities ──────────────────────────────────
        scores   = compute_scores(sigs)
        best_opp = max(scores, key=scores.get) if scores else ""

        # Genera opportunity PRIMA del PUT così possiamo includere deal value
        opps = generate_opps(cid, cname, domain, sigs, scores)

        # Best deal value dalla top opportunity
        deal_min = opps[0]["estimated_deal_value_min"] if opps else 0
        deal_max = opps[0]["estimated_deal_value_max"] if opps else 0
        best_opp_type = opps[0]["opportunity_type"] if opps else best_opp

        # Fetch revenue + employees (Apollo > Schema.org)
        revenue_data = await fetch_revenue(session, domain, cname)

        # Enrich description da LLM se disponibile
        # CAMPI CONFERMATI come scrivibili via PUT su B44 (service token):
        # ✅ revenue (string), employee_count (number), estimated_deal_value_max (number)
        # ✅ buying_intent_score, automation_readiness_score, robotics_opportunity_score
        # ❌ scan_status, revenue_range, annual_revenue_eur_k (nuovi — non persistiti su record vecchi)
        company_update = {**scores,
                          "estimated_deal_value_min": deal_min,
                          "estimated_deal_value_max": deal_max}

        # Revenue come stringa leggibile nel campo 'revenue' (già presente)
        if not c.get("revenue"):
            rev_str = ""
            if revenue_data.get("revenue_range"):
                rev_str = str(revenue_data["revenue_range"])[:80]
            elif revenue_data.get("annual_revenue_eur_k"):
                k = int(revenue_data["annual_revenue_eur_k"])
                if k >= 1_000_000: rev_str = f"EUR {k//1000:.0f}M+"
                elif k >= 1_000:   rev_str = f"EUR {k//1000:.0f}M"
                else:              rev_str = f"EUR {k}K"
            if rev_str:
                company_update["revenue"] = rev_str
                stats["revenue_found"] = stats.get("revenue_found", 0) + 1

        # Dipendenti
        if revenue_data.get("employee_count") and not c.get("employee_count"):
            company_update["employee_count"] = int(revenue_data["employee_count"])
        if llm_result.get("employees_estimate") and not c.get("employee_count") and not revenue_data.get("employee_count"):
            emp = llm_result["employees_estimate"]
            company_update["employee_count"] = emp
            sz = ("micro" if emp < 10 else "small" if emp < 50 else
                  "medium" if emp < 250 else "large" if emp < 1000 else "enterprise")
            company_update["company_size"] = sz

        await b44_put(session, "IndustrialCompany", cid, company_update)

        # Salva segnali
        for s in sigs:
            await b44_post(session, "IndustrialSignal", s)
            await asyncio.sleep(0.07)

        # Salva opportunity
        for o in opps:
            await b44_post(session, "IndustrialOpportunity", o)
            await asyncio.sleep(0.07)

        # Completa ScanJob
        duration = int(time.time() - start_ts)
        await complete_scan_job(session, job_id, pages_done, len(sigs), len(opps))

        stats["scanned"]       += 1
        stats["signals"]       += len(sigs)
        stats["opportunities"] += len(opps)

        llm_note = f" 🤖llm={bool(llm_result)}" if USE_LLM else ""
        log.info(
            f"[{stats['scanned']:4d}] {domain:<35s} "
            f"pages={pages_done:2d} sigs={len(sigs):2d} opps={len(opps):2d} "
            f"rob={scores.get('robotics_opportunity_score',0):3d} "
            f"intent={scores.get('buying_intent_score',0):3d} "
            f"⏱{duration}s{llm_note}"
        )

    except Exception as e:
        log.warning(f"ERR {domain}: {e}")
        stats["errors"] += 1
        await complete_scan_job(session, job_id, 0, 0, 0, str(e)[:200])


# ─── SEED COMPANIES ──────────────────────────────────────────────────────────
SEED_COMPANIES = [
    # IT — Automazione e Robotica
    {"domain": "comau.com",          "name": "Comau",              "industry": "robotics",      "country": "IT", "city": "Turin"},
    {"domain": "salvagnini.com",     "name": "Salvagnini",         "industry": "metalworking",  "country": "IT", "city": "Sarego"},
    {"domain": "prima-industrie.com","name": "Prima Industrie",    "industry": "metalworking",  "country": "IT", "city": "Collegno"},
    {"domain": "ficep.com",          "name": "Ficep",              "industry": "metalworking",  "country": "IT", "city": "Varese"},
    {"domain": "marposs.com",        "name": "Marposs",            "industry": "metrology",     "country": "IT", "city": "Bologna"},
    {"domain": "datalogic.com",      "name": "Datalogic",          "industry": "automation",    "country": "IT", "city": "Bologna"},
    {"domain": "loccioni.com",       "name": "Loccioni",           "industry": "automation",    "country": "IT", "city": "Angeli"},
    {"domain": "bonfiglioli.com",    "name": "Bonfiglioli",        "industry": "automation",    "country": "IT", "city": "Bologna"},
    {"domain": "camozzi.com",        "name": "Camozzi Automation", "industry": "pneumatics",    "country": "IT", "city": "Brescia"},
    {"domain": "gimatic.it",         "name": "Gimatic",            "industry": "robotics",      "country": "IT", "city": "Orzinuovi"},
    {"domain": "pneumax.it",         "name": "Pneumax",            "industry": "pneumatics",    "country": "IT", "city": "Lurano"},
    {"domain": "gefran.com",         "name": "Gefran",             "industry": "automation",    "country": "IT", "city": "Provaglio"},
    {"domain": "cama-group.com",     "name": "Cama Group",         "industry": "packaging",     "country": "IT", "city": "Lecco"},
    {"domain": "ima.it",             "name": "IMA Group",          "industry": "packaging",     "country": "IT", "city": "Bologna"},
    {"domain": "marchesini.com",     "name": "Marchesini Group",   "industry": "packaging",     "country": "IT", "city": "Bologna"},
    {"domain": "coesia.com",         "name": "Coesia",             "industry": "packaging",     "country": "IT", "city": "Bologna"},
    {"domain": "sacmi.com",          "name": "Sacmi",              "industry": "machinery",     "country": "IT", "city": "Imola"},
    {"domain": "brembo.com",         "name": "Brembo",             "industry": "automotive",    "country": "IT", "city": "Curno"},
    {"domain": "interpump.com",      "name": "Interpump",          "industry": "hydraulics",    "country": "IT", "city": "Reggio Emilia"},
    {"domain": "elica.com",          "name": "Elica",              "industry": "appliances",    "country": "IT", "city": "Fabriano"},
    {"domain": "tenova.com",         "name": "Tenova",             "industry": "steel",         "country": "IT", "city": "Milan"},
    {"domain": "elettric80.com",     "name": "Elettric80",         "industry": "warehouse_agv", "country": "IT", "city": "Viano"},
    {"domain": "arol.com",           "name": "Arol",               "industry": "packaging",     "country": "IT", "city": "Canelli"},
    {"domain": "gd.it",              "name": "G.D",                "industry": "packaging",     "country": "IT", "city": "Bologna"},
    {"domain": "robopac.com",        "name": "Robopac",            "industry": "wrapping",      "country": "IT", "city": "Forli"},
    {"domain": "ocme.com",           "name": "OCME",               "industry": "packaging",     "country": "IT", "city": "Parma"},
    {"domain": "automha.com",        "name": "Automha",            "industry": "warehouse_agv", "country": "IT", "city": "Cologno al Serio"},
    {"domain": "ferretto-group.com", "name": "Ferretto Group",     "industry": "warehouse",     "country": "IT", "city": "Vicenza"},
    {"domain": "datasensor.com",     "name": "Datasensor",         "industry": "sensors",       "country": "IT", "city": "San Giorgio"},
    {"domain": "reer.it",            "name": "Reer",               "industry": "safety",        "country": "IT", "city": "Turin"},
    {"domain": "pizzato.net",        "name": "Pizzato Elettrica",  "industry": "safety",        "country": "IT", "city": "Rossano Veneto"},
    {"domain": "gefran.com",         "name": "Gefran",             "industry": "automation",    "country": "IT", "city": "Provaglio"},
    # DE — German Mittelstand
    {"domain": "trumpf.com",         "name": "Trumpf",             "industry": "metalworking",  "country": "DE", "city": "Ditzingen"},
    {"domain": "kuka.com",           "name": "KUKA",               "industry": "robotics",      "country": "DE", "city": "Augsburg"},
    {"domain": "festo.com",          "name": "Festo",              "industry": "automation",    "country": "DE", "city": "Esslingen"},
    {"domain": "sew-eurodrive.com",  "name": "SEW-Eurodrive",      "industry": "automation",    "country": "DE", "city": "Bruchsal"},
    {"domain": "duerr.com",          "name": "Dürr",               "industry": "automotive",    "country": "DE", "city": "Bietigheim"},
    {"domain": "grob.de",            "name": "Grob-Werke",         "industry": "metalworking",  "country": "DE", "city": "Mindelheim"},
    {"domain": "zf.com",             "name": "ZF Friedrichshafen", "industry": "automotive",    "country": "DE", "city": "Friedrichshafen"},
    {"domain": "schaeffler.com",     "name": "Schaeffler",         "industry": "automotive",    "country": "DE", "city": "Herzogenaurach"},
    {"domain": "ifm.com",            "name": "IFM Electronic",     "industry": "sensors",       "country": "DE", "city": "Essen"},
    {"domain": "sick.com",           "name": "Sick AG",            "industry": "sensors",       "country": "DE", "city": "Waldkirch"},
    {"domain": "beckhoff.com",       "name": "Beckhoff Automation","industry": "automation",    "country": "DE", "city": "Verl"},
    {"domain": "pilz.com",           "name": "Pilz",               "industry": "safety",        "country": "DE", "city": "Ostfildern"},
    {"domain": "schunk.com",         "name": "Schunk",             "industry": "gripping",      "country": "DE", "city": "Lauffen"},
    {"domain": "igus.com",           "name": "Igus",               "industry": "energy_chains", "country": "DE", "city": "Cologne"},
    {"domain": "basler.com",         "name": "Basler",             "industry": "vision_cameras","country": "DE", "city": "Ahrensburg"},
    {"domain": "mvtec.com",          "name": "MVTec Software",     "industry": "machine_vision","country": "DE", "city": "Munich"},
    {"domain": "isra-vision.com",    "name": "ISRA Vision",        "industry": "machine_vision","country": "DE", "city": "Darmstadt"},
    {"domain": "pepperl-fuchs.com",  "name": "Pepperl+Fuchs",      "industry": "sensors",       "country": "DE", "city": "Mannheim"},
    {"domain": "balluff.com",        "name": "Balluff",            "industry": "sensors",       "country": "DE", "city": "Neuhausen"},
    {"domain": "krones.com",         "name": "Krones",             "industry": "food_packaging","country": "DE", "city": "Neutraubling"},
    {"domain": "gea.com",            "name": "GEA Group",          "industry": "food_machinery","country": "DE", "city": "Duesseldorf"},
    {"domain": "krauss-maffei.com",  "name": "KraussMaffei",       "industry": "plastics",      "country": "DE", "city": "Munich"},
    {"domain": "linde-mh.com",       "name": "Linde Material Handling","industry": "forklifts", "country": "DE", "city": "Aschaffenburg"},
    {"domain": "ssi-schaefer.com",   "name": "SSI Schaefer",       "industry": "warehouse",     "country": "DE", "city": "Neunkirchen"},
    {"domain": "jungheinrich.com",   "name": "Jungheinrich",       "industry": "warehouse",     "country": "DE", "city": "Hamburg"},
    # AT / CH
    {"domain": "engel.at",           "name": "Engel Austria",      "industry": "plastics",      "country": "AT", "city": "Schwertberg"},
    {"domain": "knapp.com",          "name": "Knapp",              "industry": "warehouse",     "country": "AT", "city": "Hart"},
    {"domain": "voestalpine.com",    "name": "voestalpine",        "industry": "steel",         "country": "AT", "city": "Linz"},
    {"domain": "bystronic.com",      "name": "Bystronic",          "industry": "metalworking",  "country": "CH", "city": "Niederoenz"},
    {"domain": "maxon.com",          "name": "Maxon",              "industry": "precision_motors","country": "CH","city": "Sachseln"},
    {"domain": "swisslog.com",       "name": "Swisslog",           "industry": "warehouse",     "country": "CH", "city": "Buchs"},
    {"domain": "interroll.com",      "name": "Interroll",          "industry": "conveying",     "country": "CH", "city": "Sant Antonio"},
    {"domain": "endress-hauser.com", "name": "Endress+Hauser",     "industry": "process_auto",  "country": "CH", "city": "Reinach"},
    {"domain": "baumer.com",         "name": "Baumer",             "industry": "sensors",       "country": "CH", "city": "Frauenfeld"},
    # FR / ES / NL
    {"domain": "sidel.com",          "name": "Sidel",              "industry": "food_packaging","country": "FR", "city": "Octeville"},
    {"domain": "fives.com",          "name": "Fives",              "industry": "industrial_auto","country": "FR","city": "Paris"},
    {"domain": "staubli.com",        "name": "Staubli",            "industry": "robotics",      "country": "FR", "city": "Faverges"},
    {"domain": "savoye.com",         "name": "Savoye",             "industry": "warehouse",     "country": "FR", "city": "Courcouronnes"},
    {"domain": "mecalux.com",        "name": "Mecalux",            "industry": "warehouse",     "country": "ES", "city": "Barcelona"},
    {"domain": "vanderlande.com",    "name": "Vanderlande",        "industry": "warehouse",     "country": "NL", "city": "Veghel"},
    # SE / DK / FI / GB
    {"domain": "renishaw.com",       "name": "Renishaw",           "industry": "metrology",     "country": "GB", "city": "Wotton-under-Edge"},
    {"domain": "hexagonmi.com",      "name": "Hexagon MI",         "industry": "metrology",     "country": "SE", "city": "Stockholm"},
    {"domain": "abb.com",            "name": "ABB",                "industry": "robotics_auto", "country": "CH", "city": "Zurich"},
    {"domain": "danfoss.com",        "name": "Danfoss",            "industry": "drives",        "country": "DK", "city": "Nordborg"},
    {"domain": "ur.dk",              "name": "Universal Robots",   "industry": "cobots",        "country": "DK", "city": "Odense"},
    # JP
    {"domain": "fanuc.eu",           "name": "Fanuc Europe",       "industry": "robotics",      "country": "JP", "city": "Luxembourg"},
    {"domain": "omron.com",          "name": "Omron",              "industry": "automation",    "country": "JP", "city": "Kyoto"},
    {"domain": "yaskawa.eu",         "name": "Yaskawa Europe",     "industry": "robotics",      "country": "JP", "city": "Allershausen"},
    {"domain": "kawasaki-robotics.com","name": "Kawasaki Robotics","industry": "robotics",      "country": "JP", "city": "Akashi"},
    {"domain": "keyence.com",        "name": "Keyence",            "industry": "sensors_vision","country": "JP", "city": "Osaka"},
    # US
    {"domain": "jabil.com",          "name": "Jabil",              "industry": "electronics",   "country": "US", "city": "St. Petersburg"},
    {"domain": "dematic.com",        "name": "Dematic",            "industry": "warehouse",     "country": "US", "city": "Grand Rapids"},
    {"domain": "rockwellautomation.com","name": "Rockwell Automation","industry": "automation",  "country": "US", "city": "Milwaukee"},
    {"domain": "cognex.com",         "name": "Cognex",             "industry": "machine_vision","country": "US", "city": "Natick"},
    {"domain": "parker.com",         "name": "Parker Hannifin",    "industry": "motion_control","country": "US", "city": "Cleveland"},
    {"domain": "emerson.com",        "name": "Emerson Electric",   "industry": "automation",    "country": "US", "city": "St. Louis"},
    {"domain": "honeywell.com",      "name": "Honeywell",          "industry": "automation",    "country": "US", "city": "Charlotte"},
    {"domain": "teledyne-dalsa.com", "name": "Teledyne DALSA",     "industry": "vision_cameras","country": "CA", "city": "Waterloo"},
    # Pharma / Medical
    {"domain": "getinge.com",        "name": "Getinge",            "industry": "medical",       "country": "SE", "city": "Gothenburg"},
    {"domain": "sartorius.com",      "name": "Sartorius",          "industry": "pharma",        "country": "DE", "city": "Goettingen"},
]


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    my = [c for i, c in enumerate(SEED_COMPANIES) if i % TOTAL_WORKERS == WORKER_ID]
    llm_note = f" | LLM={'ON (claude-haiku)' if USE_LLM else 'OFF (set ANTHROPIC_API_KEY)'}"
    log.info(f"=== Industrial Scanner v3.0 | Worker {WORKER_ID}/{TOTAL_WORKERS} | "
             f"{len(my)} aziende | MAX_SUBPAGES={MAX_SUBPAGES}{llm_note} ===")

    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:

        async def _run(c):
            async with sem:
                try:
                    await scan_company(session, c)
                except Exception as e:
                    log.warning(f"ERR {c['domain']}: {e}")
                    stats["errors"] += 1
                await asyncio.sleep(1.5)

        await asyncio.gather(*[_run(c) for c in my], return_exceptions=True)

    stats["status"] = "done"
    log.info(
        f"=== COMPLETATO: scanned={stats['scanned']} signals={stats['signals']} "
        f"opps={stats['opportunities']} llm_calls={stats['llm_calls']} "
        f"errors={stats['errors']} ==="
    )

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())

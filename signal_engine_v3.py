#!/usr/bin/env python3
"""
signal_engine_v3.py — Motore segnali industriali
v3.2: keyword drasticamente ridotti e più specifici per eliminare falsi positivi
Marker: buying_intent_score=None → non scansionato / =0 → scansionato senza segnali
Quality check ogni 50 scan, scansione continua
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_KEY = os.environ.get("BASE44_API_KEY","907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID","6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT",8080))
UA      = {"User-Agent":"Mozilla/5.0 Chrome/124 Safari/537.36",
           "Accept":"text/html,*/*",
           "Accept-Language":"en-US,en;q=0.9,it;q=0.8,de;q=0.7,fr;q=0.6"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,
         "current":"","queue":0,"good_signals":0,
         "last_qc":"never","quality":{}}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body=json.dumps(stats,default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda:HTTPServer(("0.0.0.0",PORT),H).serve_forever(),daemon=True).start()
log.info(f"[OK] Healthcheck :{PORT}")

# ════════════════════════════════════════════════════════════
# KEYWORD v3.2 — PRECISI, CONTESTUALI, SOLO MANIFATTURA
# Regola: ogni keyword deve essere INEQUIVOCABILE nel contesto
#         di un sito manifatturiero
# ════════════════════════════════════════════════════════════

# AUTOMATION — presenza di tecnologie di automazione nel sito
AUTO_KW = [
    # Robotica fisica (molto specifici)
    "industrial robot","collaborative robot","cobot","robotic arm","robot cell",
    "robot palletizer","welding robot","robot welder","robot gripper",
    "autonomous mobile robot","automated guided vehicle","agv system","amr system",
    # CNC e lavorazioni
    "cnc machining","cnc machine","cnc turning","cnc milling","cnc lathe",
    "machining center","turning center","5-axis machining","5 axis machining",
    "grinding machine","milling machine","lathe machine",
    # Automazione di processo
    "plc programming","scada system","hmi interface","servo drive","servo system",
    "machine vision system","vision inspection","automated inspection",
    "conveyor system","automated conveyor","automated assembly line",
    "pick and place","palletizing system","depalletizing",
    "laser cutting machine","laser welding machine","plasma cutting",
    # IT (italiano)
    "robot industriale","braccio robotico","cella robotizzata",
    "saldatura robotizzata","macchina cnc","lavorazione cnc",
    "controllo numerico computerizzato","sistema di visione",
    "nastro trasportatore automatico","linea automatizzata",
    # DE (tedesco)
    "industrieroboter","schweißroboter","roboterarm","roboterzelle",
    "cnc-fräsmaschine","cnc-drehmaschine","cnc-bearbeitungszentrum",
    "fördersystem","automatische montagelinie","bildverarbeitungssystem",
    # FR (francese)
    "robot industriel","bras robotique","cellule robotisée",
    "soudage robotisé","machine cnc","usinage cnc","centre d'usinage",
    "convoyeur automatique","système de vision industrielle",
]

# ROBOTICS OPPORTUNITY — segnali che indicano BISOGNO di robot
# (processi manuali + settori target + volumi alti)
ROBOT_KW = [
    # Processi manuali ad alto rischio → ottimi candidati cobot
    "manual welding","manual assembly","manual handling","manual loading",
    "heavy lifting","ergonomic risk","repetitive motion","repetitive assembly",
    "hazardous environment","hot environment","dusty environment",
    # Settori con altissima adozione robotica
    "tier 1 supplier","tier 2 supplier","automotive supplier","auto parts",
    "electronics assembly","pcb assembly","semiconductor assembly",
    "injection molding","die casting","metal stamping","sheet metal stamping",
    "forging plant","foundry","casting plant",
    # Volumi e turni (indica bisogno di automazione)
    "high volume production","mass production","continuous production",
    "3 shift operation","three shift","24/7 operation","lights out manufacturing",
    # Celle e integrazioni
    "robot integration","robotic integration","system integration",
    "robot cell design","turnkey automation","end of line automation",
    # IT
    "saldatura manuale","assemblaggio manuale","movimentazione manuale",
    "sollevamento carichi","rischio ergonomico","produzione di massa",
    "fornitore automotive","fornitore tier","fonderia","stampaggio",
    # DE
    "manuelle schweißung","manuelle montage","manuelle handhabung",
    "schwerlasthandhabung","automobilzulieferer","großserienfertigung",
    "druckguss","spritzguss","gießerei","blechstanzung",
    # FR
    "soudage manuel","assemblage manuel","manutention manuelle",
    "fournisseur automotive","fonderie","emboutissage","production de masse",
]

# MES — segnali di DIGITALIZZAZIONE della produzione
# (sia gap che presenza — entrambi indicano opportunità)
MES_KW = [
    # Sistemi già presenti (upsell / upgrade)
    "manufacturing execution system","mes system","erp system","sap manufacturing",
    "oee monitoring","overall equipment effectiveness",
    "scada system","dcs system","distributed control",
    "opc ua","opcua protocol","mqtt protocol",
    "digital twin","digital factory","smart factory",
    "iiot platform","industrial iot","industry 4.0 platform",
    "predictive maintenance","condition monitoring","vibration monitoring",
    "production scheduling software","advanced planning",
    # Gap digitale (bisogno urgente)
    "paper-based","paper records","manual records","excel-based tracking",
    "production traceability","lot traceability","batch traceability",
    "serialization system","track and trace system",
    "quality management system","qms software",
    # Compliance che richiede MES
    "iso 9001 certified","iatf 16949","iso 13485 certified",
    "fda 21 cfr part 11","cfr part 11","gmp compliant",
    # IT
    "sistema mes","sistema erp produzione","industria 4.0",
    "manutenzione predittiva","gemello digitale","fabbrica digitale",
    "tracciabilità di produzione","monitoraggio oee",
    # DE
    "fertigungsmanagementsystem","predictive maintenance system",
    "industrie 4.0","digitale fabrik","digitaler zwilling",
    "zustandsüberwachung","anlageneffizienz",
    # FR
    "système mes","maintenance prédictive","usine numérique",
    "jumeau numérique","industrie 4.0","traçabilité de production",
]

# BUYING INTENT — segnali CONTESTUALI di acquisto imminente
# REGOLA CHIAVE: ogni keyword qui deve comparire su un sito MANIFATTURIERO
# con chiaro senso di investimento/crescita/cambiamento
INTENT_KW = [
    # Espansione fisica (verde → acquisto macchinari)
    "new manufacturing plant","new production facility","greenfield plant",
    "brownfield expansion","capacity expansion","production capacity increase",
    "new factory opening","plant expansion","new assembly line",
    "new production line","new machining line","new welding line",
    # Hiring tecnico specifico (manifattura)
    "automation engineer","robotics engineer","manufacturing engineer",
    "process engineer","production engineer","cnc programmer",
    "robot programmer","plc programmer","scada engineer",
    "lean manufacturing engineer","industrial engineer",
    # Investimenti tecnologici espliciti
    "capital expenditure","capex investment","technology investment",
    "equipment investment","machinery investment","automation investment",
    "digital transformation manufacturing","industry 4.0 implementation",
    "lean transformation","manufacturing modernization",
    # Retrofit e upgrade
    "machine retrofit","equipment upgrade","production line upgrade",
    "legacy system replacement","plc upgrade","scada upgrade",
    # IT
    "nuovo stabilimento produttivo","ampliamento capacità produttiva",
    "nuova linea di produzione","nuovo impianto manifatturiero",
    "ingegnere automazione","ingegnere produzione","programmatore cnc",
    "trasformazione digitale produzione","modernizzazione impianti",
    "investimento tecnologico","revamping impianto",
    # DE
    "neues produktionswerk","kapazitätserweiterung","neue produktionslinie",
    "automatisierungsingenieur","fertigungsingenieur","cnc-programmierer",
    "digitale transformation fertigung","maschinenmodernisierung",
    "investition automatisierung","retrofit maschinen",
    # FR
    "nouvelle usine de production","expansion capacité","nouvelle ligne de production",
    "ingénieur automatisation","ingénieur de production","programmeur cnc",
    "transformation digitale industrie","modernisation des équipements",
    "investissement automatisation","retrofit machines",
]

# Blacklist contestuale — se questi termini dominano il sito, skip
BLACKLIST = re.compile(
    r'\b(law firm|law office|legal services|avvocato|anwaltskanzlei|'
    r'we buy houses|real estate agent|immobilienmakler|'
    r'restaurant|ristorante|hotel|albergo|'
    r'insurance broker|financial advisor|wealth management|'
    r'software development company|app development|web agency|'
    r'digital marketing agency|seo agency|'
    r'university|hospital|school|'
    r'charity|non.?profit|ngo|onlus)\b',
    re.I
)

def fetch(url, timeout=7):
    try:
        r = requests.get(url,headers=UA,timeout=timeout,verify=False,allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 300:
            t = re.sub(r'<script[^>]*>.*?</script>',' ',r.text,flags=re.S)
            t = re.sub(r'<style[^>]*>.*?</style>','',t,flags=re.S)
            t = re.sub(r'<[^>]+>',' ',t)
            return re.sub(r'\s+',' ',t).lower()[:12000]
    except: pass
    return ""

def scan(rec):
    domain = (rec.get("domain") or "").strip()
    if not domain:
        return None  # skip silenzioso

    base_url = rec.get("website_url") or f"https://www.{domain}"
    if not base_url.startswith("http"): base_url = f"https://www.{domain}"

    # Scarica homepage + pagine produttive
    text = ""; pages_ok = 0
    for url in [base_url,
                f"{base_url}/products",    f"{base_url}/prodotti",
                f"{base_url}/solutions",   f"{base_url}/soluzioni",
                f"{base_url}/technology",  f"{base_url}/technologie",
                f"{base_url}/careers",     f"{base_url}/lavora-con-noi",
                f"{base_url}/jobs"]:
        t = fetch(url)
        if t: text += " " + t; pages_ok += 1
        if len(text) > 28000: break

    if len(text.strip()) < 300:
        return {"_unreachable": True}

    # Blacklist check — sito non industriale
    if BLACKLIST.search(text[:3000]):  # Solo primi 3000 char (homepage)
        return {"_blacklisted": True}

    # Calcola hit per ogni categoria
    a_h  = [k for k in AUTO_KW   if k in text]
    ro_h = [k for k in ROBOT_KW  if k in text]
    m_h  = [k for k in MES_KW    if k in text]
    i_h  = [k for k in INTENT_KW if k in text]

    # Score con pesi calibrati
    # Ogni keyword è ora molto specifica → pesi più alti per compensare
    auto_s   = min(100, len(a_h)  * 8)   # max ~12 hit tipici → 96
    robot_s  = min(100, len(ro_h) * 12)  # max ~8 hit tipici → 96
    mes_s    = min(100, len(m_h)  * 10)  # max ~10 hit tipici → 100
    intent_s = min(100, len(i_h)  * 15)  # max ~6 hit tipici → 90

    # Determina categoria principale
    scores = {
        "Robotics & Cobot Integration":    robot_s,
        "MES / Digital Factory":           mes_s,
        "Industrial Automation Upgrade":   auto_s,
        "Proactive Outreach – High Intent": intent_s,
    }
    top = max(scores, key=scores.get)
    best = scores[top]

    ev = {
        "Robotics & Cobot Integration":    ro_h[:4],
        "MES / Digital Factory":           m_h[:4],
        "Industrial Automation Upgrade":   a_h[:4],
        "Proactive Outreach – High Intent": i_h[:4],
    }
    if best < 10:
        top = "Low Signal"
        solution = "No specific manufacturing signals detected"
    else:
        solution = "Signals: " + ", ".join(ev[top])

    # Deal estimate
    emp = int(float(rec.get("employee_count") or 0))
    if   emp > 5000: dmin,dmax = 300000, 2000000
    elif emp > 500:  dmin,dmax = 80000,  500000
    elif emp > 100:  dmin,dmax = 25000,  120000
    elif emp > 20:   dmin,dmax = 8000,   40000
    else:            dmin,dmax = 3000,   15000
    if intent_s > 40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)

    return {
        "automation_readiness_score":  auto_s,
        "robotics_opportunity_score":  robot_s,
        "mes_opportunity_score":       mes_s,
        "buying_intent_score":         intent_s,
        "amr_agv_opportunity_score":   round((robot_s + auto_s) / 2),
        "estimated_deal_value_min":    dmin,
        "estimated_deal_value_max":    dmax,
        "_top":      top,
        "_solution": solution,
        "_pages":    pages_ok,
    }

def quality_check():
    log.info("  ── QUALITY CHECK ──")
    try:
        skip=0; total=0; with_sig=0; zero_c=0; pending=0; opps={}; top10=[]
        while True:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=buying_intent_score,"
                             f"robotics_opportunity_score,automation_readiness_score,"
                             f"mes_opportunity_score,name",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b,list) or not b: break
            for x in b:
                bi = x.get("buying_intent_score")
                if bi is None: pending+=1; continue
                total+=1
                ro = x.get("robotics_opportunity_score") or 0
                au = x.get("automation_readiness_score") or 0
                me = x.get("mes_opportunity_score") or 0
                best = max(bi, ro, au, me)
                if best == 0: zero_c+=1
                elif best >= 20: with_sig+=1
                if bi >= 30: top10.append((bi, ro, au, me, x.get("name","?")))
            skip+=500
            if len(b)<500: break

        signal_rate = round(with_sig/total*100,1) if total else 0
        top10.sort(reverse=True)
        log.info(f"  Totale: {total+pending} | Scansionati: {total} | Pending: {pending}")
        log.info(f"  Con segnale (≥20): {with_sig} ({signal_rate}%) | Zero: {zero_c}")
        if top10:
            log.info("  Top 5 per buying intent:")
            for bi,ro,au,me,nm in top10[:5]:
                log.info(f"    {nm[:40]:42} intent={bi:.0f} robot={ro:.0f} auto={au:.0f} mes={me:.0f}")
        stats["quality"] = {"total":total,"pending":pending,"with_signal":with_sig,
                            "zero":zero_c,"signal_rate":signal_rate}
        stats["last_qc"] = time.strftime("%H:%M:%S")
        log.info("  ── END QC ──")
    except Exception as e:
        log.warning(f"quality_check: {e}")

def load_pending(limit=100):
    """buying_intent_score IS NULL = non ancora scansionato"""
    results=[]; skip=0
    while len(results) < limit:
        try:
            b = requests.get(
                f"{BASE}?limit=200&skip={skip}"
                f"&fields=id,name,domain,website_url,employee_count,"
                f"buying_intent_score,country,description",
                headers=HDRS, timeout=20).json()
        except Exception as e:
            log.warning(f"load_pending: {e}"); break
        if not isinstance(b,list) or not b: break
        for r in b:
            if r.get("buying_intent_score") is None:
                results.append(r)
                if len(results) >= limit: break
        if len(b) < 200: break
        skip += 200
    return results

PRIORITY = {"ITA","IT","ITALY","DEU","DE","DD","GERMANY","FRA","FR","FRANCE",
            "ESP","ES","SPAIN","CHE","CH","AUT","AT","NLD","NL","BEL","BE",
            "POL","PL","SWE","SE","FIN","FI","USA","US","UNITED STATES",
            "GBR","GB","UK","JPN","JP","JAPAN"}

log.info("=== SIGNAL ENGINE v3.2 — Keyword precisi, no falsi positivi ===")
log.info("Marker: buying_intent_score=None → pending | =0 → no signal")
log.info("Quality check ogni 50 scan")

scan_since_qc = 0

while True:
    stats["cycle"] += 1
    batch = load_pending(limit=100)
    stats["queue"] = len(batch)

    if not batch:
        log.info("DB completamente scansionato. QC finale + pausa 2h.")
        quality_check()
        time.sleep(7200)
        continue

    batch.sort(key=lambda x:(0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
    log.info(f"[C{stats['cycle']}] Batch {len(batch)} (IT/DE/FR/US in testa)")

    for rec in batch:
        name   = (rec.get("name") or rec.get("domain") or "?")[:38]
        domain = rec.get("domain","?")
        stats["current"] = domain

        try:
            result = scan(rec)

            if result is None:
                # Nessun dominio — skip senza scrivere
                continue

            if result.get("_unreachable") or result.get("_blacklisted"):
                # Segna come "scansionato senza segnale" con score 0
                r = requests.put(f"{BASE}/{rec['id']}",
                    json={"buying_intent_score":0,"automation_readiness_score":0,
                          "robotics_opportunity_score":0,"mes_opportunity_score":0},
                    headers=HDRS, timeout=10)
                if r.status_code in (200,201,204):
                    stats["unreachable"] += 1
                    scan_since_qc += 1
                    reason = "unreachable" if result.get("_unreachable") else "blacklisted"
                    log.info(f"  ⚠️  {name}: {reason}")
                continue

            payload = {
                "automation_readiness_score": result["automation_readiness_score"],
                "robotics_opportunity_score":  result["robotics_opportunity_score"],
                "mes_opportunity_score":       result["mes_opportunity_score"],
                "buying_intent_score":         result["buying_intent_score"],
                "amr_agv_opportunity_score":   result["amr_agv_opportunity_score"],
                "estimated_deal_value_min":    result["estimated_deal_value_min"],
                "estimated_deal_value_max":    result["estimated_deal_value_max"],
            }
            # Scrivi il tag nella description solo se il campo è vuoto
            existing = (rec.get("description") or "").strip()
            top = result["_top"]
            if not existing and top != "Low Signal":
                payload["description"] = f"[{top}] {result['_solution']}"[:400]

            r = requests.put(f"{BASE}/{rec['id']}",json=payload,headers=HDRS,timeout=15)

            if r.status_code in (200,201,204):
                stats["scanned"] += 1
                scan_since_qc += 1
                bi = result["buying_intent_score"]
                ro = result["robotics_opportunity_score"]
                au = result["automation_readiness_score"]
                me = result["mes_opportunity_score"]
                if max(bi,ro,au,me) >= 20: stats["good_signals"] += 1
                log.info(f"  ✅ {name:40} "
                         f"auto={au:3.0f} robot={ro:3.0f} mes={me:3.0f} intent={bi:3.0f} "
                         f"→ {top[:35]}")
            else:
                stats["errors"] += 1
                log.warning(f"  ❌ {domain}: HTTP {r.status_code} {r.text[:60]}")

        except Exception as e:
            stats["errors"] += 1
            log.warning(f"  ❌ {domain}: {e}")

        if scan_since_qc >= 50:
            scan_since_qc = 0
            quality_check()

        time.sleep(1.0)

    log.info(f"[C{stats['cycle']}] DONE — "
             f"scanned={stats['scanned']} unreachable={stats['unreachable']} "
             f"good={stats['good_signals']} errors={stats['errors']}")

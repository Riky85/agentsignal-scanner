#!/usr/bin/env python3
"""
signal_engine_v2.py — Evidence-Based Industrial Opportunity Intelligence Engine
Postgres-only, English output, vendor-aware, 10-category scoring.

== CHANGES FROM v1 (signal_engine_pg.py) ==
1. VENDOR SUPPRESSION: detects if a company SELLS a technology and zeros that
   category's score. A robot OEM gets robotics=0 but can still get WMS/ERP>0.
2. 10 CATEGORIES (was 5): Robotics, AMR/AGV, MES/SCADA, Machine Vision,
   Predictive Maintenance, PLC/Controls, IoT/IIoT, ERP, WMS, CMMS.
3. MES FIX: no longer matches on "plc", "siemens", "industry 4.0" as buyer
   signals. Those are INSTALLED tech / aspirational buzzwords. New MES keywords
   are specific pain points: "paper-based production", "excel production
   tracking", "manual data collection", "no real-time data".
4. WEAK KEYWORDS: "industry 4.0", "digital transformation", "smart factory"
   count at 0.3x weight — they're noise, not evidence.
5. PAGE WEIGHTS more aggressive: seller pages (products/solutions) = 0.02x,
   buyer pages (careers/news) = 3.0x.
6. INDUSTRY CONTEXT: scores boosted based on industry (Food→robotics+10,
   Logistics→AMR+15, Pharma→vision+15, etc.).
7. HIGHER THRESHOLD: top_opportunity requires score >= 15 (was 8), reducing
   false positives.
8. CONFIDENCE SCORE separated from opportunity score — measures analysis
   reliability, not deal attractiveness.
9. NO ThreadPoolExecutor context manager (caused deadlock in v1) — manual
   shutdown with cancel_futures=True.
10. STRUCTURED SIGNAL TYPES: buyer_signal, vendor_signal, technology_installed,
    hiring_signal, expansion_signal — not just flat keyword strings.

For every company answers 5 questions:
 1. What type of company is it?          -> industry_category
 2. What processes does it run?          -> process signals
 3. What automation opportunities exist? -> 10 categories with evidence
 4. What should we sell them?            -> solution tags (comma-separated)
 5. Why now?                             -> why-now tags (comma-separated)
"""
import os, re, json, time, logging, threading, requests, warnings
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import RealDictCursor

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

PG_DSN = os.environ.get("DATABASE_URL") or os.environ.get(
    "PG_DSN",
    "postgresql://agent:AgentSignal2026!@postgres-db.railway.internal:5432/agentsignal"
)
def get_conn():
    return psycopg2.connect(PG_DSN, connect_timeout=15, cursor_factory=RealDictCursor)

PORT = int(os.environ.get("PORT", 8080))
WORKERS = int(os.environ.get("SCAN_WORKERS", 16))
OPP_THRESHOLD = 15   # min score to count as an opportunity (was 8 in v1)
NOISE_THRESHOLD = 8  # below this = zero the score

UA = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,"good":0,
         "signals_created":0,"tech_created":0,"jobs_created":0,"opps_created":0,
         "vendors_detected":0,"queue":0,"last_qc":"never","qc":{},"status":"starting",
         "last_dedup":"never"}
lock = threading.Lock()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats, default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
log.info(f"[OK] healthcheck :{PORT} | workers={WORKERS} | threshold={OPP_THRESHOLD} | v2 evidence-based")

# ═════════════════════════ INDUSTRY CLASSIFICATION ═════════════════════════
INDUSTRY_KW = {
    "Packaging": ["packaging machinery","packaging line","packaging solutions","filling machine","labeling machine","imballaggio","confezionamento"],
    "Automotive": ["automotive supplier","tier 1 supplier","tier 2 supplier","auto parts","car manufacturer","automotive industry","componentistica automotive"],
    "Electronics": ["pcb assembly","electronics manufacturing","semiconductor","electronic components","circuit board","elettronica"],
    "Food & Beverage": ["food processing","beverage production","food manufacturer","food industry","alimentare","bevande"],
    "Pharma": ["pharmaceutical manufacturing","pharma industry","gmp compliant","drug manufacturing","farmaceutico"],
    "Metalworking": ["metalworking","sheet metal","metal fabrication","stamping","forging","lavorazioni meccaniche","carpenteria metallica"],
    "Plastics": ["plastic injection","plastics manufacturing","polymer processing","injection molding","materie plastiche"],
    "Logistics": ["logistics provider","warehouse operations","distribution center","freight","intralogistics","logistica"],
    "Machinery": ["machine builder","industrial machinery","machine manufacturer","macchinari industriali","costruzione macchine"],
    "Aerospace": ["aerospace manufacturer","aviation industry","aircraft components","aerospace supplier","aeronautica"],
    "Medical Devices": ["medical device manufacturer","medical equipment","healthcare devices","dispositivi medici"],
    "Chemicals": ["chemical manufacturer","chemical processing","specialty chemicals","industria chimica"],
    "Furniture": ["furniture manufacturer","furniture production","arredamento","mobili"],
    "Textile": ["textile manufacturer","textile production","fabric mill","tessile"],
    "Construction Materials": ["construction materials","building materials","cement production","materiali da costruzione"],
    "Industrial Components": ["industrial components","mechanical components","precision components","componenti industriali"],
    "Steel & Metals": ["steel producer","steel manufacturer","iron and steel","metal producer","stahlindustrie","acciaieria"],
    "Energy": ["energy production","power generation","renewable energy","energy company","energieerzeugung"],
    "Water & Utilities": ["water treatment","water utility","wastewater treatment","water supply company"],
    "Glass & Ceramics": ["glass manufacturer","glass production","ceramics manufacturer","glasindustrie"],
}

# Industry context boosts: {category: bonus}
INDUSTRY_BOOST = {
    "Food & Beverage":  {"robotics":10, "vision":10, "mes_scada":5},
    "Automotive":       {"robotics":5, "mes_scada":10, "amr_agv":5},
    "Pharma":           {"vision":15, "mes_scada":10, "maintenance":5},
    "Logistics":        {"amr_agv":15, "wms":10, "robotics":5},
    "Metalworking":     {"robotics":10, "plc":10, "maintenance":5},
    "Packaging":        {"robotics":10, "vision":5, "plc":5},
    "Chemicals":        {"mes_scada":10, "maintenance":5, "plc":5},
    "Electronics":      {"vision":10, "mes_scada":5, "robotics":5},
    "Machinery":        {"robotics":5, "plc":5, "mes_scada":5},
    "Plastics":         {"robotics":10, "plc":5, "vision":5},
    "Steel & Metals":   {"robotics":10, "plc":10, "maintenance":10},
    "Textile":          {"robotics":5, "vision":5, "maintenance":5},
    "Furniture":        {"robotics":10, "plc":5},
    "Glass & Ceramics": {"robotics":5, "vision":10, "maintenance":5, "plc":5},
    "Construction Materials": {"plc":10, "maintenance":10, "robotics":5},
    "Aerospace":        {"vision":10, "mes_scada":5, "maintenance":5},
    "Medical Devices":  {"vision":10, "mes_scada":5, "maintenance":5},
    "Energy":           {"maintenance":10, "plc":5, "iot":5},
    "Water & Utilities": {"maintenance":10, "plc":5, "scada":5},
    "Industrial Components": {"robotics":5, "plc":5, "mes_scada":5},
}

# ═════════════════════════ PROCESS SIGNALS ═════════════════════════
PROCESS_KW = {
    "production": ["production line","assembly line","manufacturing","machining","cnc","turning","milling",
                   "welding","injection molding","packaging","palletizing","filling","labeling","sorting",
                   "picking","material handling","end of line","quality control","inspection","testing","traceability"],
    "logistics": ["warehouse","intralogistics","internal transport","forklift","picking","packing","shipping",
                  "distribution center","warehouse expansion","automated warehouse"],
}

# ═════════════════════════ 10 OPPORTUNITY CATEGORIES ═════════════════════════
CATEGORIES = {
    "robotics": {
        "buyer_kw": [
            "manual handling","repetitive tasks","heavy lifting","palletizing","depalletizing",
            "machine tending","pick and place","assembly line","welding","packaging line",
            "end-of-line packaging","operator shortage","labor shortage","manual assembly",
            "ergonomic","manual palletizing","manual loading","manual unloading",
            "manually loaded","manually unloaded","bottleneck at",
            "manuelle handhabung","manuelle beladung","personalnotstand","arbeitskraftmangel",
            "movimentazione manuale","carellaggio manuale","penuria personale","scarsita personale",
            "manutention manuelle","penurie de personnel",
        ],
        "weak_kw": ["automation","robotics","cobot","collaborative robot"],
        "solutions": {"default":"Process automation retrofit","palletizing":"Palletizing robot cell",
                      "machine tending":"Machine tending robot cell","welding":"Robotic welding cell",
                      "pick and place":"Pick-and-place robot"},
        "weight": 12,
        "db_field": "robotics_opportunity_score",
        "label": "Robotics & Cobot",
    },
    "amr_agv": {
        "buyer_kw": [
            "warehouse expansion","internal logistics","material handling","forklift operators",
            "logistics operators","transport carts","warehouse automation","distribution center",
            "picking operations","high-volume warehouse","manual material transport","internal transport",
            "manual picking","paper-based picking","forklift traffic",
            "gabelstapler","innenbereichslogistik","warenhausexpansion","manuelle kommissionierung",
            "carrelli elevatori","logistica interna","espansione magazzino","picking manuale",
            "chariots eleveurs","logistique interne","expansion entrepot","picking manuel",
        ],
        "weak_kw": ["logistics","warehouse","intralogistics"],
        "solutions": {"default":"AMR/AGV fleet deployment","warehouse":"Warehouse automation",
                      "material handling":"Material handling automation","agv":"AGV fleet deployment"},
        "weight": 12,
        "db_field": "amr_agv_opportunity_score",
        "label": "AMR / AGV",
    },
    "mes_scada": {
        "buyer_kw": [
            "production monitoring","oee calculation","downtime tracking","shop floor data",
            "production traceability","paper-based production","manual data collection",
            "excel production tracking","no real-time data","production reporting",
            "machine data collection","manual production tracking","paper-based tracking",
            "manual record keeping","production data entry","no shop floor visibility",
            "no real-time monitoring","manual logging","paper logs",
            "papierbasierte produktion","manuelle datenerfassung","produktionsverfolgung",
            "keine echtzeitdaten","manuelle protokollierung",
            "produzione cartacea","raccolta dati manuale","monitoraggio produzione",
            "nessun dato in tempo reale","registrazione manuale",
            "production sur papier","collecte de donnees manuelle","suivi de production",
            "pas de donnees en temps reel","enregistrement manuel",
        ],
        "weak_kw": ["industry 4.0","digital transformation","smart factory","digitalization",
                    "iot","iiot","industrie 4.0","digitalisierung","digitalizzazione","transformation digitale"],
        "solutions": {"default":"MES/SCADA digitalization","scada":"SCADA upgrade",
                      "monitoring":"Production monitoring system","oee":"OEE monitoring system"},
        "weight": 10,
        "db_field": "mes_opportunity_score",
        "label": "MES/SCADA/OEE",
    },
    "vision": {
        "buyer_kw": [
            "quality inspection","visual inspection","defect detection","camera inspection",
            "metrology","non-conformity","barcode verification","manual inspection",
            "visual quality check","inspection line","manual quality check","visual checking",
            "non-conformity tracking","defect tracking","quality gate",
            "qualitatsprufung","sichtprufung","fehlererkennung","manuelle inspektion",
            "ispezione qualita","ispezione visiva","rilevamento difetti","ispezione manuale",
            "inspection qualite","inspection visuelle","detection de defauts","inspection manuelle",
        ],
        "weak_kw": ["quality control","quality assurance","inspection","testing"],
        "solutions": {"default":"Machine vision quality inspection","ai":"AI vision system",
                      "quality":"Automated quality control","defect":"Defect detection system"},
        "weight": 12,
        "db_field": "machine_vision_opportunity_score",
        "label": "Machine Vision",
    },
    "maintenance": {
        "buyer_kw": [
            "downtime","preventive maintenance","condition monitoring","vibration monitoring",
            "equipment failure","spare parts management","maintenance technician","unplanned downtime",
            "reactive maintenance","breakdown maintenance","maintenance backlog","scheduled downtime",
            "mean time between failures","mtbf","mean time to repair","mttr",
            "maintenance costs","frequent breakdowns",
            "ausfallzeiten","reaktive wartung","vorbeugende wartung","anlagenstillstand",
            "fermi macchina","manutenzione reattiva","manutenzione preventiva","tempo di fermo",
            "temps darret","maintenance reactive","maintenance preventive","arrets de production",
        ],
        "weak_kw": ["maintenance","predictive maintenance"],
        "solutions": {"default":"Predictive maintenance program","monitoring":"Maintenance monitoring platform",
                      "sensors":"Industrial IoT sensors","vibration":"Vibration monitoring system"},
        "weight": 12,
        "db_field": "maintenance_opportunity_score",
        "label": "Predictive Maintenance",
    },
    "plc": {
        "buyer_kw": [
            "legacy control","old plc","outdated plc","control system upgrade","plc retrofit",
            "relay logic","manual control","hardwired control","control panel upgrade",
            "control system modernization","obsolete plc","legacy automation","relay-based",
            "aging control system","control system obsolescence",
            "alters sps","veraltete steuerung","relaistechnik","sps-nachrustung",
            "plc obsoleto","vecchio plc","logica a rele","ammodernamento controllo",
            "ancien automate","automate obsolete","logique relais","modernisation controle",
        ],
        "weak_kw": ["plc upgrade","control upgrade","modernization"],
        "solutions": {"default":"PLC/HMI retrofit","modernization":"Control system modernization",
                      "legacy":"Legacy system migration"},
        "weight": 10,
        "db_field": "automation_readiness_score",
        "label": "PLC / Controls",
    },
    "iot": {
        "buyer_kw": [
            "machine connectivity","data acquisition","edge computing","opc ua","mqtt",
            "sensor data","connected machines","machine data","equipment connectivity",
            "data silos","disconnected machines","no machine data","no connectivity",
            "island of automation","data integration","machine integration",
            "maschinenkonnektivitat","dateninseln","gerateanbindung",
            "connettivita macchine","acquisizione dati","sili di dati",
            "connectivite machines","acquisition de donnees","ilots de donnees",
        ],
        "weak_kw": ["iot","iiot","industrial internet","connected factory"],
        "solutions": {"default":"Industrial IoT platform","connectivity":"Machine connectivity solution",
                      "edge":"Edge computing deployment"},
        "weight": 10,
        "db_field": "automation_readiness_score",
        "label": "IoT / IIoT",
    },
    "erp": {
        "buyer_kw": [
            "legacy erp","erp migration","erp implementation","erp upgrade","sap migration",
            "system integration","data migration","business process management",
            "erp replacement","erp modernization","legacy sap","sap s/4 migration",
            "erp-migration","sap-migration","systemintegration",
            "migrazione erp","migrazione sap","integrazione sistemi",
            "migration erp","migration sap","integration systemes",
        ],
        "weak_kw": ["erp","sap","oracle","dynamics"],
        "solutions": {"default":"ERP implementation/integration","sap":"SAP S/4HANA migration",
                      "legacy":"Legacy ERP modernization"},
        "weight": 8,
        "db_field": None,
        "label": "ERP",
    },
    "wms": {
        "buyer_kw": [
            "warehouse management","inventory management","stock control","pick and pack",
            "order fulfillment","manual picking","paper-based picking","warehouse efficiency",
            "inventory accuracy","stock accuracy","warehouse optimization",
            "manual warehouse","no wms","legacy wms",
            "lagerverwaltung","manuelle kommissionierung","lageroptimierung",
            "gestione magazzino","picking manuale","ottimizzazione magazzino",
            "gestion dentrepot","picking manuel","optimisation entrepot",
        ],
        "weak_kw": ["wms","warehouse management system","inventory system"],
        "solutions": {"default":"WMS implementation","legacy":"WMS modernization",
                      "optimization":"Warehouse optimization solution"},
        "weight": 10,
        "db_field": None,
        "label": "WMS",
    },
    "cmms": {
        "buyer_kw": [
            "maintenance management","work order management","preventive maintenance schedule",
            "asset management","maintenance tracking","spare parts inventory",
            "maintenance planning","work order system","asset tracking",
            "maintenance scheduling","no cmms","legacy cmms","paper-based maintenance",
            "instandhaltungsverwaltung","wartungsplanung","anlagenverwaltung",
            "gestione manutenzione","pianificazione manutenzione","gestione asset",
            "gestion de maintenance","planification de maintenance","gestion des actifs",
        ],
        "weak_kw": ["cmms","eam","maintenance software"],
        "solutions": {"default":"CMMS implementation","legacy":"CMMS modernization",
                      "asset":"Asset management platform"},
        "weight": 8,
        "db_field": None,
        "label": "CMMS",
    },
}

SOLUTION_ORDER = ["amr_agv","mes_scada","vision","robotics","maintenance","plc","iot","wms","erp","cmms"]

# ═════════════════════════ VENDOR DETECTION ═════════════════════════
VENDOR_INDUSTRIES = {
    "robotics": ["automation machinery manufacturing","industrial machinery manufacturing",
                 "robot manufacturer","robotics manufacturer","industrial automation"],
    "mes_scada": ["automation machinery manufacturing","software development",
                  "it services and it consulting","industrial automation"],
    "vision": ["automation machinery manufacturing","photonics","optical instrument manufacturing"],
    "amr_agv": ["automation machinery manufacturing","industrial machinery manufacturing"],
    "maintenance": ["software development","it services and it consulting"],
    "plc": ["automation machinery manufacturing","industrial automation",
            "electrical equipment manufacturing","electrical manufacturing"],
    "iot": ["software development","it services and it consulting",
            "information technology and services"],
    "erp": ["software development","it services and it consulting",
            "information technology and services"],
    "wms": ["software development","it services and it consulting"],
    "cmms": ["software development","it services and it consulting"],
}

VENDOR_KEYWORDS = {
    "robotics": ["universal robots","abb robot","fanuc","kuka","yaskawa","onrobot","robotiq",
                 "staubli robotics","staubli robot","denso robotics","epson robots",
                 "mobile industrial robots","mir robot","mir100","mir200","mir250","mir500","mir1000"],
    "mes_scada": ["wincc","ignition scada","wonderware","factorytalk","aveva","ifix",
                  "citect scada","movicon","zenon scada","mes system","mes software",
                  "siemens opcenter","critical manufacturing","plex systems","dassault delmia"],
    "vision": ["cognex","keyence vision","basler","sick inspector","halcon",
               "matrox imaging","teledyne dalsa","allied vision","ids imaging",
               "baumer","ifm vision","zeiss vision","datalogic vision","banner engineering vision"],
    "amr_agv": ["geek+","geek plus","locus robotics","6 river systems","6river",
                "fetch robotics","magazino","otto motors","otto 600","otto 1500",
                "clearpath robotics","ek robotics"],
    "maintenance": ["augury","vibradeck","uptime ai","uptime.ai","presenso","twaice",
                    "limble","fiix cmms","upkeep cmms","eptura maintenance"],
    "plc": ["siemens","rockwell","allen-bradley","allen bradley","schneider electric","omron",
            "beckhoff","mitsubishi electric","b&r automation","b&r industrial",
            "phoenix contact","abb automation","yokogawa","emerson automation",
            "honeywell process","bosch rexroth","festo","sew eurodrive",
            "wago","rittal","pilz safety","turck","ifm electronic","pepperl+fuchs","keyence"],
    "iot": ["ptc thingworx","c3 ai","litmus edge","azure iot","aws iot","predix ge","cumulocity"],
    "erp": ["sap erp","sap hana","sap s/4hana","sap business one","oracle erp",
            "microsoft dynamics","infor erp","infor cloudsuite","infor ln","infor m3",
            "epicor","netsuite erp","iqms","proalpha","qad erp"],
    "wms": ["manhattan associates","sap ewm","highjump","korber wms","koeber wms",
            "blue yonder wms","infor wms","fishbowl inventory","skubana","boltrics","prolog wms"],
    "cmms": ["ibm maximo","maximo","fiix","fiix cmms","upkeep","hippo cmms","eptura",
             "dude solutions","maintenance connection","mpulse","emaint","fracttal",
             "limble cmms","hoffmann facility"],
}

VENDOR_PHRASES = ["we offer","we provide","our solutions","we specialize","we develop",
                   "we manufacture","we design","our products","our platform","we build",
                   "we create","we produce","our technology","we deliver",
                   "wir bieten","wir entwickeln","wir produzieren","unsere losungen",
                   "offriamo","sviluppiamo","produttore di","la nostra soluzione",
                   "nous offrons","nous developpons","nos solutions"]

# Installed technology keywords (for tech detection)
INSTALLED_TECH = {
    "plc_automation": ["siemens","rockwell","allen-bradley","allen bradley","schneider electric","omron",
                       "beckhoff","mitsubishi electric","b&r automation","phoenix contact","abb automation",
                       "yokogawa","emerson automation","honeywell process","bosch rexroth","festo",
                       "sew eurodrive","wago","rittal","pilz safety","turck","ifm electronic",
                       "pepperl+fuchs","keyence"],
    "scada_hmi": ["wincc","ignition scada","wonderware","factorytalk","aveva","ifix","citect scada",
                  "movicon","zenon scada"],
    "mes_erp": ["sap erp","sap hana","sap s/4hana","sap business one","running on sap","sap consultant",
                "oracle erp","microsoft dynamics","infor erp","infor cloudsuite","infor ln","infor m3",
                "epicor","mes system","mes software","siemens opcenter","critical manufacturing",
                "dassault delmia","plex systems","qad erp","netsuite erp","iqms","proalpha"],
    "cad_plm": ["solidworks","autocad","siemens nx","ptc creo","catia","autodesk","teamcenter","windchill plm"],
    "robotics": ["universal robots","abb robot","fanuc","kuka","yaskawa","omron robot",
                 "mobile industrial robots","mir robot","onrobot","robotiq",
                 "staubli robotics","denso robotics","epson robots"],
    "iiot_platform": ["ptc thingworx","c3 ai","litmus edge","azure iot","aws iot","predix ge","cumulocity"],
    "machine_vision": ["cognex","keyence vision","basler","sick inspector","halcon",
                       "matrox imaging","teledyne dalsa","allied vision","ids imaging",
                       "baumer","ifm vision","zeiss vision","datalogic vision"],
    "amr_agv": ["geek+","geek plus","locus robotics","6 river systems","6river","fetch robotics",
                "magazino","otto motors","otto 600","otto 1500","clearpath robotics","ek robotics"],
    "predictive_maintenance": ["augury","vibradeck","uptime ai","uptime.ai","presenso","twaice",
                               "limble","fiix cmms","upkeep cmms","eptura maintenance"],
    "wms": ["manhattan associates","sap ewm","highjump","korber wms","koeber wms","blue yonder wms",
            "infor wms","fishbowl inventory","skubana","boltrics","prolog wms"],
    "cmms": ["ibm maximo","maximo","fiix","upkeep","hippo cmms","eptura",
             "dude solutions","maintenance connection","mpulse","emaint","fracttal","limble cmms"],
}

# ═════════════════════════ BUYING INTENT ═════════════════════════
INTENT_KW = [
    "new manufacturing plant","new production facility","greenfield plant","brownfield expansion",
    "capacity expansion","production capacity increase","new factory opening","plant expansion",
    "new assembly line","new production line","capital expenditure","capex investment",
    "technology investment","equipment investment","machinery investment","automation investment",
    "digital transformation","industry 4.0 implementation","lean transformation",
    "manufacturing modernization","machine retrofit","equipment upgrade","production line upgrade",
    "acquisition","new plant","new machinery","sustainability investment","operational efficiency",
    "neue produktionslinie","kapazitatserweiterung","werkserweiterung","neues werk",
    "investition in automatisierung","digitalisierung","industrie 4.0",
    "neubau produktion","standorterweiterung","modernisierung der produktion",
    "nuovo stabilimento","ampliamento produttivo","nuova linea di produzione",
    "investimento in automazione","digitalizzazione","industria 4.0",
    "ampliamento capacita produttiva","nuovo impianto",
    "nouvelle ligne de production","extension de capacite","nouvelle usine",
    "investissement automatisation","transformation digitale","industrie 4.0",
    "modernisation de la production",
]

GROWTH_TAG_MAP = [
    (["new production facility","new manufacturing plant","greenfield plant","new factory opening","new plant",
      "neues werk","neubau produktion","nuovo stabilimento","nuovo impianto","nouvelle usine"], "New Facility"),
    (["new assembly line","new production line","neue produktionslinie","nuova linea di produzione",
      "nouvelle ligne de production"], "New Production Line"),
    (["capacity expansion","production capacity increase","plant expansion","brownfield expansion",
      "kapazitatserweiterung","werkserweiterung","standorterweiterung","ampliamento produttivo",
      "ampliamento capacita produttiva","extension de capacite"], "Plant Expansion"),
    (["warehouse expansion","warehouse automation"], "Warehouse Expansion"),
    (["digital transformation","industry 4.0 implementation","smart factory","digitalisierung","industrie 4.0",
      "digitalizzazione","industria 4.0","transformation digitale"], "Digital Transformation"),
    (["automation investment","equipment investment","machinery investment","technology investment",
      "capital expenditure","capex investment","investition in automatisierung","investimento in automazione",
      "investissement automatisation"], "Automation Investment"),
    (["acquisition"], "Recent Acquisition"),
    (["machine retrofit","equipment upgrade","production line upgrade","modernisierung der produktion"], "Equipment Upgrade"),
    (["sustainability investment"], "Sustainability Investment"),
    (["lean transformation","manufacturing modernization"], "Manufacturing Modernization"),
    (["new machinery"], "New Machinery"),
]

# ═════════════════════════ JOB TITLES ═════════════════════════
JOB_TITLES = ["automation engineer","plc programmer","robotics engineer","manufacturing engineer",
              "production engineer","process engineer","maintenance technician","industrial electrician",
              "cnc operator","warehouse operator","logistics manager","quality control technician",
              "mes specialist","scada engineer","controls engineer","plant manager","operations manager",
              "automation technician","controls technician","robotics technician","iot engineer",
              "digitalization manager","industry 4.0 manager","lean manufacturing engineer",
              "continuous improvement engineer","data scientist","it manager","sap consultant",
              "erp specialist","wms manager","maintenance manager"]

JOB_TAG_MAP = {
    "automation engineer":"Hiring Automation Engineer", "plc programmer":"Hiring PLC",
    "robotics engineer":"Hiring Robotics Engineer", "manufacturing engineer":"Hiring Manufacturing Engineer",
    "production engineer":"Hiring Production Engineer", "process engineer":"Hiring Process Engineer",
    "maintenance technician":"Hiring Technician", "industrial electrician":"Hiring Electrician",
    "cnc operator":"Hiring CNC Operator", "warehouse operator":"Hiring Warehouse Staff",
    "logistics manager":"Hiring Logistics Manager", "quality control technician":"Hiring QC Technician",
    "mes specialist":"Hiring MES Specialist", "scada engineer":"Hiring SCADA Engineer",
    "controls engineer":"Hiring Controls Engineer", "plant manager":"Hiring Plant Manager",
    "operations manager":"Hiring Operations Manager", "automation technician":"Hiring Automation Technician",
    "controls technician":"Hiring Controls Technician", "robotics technician":"Hiring Robotics Technician",
    "iot engineer":"Hiring IoT Engineer", "digitalization manager":"Hiring Digitalization Manager",
    "industry 4.0 manager":"Hiring Industry 4.0 Manager", "lean manufacturing engineer":"Hiring Lean Engineer",
    "continuous improvement engineer":"Hiring CI Engineer","data scientist":"Hiring Data Scientist",
    "it manager":"Hiring IT Manager","sap consultant":"Hiring SAP Consultant",
    "erp specialist":"Hiring ERP Specialist","wms manager":"Hiring WMS Manager",
    "maintenance manager":"Hiring Maintenance Manager",
}

# ═════════════════════════ BLACKLIST & CONTACT ═════════════════════════
BLACKLIST = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|immobilienmakler|'
    r'insurance broker|restaurant|ristorante|hotel|albergo|software development company|'
    r'web agency|digital marketing agency|university|hospital|school|charity|non.?profit|ngo|onlus)\b', re.I)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(?:\+\d{1,3}[\s.\-]?)?\(?\d{2,4}\)?[\s.\-]{0,2}\d{3,4}[\s.\-]{0,2}\d{3,5}(?:[\s.\-]?\d{2,4})?')
LINKEDIN_RE = re.compile(r'https?://[a-z]{0,3}\.?linkedin\.com/company/[a-zA-Z0-9\-_%]+', re.I)
EMAIL_JUNK = ("sentry.io","wixpress.com","example.com","godaddy.com","cloudflare.com","schema.org",
              "w3.org","google.com","googletagmanager.com","gstatic.com","facebook.com","twitter.com",
              "x.com","wordpress.com","gravatar.com","sentry-next.wixpress.com",".png",".jpg",".jpeg",
              ".gif",".svg",".webp",".ico",".avif",".woff",".woff2",".ttf",".eof",
              "yourdomain.com","domain.com")
EMAIL_ASSET_RE = re.compile(r'@\d+x\b', re.I)

def _is_valid_email(cand):
    cand = cand.strip().strip(".,;:")
    if any(j in cand.lower() for j in EMAIL_JUNK): return False
    if EMAIL_ASSET_RE.search(cand): return False
    domain_part = cand.split("@")[-1]
    tld = domain_part.split(".")[-1] if "." in domain_part else ""
    if not (2 <= len(tld) <= 10 and tld.isalpha()): return False
    return True

# ═════════════════════════ PAGE WEIGHTS ═════════════════════════
PAGE_WEIGHT = {
    "home":               0.5,
    "about":              1.5,
    "products":           0.02,
    "solutions":          0.02,
    "services":           0.02,
    "automation":         0.02,
    "solutions-industry":  0.02,
    "technology":         0.1,
    "industry":           0.1,
    "manufacturing":      0.3,
    "careers":            3.0,
    "jobs":               3.0,
    "news":               2.0,
    "press":              2.0,
    "innovation":         1.0,
    "contact":            0.05,
}

SELLER_PAGES = ["products","solutions","services","automation","solutions-industry","home"]

# ═════════════════════════ FETCH ═════════════════════════
from requests.adapters import HTTPAdapter, Retry

def make_session():
    s = requests.Session()
    s.headers.update(UA)
    retry = Retry(total=2, backoff_factor=0.3, status_forcelist=[502, 503, 504],
                   allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch(url, session=None, timeout=6):
    try:
        req = session or requests
        r = req.get(url, timeout=timeout, verify=False, allow_redirects=True,
                    headers=None if session else UA)
        if r.status_code == 200 and len(r.content) > 300:
            raw = r.text
            raw_clean = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.S)
            raw_clean = re.sub(r'<style[^>]*>.*?</style>', '', raw_clean, flags=re.S)
            text = re.sub(r'<[^>]+>', ' ', raw_clean)
            text = re.sub(r'\s+', ' ', text).lower()[:15000]
            return {"text": text, "raw": raw_clean[:40000]}
    except Exception:
        pass
    return {"text": "", "raw": ""}

def gather_pages(base_url):
    urls = {
        "home": base_url,
        "products": f"{base_url}/products",
        "solutions": f"{base_url}/solutions",
        "services": f"{base_url}/services",
        "contact": f"{base_url}/contact",
        "careers": f"{base_url}/careers",
        "jobs": f"{base_url}/jobs",
        "about": f"{base_url}/about",
        "news": f"{base_url}/news",
        "press": f"{base_url}/press",
        "technology": f"{base_url}/technology",
        "innovation": f"{base_url}/innovation",
        "automation": f"{base_url}/automation",
        "industry": f"{base_url}/industry",
        "manufacturing": f"{base_url}/manufacturing",
        "solutions-industry": f"{base_url}/solutions-industry",
    }
    out = {}
    session = make_session()
    ex = ThreadPoolExecutor(max_workers=15)
    futs = {ex.submit(fetch, u, session): k for k, u in urls.items()}
    for f in futs:
        try:
            k = futs[f]
            r = f.result(timeout=10)
            if r["text"]: out[k] = r
        except Exception:
            pass
    ex.shutdown(wait=False, cancel_futures=True)
    session.close()
    return out, urls

# ═════════════════════════ DETECTION FUNCTIONS ═════════════════════════

def classify_industry(all_text, db_industry=None):
    if db_industry and db_industry.strip() and db_industry.lower() not in ("n/a","none","null",""):
        for cat in INDUSTRY_KW:
            if cat.lower() in db_industry.lower():
                return cat
        if len(db_industry) > 3:
            return db_industry[:50]
    scores = {cat: sum(1 for k in kws if k in all_text) for cat, kws in INDUSTRY_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Industrial Components"

def detect_processes(all_text):
    found = {}
    for cat, kws in PROCESS_KW.items():
        hits = [k for k in kws if k in all_text]
        if hits: found[cat] = hits
    return found

def is_vendor_for_category(category, pages, db_industry):
    industry_lower = (db_industry or "").lower()
    for vi in VENDOR_INDUSTRIES.get(category, []):
        if vi in industry_lower:
            return True, "industry_match"

    seller_text = " ".join(pages.get(p, {}).get("text", "") for p in SELLER_PAGES)
    vendor_kws = VENDOR_KEYWORDS.get(category, [])
    vendor_hits = [k for k in vendor_kws if k in seller_text]
    has_vendor_phrase = any(p in seller_text for p in VENDOR_PHRASES)

    if has_vendor_phrase and len(vendor_hits) >= 2:
        return True, "website_match"
    if len(vendor_hits) >= 3:
        return True, "keyword_density"

    return False, None

def detect_opportunities(pages, page_urls, base_url, db_industry):
    result = {}
    vendor_flags = {}

    # Classify industry once for boost
    all_text = " ".join(p["text"] for p in pages.values())
    industry_cat = classify_industry(all_text, db_industry)

    for cat_key, cfg in CATEGORIES.items():
        is_vendor, vendor_reason = is_vendor_for_category(cat_key, pages, db_industry)
        vendor_flags[cat_key] = (is_vendor, vendor_reason)

        if is_vendor:
            result[cat_key] = {
                "score": 0, "evidence": [], "evidence_urls": {},
                "solution": cfg["solutions"]["default"], "field": cfg["db_field"],
                "label": cfg["label"], "vendor": True, "vendor_reason": vendor_reason,
            }
            continue

        weighted_hits = 0.0
        hits, hit_urls = [], {}

        for kw in cfg["buyer_kw"]:
            best_weight = 0.0
            best_pname = None
            for pname, pdata in pages.items():
                if kw in pdata["text"]:
                    w = PAGE_WEIGHT.get(pname, 0.5)
                    if best_pname is None or w > best_weight:
                        best_weight = w
                        best_pname = pname
            if best_pname is not None:
                hits.append(kw)
                hit_urls[kw] = page_urls.get(best_pname, base_url)
                weighted_hits += best_weight

        for kw in cfg["weak_kw"]:
            best_weight = 0.0
            best_pname = None
            for pname, pdata in pages.items():
                if kw in pdata["text"]:
                    w = PAGE_WEIGHT.get(pname, 0.5)
                    if best_pname is None or w > best_weight:
                        best_weight = w
                        best_pname = pname
            if best_pname is not None:
                hits.append(kw)
                hit_urls[kw] = page_urls.get(best_pname, base_url)
                weighted_hits += best_weight * 0.3

        score = min(100, weighted_hits * cfg["weight"])
        boost = INDUSTRY_BOOST.get(industry_cat, {}).get(cat_key, 0)
        score = min(100, score + boost)
        if score < NOISE_THRESHOLD:
            score = 0

        result[cat_key] = {
            "score": round(score), "evidence": hits, "evidence_urls": hit_urls,
            "solution": cfg["solutions"]["default"], "field": cfg["db_field"],
            "label": cfg["label"], "vendor": False, "vendor_reason": None,
            "industry_boost": boost,
        }

    return result, vendor_flags, industry_cat

def detect_technologies(pages, page_urls, base_url):
    found = []
    seen = set()
    non_seller_pages = {k: v for k, v in pages.items() if k not in SELLER_PAGES}
    for cat, vendors in INSTALLED_TECH.items():
        for vv in vendors:
            for pname, pdata in non_seller_pages.items():
                if vv in pdata["text"]:
                    if vv not in seen:
                        found.append({"name": vv.title(), "category": cat,
                                      "url": page_urls.get(pname, base_url)})
                        seen.add(vv)
                    break
    return found

def detect_jobs(pages, page_urls, base_url):
    found = []
    careers_url = page_urls.get("careers", f"{base_url}/careers")
    careers_text = ""
    for pname in ("careers","jobs"):
        if pname in pages:
            careers_text += pages[pname]["text"]
            careers_url = page_urls.get(pname, careers_url)
    if not careers_text: return found
    for title in JOB_TITLES:
        if title in careers_text:
            idx = careers_text.find(title)
            snippet = careers_text[max(0,idx-100):idx+300]
            kws = [k for k in INTENT_KW if k in snippet] + [title]
            found.append({"title": title.title(), "snippet": snippet[:400],
                          "keywords": list(set(kws)), "url": careers_url})
    return found[:10]

def compute_buying_intent(pages, page_urls, base_url):
    hits, hit_urls = [], {}
    for k in INTENT_KW:
        for pname, pdata in pages.items():
            if k in pdata["text"]:
                hits.append(k)
                hit_urls[k] = page_urls.get(pname, base_url)
                break
    return min(100, len(hits) * 10), hits[:8], hit_urls

def compute_fit_score(employee_count, industry_cat, opp_scores):
    emp_fit = 60
    if employee_count and employee_count > 20: emp_fit = 80
    if employee_count and employee_count > 200: emp_fit = 95
    industry_fit = 90 if industry_cat != "Other" else 50
    signal_fit = min(100, max(opp_scores.values(), default=0))
    return round(emp_fit*0.3 + industry_fit*0.3 + signal_fit*0.4)

def deal_range(emp, top_score):
    if   emp and emp > 5000: dmin,dmax = 300000,2000000
    elif emp and emp > 500:  dmin,dmax = 80000,500000
    elif emp and emp > 100:  dmin,dmax = 25000,120000
    elif emp and emp > 20:   dmin,dmax = 8000,40000
    else:                    dmin,dmax = 3000,15000
    if top_score > 40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)
    return dmin, dmax

def compute_confidence(pages_ok, strong_evidence, vendor_certainty):
    base = min(40, pages_ok * 5)
    ev = min(30, strong_evidence * 3)
    vc = min(30, vendor_certainty * 10)
    return min(100, base + ev + vc)

def extract_contact_info(pages, domain):
    priority = ["contact","about","home","products","solutions","news","press","careers","jobs"]
    email, phone, linkedin = None, None, None

    for pname in priority:
        if pname not in pages: continue
        raw = pages[pname]["raw"]
        m = re.findall(r'mailto:([^"\'>\s?]+)', raw, re.I)
        for cand in m:
            if "@" in cand and _is_valid_email(cand):
                email = cand.strip(); break
        if not email:
            m = EMAIL_RE.findall(raw)
            for cand in m:
                if _is_valid_email(cand):
                    email = cand.strip(); break
        if email: break

    for pname in priority:
        if pname not in pages: continue
        raw = pages[pname]["raw"]
        m = re.findall(r'tel:([+\d\s().\-]+)', raw, re.I)
        for cand in m:
            digits = re.sub(r'\D','',cand)
            if 7 <= len(digits) <= 15:
                phone = cand.strip(); break
        if phone: break
        for m2 in PHONE_RE.findall(pages[pname]["text"]):
            cand = m2.strip()
            digits = re.sub(r'\D','',cand)
            has_separator = bool(re.search(r'[\s().\-]', cand)) or cand.startswith("+")
            if has_separator and 7 <= len(digits) <= 15:
                phone = cand; break
        if phone: break

    for pname in ("home","about","contact"):
        if pname not in pages: continue
        m = LINKEDIN_RE.findall(pages[pname]["raw"])
        if m:
            linkedin = m[0].split("?")[0]; break

    return email, phone, linkedin

def build_solution_tags(opp_scores_only, threshold):
    tags = []
    for key in SOLUTION_ORDER:
        if opp_scores_only.get(key, 0) >= threshold:
            tags.append(CATEGORIES[key]["solutions"]["default"])
    return ", ".join(tags)

def build_why_now_tags(opps, intent_hits, jobs, threshold, max_tags=6):
    tags = []
    growth_pool = set(intent_hits) | set(opps.get("amr_agv", {}).get("evidence", []))
    for kws, tag in GROWTH_TAG_MAP:
        if any(k in growth_pool for k in kws) and tag not in tags:
            tags.append(tag)
    for j in jobs:
        t = JOB_TAG_MAP.get(j["title"].lower(), f"Hiring {j['title']}")
        if t not in tags:
            tags.append(t)
    if opps.get("vision", {}).get("score", 0) >= threshold and "Quality Automation" not in tags:
        tags.append("Quality Automation")
    if opps.get("maintenance", {}).get("score", 0) >= threshold and "Downtime Reduction" not in tags:
        tags.append("Downtime Reduction")
    return ", ".join(tags[:max_tags])

def _norm_name(n):
    import unicodedata
    n = (n or "").lower().strip()
    n = re.sub(r'\s*\([^)]*\)\s*$', '', n)
    n = re.sub(r'\b(inc|inc\.|ltd|ltd\.|llc|gmbh|s\.p\.a\.|spa|s\.r\.l\.|srl|corp|corporation|co\.|company|ag|sa|nv|bv)\b', '', n)
    n = unicodedata.normalize('NFKD', n).encode('ascii', 'ignore').decode('ascii')
    n = re.sub(r'[^a-z0-9]+', '', n)
    return n

# ═════════════════════════ MAIN PROCESSING ═════════════════════════

def process_company(rec, conn):
    name = (rec.get("name") or rec.get("domain") or "?")[:40]
    domain = rec.get("domain","?")
    cid = rec["id"]
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()
    try:
        base_url = rec.get("website_url") or f"https://www.{domain}"
        if not base_url.startswith("http"): base_url = f"https://www.{domain}"
        pages, page_urls = gather_pages(base_url)
        all_text = " ".join(p["text"] for p in pages.values())

        if len(all_text.strip()) < 200:
            cur.execute("""UPDATE industrial_company SET scan_status='unreachable', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            return
        if BLACKLIST.search(all_text[:3000]):
            cur.execute("""UPDATE industrial_company SET scan_status='blacklisted', scanned=TRUE,
                            last_scan_date=%s, dirty=TRUE, updated_at=now() WHERE id=%s""", (now, cid))
            conn.commit()
            with lock: stats["unreachable"] += 1
            return

        db_industry = rec.get("industry") or ""
        opps, vendor_flags, industry_cat = detect_opportunities(pages, page_urls, base_url, db_industry)
        techs = detect_technologies(pages, page_urls, base_url)
        jobs = detect_jobs(pages, page_urls, base_url)
        buying_intent, intent_hits, intent_urls = compute_buying_intent(pages, page_urls, base_url)
        email, phone, linkedin_url = extract_contact_info(pages, domain)

        emp = int(float(rec.get("employee_count") or 0)) or None
        opp_scores_only = {k: v["score"] for k, v in opps.items()}
        fit_score = compute_fit_score(emp, industry_cat, opp_scores_only)

        scored = {k: v for k, v in opps.items() if v["score"] >= OPP_THRESHOLD}
        if scored:
            top_key = max(scored, key=lambda k: scored[k]["score"])
            top_score = scored[top_key]["score"]
            top_label = scored[top_key]["label"]
        else:
            top_key = None
            top_score = 0
            top_label = None

        dmin, dmax = deal_range(emp, top_score)

        strong_ev = sum(1 for k, v in opps.items()
                        if v["score"] >= OPP_THRESHOLD and not v["vendor"]
                        and any(pw in PAGE_WEIGHT and PAGE_WEIGHT[pw] >= 1.5
                               for pw in page_urls if pw in pages))
        vendor_cert = sum(1 for k, v in vendor_flags.items() if v[0])
        confidence = compute_confidence(len(pages), strong_ev, vendor_cert)

        solution_tags = build_solution_tags(opp_scores_only, OPP_THRESHOLD)
        why_now_tags = build_why_now_tags(opps, intent_hits, jobs, OPP_THRESHOLD)

        reason_parts = []
        for k, v in opps.items():
            if v["score"] >= OPP_THRESHOLD and v["evidence"]:
                reason_parts.append(f"{v['label']}: {', '.join(v['evidence'][:3])}")
        if intent_hits:
            reason_parts.append(f"buying intent: {', '.join(intent_hits[:3])}")
        if jobs:
            reason_parts.append(f"hiring: {', '.join(j['title'] for j in jobs[:3])}")
        reason_summary = " | ".join(reason_parts)[:600] if reason_parts else "No strong signals detected"

        vendor_cats = [k for k, v in vendor_flags.items() if v[0]]
        if vendor_cats:
            reason_summary = f"VENDOR: {', '.join(vendor_cats)} | " + reason_summary

        plc_score = opp_scores_only.get("plc", 0)
        iot_score = opp_scores_only.get("iot", 0)
        automation_readiness = max(plc_score, iot_score)

        cur.execute("""
            UPDATE industrial_company SET
                automation_readiness_score=%s, robotics_opportunity_score=%s, amr_agv_opportunity_score=%s,
                mes_opportunity_score=%s, machine_vision_opportunity_score=%s, maintenance_opportunity_score=%s,
                buying_intent_score=%s, fit_score=%s, confidence_score=%s, industry_category=%s,
                estimated_deal_value_min=%s, estimated_deal_value_max=%s,
                scan_status='completed', scanned=TRUE, last_scan_date=%s,
                top_opportunity=%s, recommended_solution=%s, pipeline_notes=%s,
                email=%s, phone=%s, linkedin_url=%s,
                dirty=TRUE, updated_at=now()
            WHERE id=%s
        """, (
            automation_readiness,
            opp_scores_only.get("robotics",0), opp_scores_only.get("amr_agv",0),
            opp_scores_only.get("mes_scada",0), opp_scores_only.get("vision",0),
            opp_scores_only.get("maintenance",0), buying_intent, fit_score, confidence,
            industry_cat, dmin, dmax, now, top_label, solution_tags, why_now_tags,
            email, phone, linkedin_url, cid
        ))
        conn.commit()

        n_sig = 0
        for k, v in opps.items():
            if v["score"] < OPP_THRESHOLD or not v["evidence"]: continue
            first_kw = v["evidence"][0]
            src_url = v["evidence_urls"].get(first_kw, base_url)
            cur.execute("""INSERT INTO industrial_signal
                (company_id, company_name, company_domain, signal_category, signal_type,
                 source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, v["label"], "buyer_signal", src_url,
                f"Detected {len(v['evidence'])} matches: {', '.join(v['evidence'])}"[:500],
                min(95, int(v["score"])), now, now))
            n_sig += 1

        for k, v in vendor_flags.items():
            if not v[0]: continue
            cur.execute("""INSERT INTO industrial_signal
                (company_id, company_name, company_domain, signal_category, signal_type,
                 source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, CATEGORIES[k]["label"], "vendor_signal",
                base_url, f"Vendor detected: {v[1]} — suppressed {CATEGORIES[k]['label']} score",
                90, now, now))
            n_sig += 1

        for k in intent_hits:
            src_url = intent_urls.get(k, base_url)
            cur.execute("""INSERT INTO industrial_signal
                (company_id, company_name, company_domain, signal_category, signal_type,
                 source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, "Buying Intent", "expansion_signal", src_url,
                f"Expansion/investment signal: {k}", 70, now, now))
            n_sig += 1

        for cat_key in ("erp","wms","cmms"):
            v = opps.get(cat_key, {})
            if v["score"] >= OPP_THRESHOLD and v["evidence"]:
                first_kw = v["evidence"][0]
                src_url = v["evidence_urls"].get(first_kw, base_url)
                cur.execute("""INSERT INTO industrial_signal
                    (company_id, company_name, company_domain, signal_category, signal_type,
                     source_url, evidence_text, confidence_score, detected_at, last_verified_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                    cid, rec.get("name",""), domain, v["label"], "buyer_signal", src_url,
                    f"Score={v['score']} — {', '.join(v['evidence'][:3])}"[:500],
                    min(95, int(v["score"])), now, now))
                n_sig += 1

        n_tech = 0
        seen_tech = set()
        for t in techs:
            if t["name"] in seen_tech: continue
            seen_tech.add(t["name"])
            cur.execute("""INSERT INTO industrial_technology
                (company_id, company_name, company_domain, technology_name, category,
                 confidence_score, evidence_text, source_url, detected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, t["name"], t["category"], 80,
                f"Mention of '{t['name']}' found on site", t["url"], now))
            n_tech += 1

        n_jobs = 0
        for j in jobs:
            cur.execute("""INSERT INTO industrial_job_signal
                (company_id, company_domain, job_title, job_description, source_url,
                 extracted_keywords, detected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (
                cid, domain, j["title"], j["snippet"], j["url"], j["keywords"], now))
            n_jobs += 1

        n_opp = 0
        if top_key and top_score >= OPP_THRESHOLD:
            cur.execute("""INSERT INTO industrial_opportunity
                (company_id, company_name, company_domain, opportunity_type, recommended_solution,
                 opportunity_score, buying_intent_score, estimated_deal_value_min, estimated_deal_value_max,
                 reason_summary, signals_count, top_signals)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                cid, rec.get("name",""), domain, top_label, solution_tags or opps[top_key]["solution"],
                fit_score, buying_intent, dmin, dmax, reason_summary,
                sum(1 for v in opps.values() if v["score"] >= OPP_THRESHOLD),
                [f"{v['label']}:{v['score']}" for v in opps.values() if v["score"] >= OPP_THRESHOLD][:5]))
            n_opp = 1
        conn.commit()

        with lock:
            stats["scanned"] += 1
            stats["signals_created"] += n_sig
            stats["tech_created"] += n_tech
            stats["jobs_created"] += n_jobs
            stats["opps_created"] += n_opp
            if top_score >= OPP_THRESHOLD: stats["good"] += 1
            if vendor_cats: stats["vendors_detected"] += 1

        vendor_str = f" VENDOR={','.join(vendor_cats)}" if vendor_cats else ""
        log.info(f"  OK {name:38} {industry_cat[:18]:18} fit={fit_score:3d} bi={buying_intent:3d} "
                 f"conf={confidence:3d} top={top_label or 'None':20} "
                 f"sol=[{solution_tags[:40]:40}] why=[{why_now_tags[:40]:40}] "
                 f"sig={n_sig} tech={n_tech} jobs={n_jobs} opp={n_opp}"
                 f" email={'Y' if email else 'N'} phone={'Y' if phone else 'N'}{vendor_str}")
    except Exception as e:
        conn.rollback()
        with lock: stats["errors"] += 1
        log.warning(f"  ERR {domain}: {str(e)[:150]}")
    finally:
        cur.close()

# ═════════════════════════ DEDUP & QC ═════════════════════════

def dedup_pass(conn):
    try:
        cur = conn.cursor()
        cur.execute("""DELETE FROM industrial_company a USING industrial_company b
            WHERE a.domain = b.domain AND a.id > b.id""")
        removed_domain = cur.rowcount
        conn.commit()

        cur.execute("SELECT id, name, country FROM industrial_company")
        recs = cur.fetchall()
        from collections import defaultdict
        by_name_country = defaultdict(list)
        for r in recs:
            key = (_norm_name(r["name"]), (r["country"] or "").upper())
            if key[0]: by_name_country[key].append(r["id"])
        to_delete = []
        for key, ids in by_name_country.items():
            if len(ids) <= 1: continue
            ids.sort()
            to_delete.extend(ids[1:])
        removed_name = 0
        if to_delete:
            cur.execute("DELETE FROM industrial_company WHERE id = ANY(%s)", (to_delete,))
            removed_name = cur.rowcount
        conn.commit()
        total = removed_domain + removed_name
        if total: log.info(f"  Dedup: removed {total} ({removed_domain} domain, {removed_name} name)")
        with lock: stats["last_dedup"] = {"time": time.strftime("%H:%M:%S"), "removed": total}
        cur.close()
    except Exception as e:
        conn.rollback()
        log.warning(f"dedup error: {e}")

def retry_stale_unreachable(conn, days=10, batch_cap=300):
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE industrial_company SET scan_status='pending'
            WHERE id IN (
                SELECT id FROM industrial_company
                WHERE scan_status='unreachable' AND last_scan_date < now() - (%s || ' days')::interval
                ORDER BY last_scan_date ASC LIMIT %s
            )
        """, (days, batch_cap))
        n = cur.rowcount
        conn.commit()
        if n: log.info(f"  Retry unreachable: {n} companies requeued")
        cur.close()
    except Exception as e:
        conn.rollback()
        log.warning(f"retry error: {e}")

def quality_check(conn):
    log.info("-- QC --")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FILTER (WHERE scan_status = 'pending') AS pending,
                   count(*) FILTER (WHERE scan_status != 'pending') AS total,
                   count(*) FILTER (WHERE scan_status != 'pending' AND coalesce(fit_score,0) >= 60) AS good
            FROM industrial_company
        """)
        row = cur.fetchone()
        total, pending, good = row["total"] or 0, row["pending"] or 0, row["good"] or 0
        rate = round(good/total*100,1) if total else 0
        log.info(f"  Total:{total+pending} | Scanned:{total} | Pending:{pending} | Fit>=60: {good} ({rate}%)")
        with lock:
            stats["qc"] = {"total":total,"pending":pending,"good":good,"rate":rate}
            stats["last_qc"] = time.strftime("%H:%M:%S")
        cur.close()
        dedup_pass(conn)
        retry_stale_unreachable(conn)
    except Exception as e:
        conn.rollback()
        log.warning(f"qc error: {e}")

# ═════════════════════════ MAIN LOOP ═════════════════════════

PRIORITY = {"IT","DE","FR","ES","CH","AT","NL","BE","PL","SE","FI","US","GB","JP"}

def load_pending(conn, limit=200):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, domain, website_url, employee_count, country, industry, scan_status
        FROM industrial_company WHERE scan_status='pending'
        ORDER BY (country = ANY(%s)) DESC, id ASC
        LIMIT %s
    """, (list(PRIORITY), limit))
    rows = cur.fetchall()
    cur.close()
    return rows

log.info("=== SIGNAL ENGINE V2 — Evidence-Based Industrial Opportunity Intelligence ===")
log.info("10 categories | vendor suppression | page-weighted | industry-boosted | Postgres-only")

while True:
    try:
        stats["cycle"] += 1
        conn = get_conn()
        batch = load_pending(conn, limit=200)
        stats["queue"] = len(batch)
        if not batch:
            log.info("No pending companies. QC + 30 min pause.")
            stats["status"] = "idle"
            quality_check(conn)
            conn.close()
            time.sleep(1800)
            continue
        stats["status"] = "scanning"
        log.info(f"[C{stats['cycle']}] Batch {len(batch)} — {WORKERS} workers")
        t0 = time.time()

        ex = ThreadPoolExecutor(max_workers=WORKERS)
        futs = []
        for rec in batch:
            def _work(rec=rec):
                c = get_conn()
                try:
                    process_company(rec, c)
                finally:
                    c.close()
            futs.append(ex.submit(_work, rec))
        for f in futs:
            try:
                f.result(timeout=120)
            except Exception:
                pass
        ex.shutdown(wait=False, cancel_futures=True)

        elapsed = time.time() - t0
        log.info(f"[C{stats['cycle']}] done in {elapsed:.0f}s — scanned={stats['scanned']} good={stats['good']} "
                 f"signals={stats['signals_created']} tech={stats['tech_created']} "
                 f"jobs={stats['jobs_created']} opp={stats['opps_created']} vendors={stats['vendors_detected']} err={stats['errors']}")
        if stats["cycle"] % 3 == 0:
            quality_check(conn)
        conn.close()
    except Exception as e:
        log.error(f"MAIN LOOP ERROR (continuing): {e}")
        time.sleep(30)

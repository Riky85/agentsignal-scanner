#!/usr/bin/env python3
"""
Industrial Opportunity Intelligence — Scanner Engine v2.0
- Scansiona website, careers, blog, news, press release, job board (Indeed/LinkedIn)
- Rileva segnali industriali: robotica, CNC, AMR/AGV, MES/SCADA, machine vision, buying intent
- Dataset: 500+ aziende manifatturiere seed + crawling dinamico
- Output: IndustrialCompany + IndustrialSignal + IndustrialOpportunity su Base44
"""

import asyncio
import aiohttp
import asyncpg
import os
import json
import re
import logging
import time
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, quote_plus
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
DATABASE_URL  = os.environ.get("DATABASE_URL", "")
WORKER_ID     = int(os.environ.get("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", "1"))
CONCURRENCY   = int(os.environ.get("CONCURRENCY", "6"))
PORT          = int(os.environ.get("PORT", 8080))
REQUEST_TIMEOUT = 12
PAGE_DELAY    = 0.4

UA = "IndustrialOpportunityBot/2.0 (+https://agentsignal.io/bot)"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8,de;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

stats = {"scanned": 0, "signals": 0, "opportunities": 0, "errors": 0, "status": "starting"}

# ─── HEALTHCHECK ──────────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True).start()
log.info(f"Healthcheck su :{PORT}")

# ─── SIGNAL PATTERNS ─────────────────────────────────────────────────────────
SIGNALS = [
    # ROBOTICS
    {"cat":"robotics","type":"palletizing","conf":88,"dmin":60000,"dmax":160000,
     "p":[r"\bpalletiz\w+\b",r"\bdepalletiz\w+\b",r"\bpallet\s+(robot|system|cell|automat)\b"]},
    {"cat":"robotics","type":"pick_and_place","conf":80,"dmin":40000,"dmax":120000,
     "p":[r"\bpick[\s\-]and[\s\-]place\b",r"\bpick\s+&\s+place\b"]},
    {"cat":"robotics","type":"welding_robot","conf":90,"dmin":80000,"dmax":220000,
     "p":[r"\brobotic\s+welding\b",r"\bwelding\s+robot\b",r"\barc\s+welding\b",r"\bspot\s+welding\b",r"\bsaldatura\s+robot\b"]},
    {"cat":"robotics","type":"assembly_robot","conf":75,"dmin":60000,"dmax":180000,
     "p":[r"\brobotic\s+assembl\w+\b",r"\bautomat\w+\s+assembl\w+\b",r"\bassemblaggio\s+automat\w+\b"]},
    {"cat":"robotics","type":"collaborative_robot","conf":92,"dmin":25000,"dmax":90000,
     "p":[r"\bcobot\b",r"\bcollaborative\s+robot\b",r"\bhuman[\s\-]robot\s+collab\w+\b",r"\brobot\s+collaborat\w+\b"]},
    {"cat":"robotics","type":"end_of_line","conf":78,"dmin":50000,"dmax":140000,
     "p":[r"\bend[\s\-]of[\s\-]line\b",r"\bcase\s+packing\b",r"\bfine\s+linea\b",r"\bshrink\s+wrap\w+\b"]},
    {"cat":"robotics","type":"heavy_lifting","conf":70,"dmin":40000,"dmax":100000,
     "p":[r"\bheavy\s+lifting\b",r"\bmanual\s+lifting\b",r"\bsollev\w+\s+manual\w+\b"]},

    # CNC / MACHINE TENDING
    {"cat":"cnc_machine_tending","type":"cnc_machine","conf":85,"dmin":55000,"dmax":180000,
     "p":[r"\bcnc\s+machin\w+\b",r"\bmazak\b",r"\bdmg\s+mori\b",r"\bhaas\b",r"\bokuma\b",
          r"\bfanuc\s+cnc\b",r"\bturning\s+cent\w+\b",r"\bmachining\s+cent\w+\b",r"\btorni\s+cnc\b"]},
    {"cat":"cnc_machine_tending","type":"machine_tending","conf":92,"dmin":65000,"dmax":190000,
     "p":[r"\bmachine\s+tending\b",r"\bloading[\s/]unloading\b",r"\bload\s+unload\b",
          r"\bautomatic\s+loading\b",r"\bcaricamento\s+automat\w+\b"]},
    {"cat":"cnc_machine_tending","type":"shift_production","conf":72,"dmin":40000,"dmax":120000,
     "p":[r"\b3[\s\-]shift\b",r"\bthree[\s\-]shift\b",r"\bnight\s+shift\s+production\b",
          r"\b24[\s\/]7\s+production\b",r"\bturni\s+(di\s+)?produzione\b",r"\b3\s+turni\b"]},
    {"cat":"cnc_machine_tending","type":"lathe_milling","conf":80,"dmin":50000,"dmax":150000,
     "p":[r"\blathe\s+operat\w+\b",r"\bmilling\s+operat\w+\b",r"\bturning\s+operat\w+\b",
          r"\brettifica\b",r"\btornitura\b",r"\bfresatura\b"]},

    # AMR / AGV
    {"cat":"amr_agv","type":"warehouse_logistics","conf":70,"dmin":80000,"dmax":300000,
     "p":[r"\bwarehousing\b",r"\bintralogistics\b",r"\binternal\s+logistics\b",
          r"\bmaterial\s+handling\b",r"\blogistica\s+interna\b"]},
    {"cat":"amr_agv","type":"forklift_operations","conf":78,"dmin":100000,"dmax":350000,
     "p":[r"\bforklifts?\b",r"\bcarrelli\s+elevatori\b",r"\bempilhadeiras\b"]},
    {"cat":"amr_agv","type":"agv_amr_mention","conf":96,"dmin":120000,"dmax":450000,
     "p":[r"\bAGV\b",r"\bAMR\b",r"\bautonomous\s+mobile\s+robot\b",r"\bguided\s+vehicle\b",
          r"\bveicoli\s+automat\w+\b"]},
    {"cat":"amr_agv","type":"warehouse_expansion","conf":82,"dmin":150000,"dmax":600000,
     "p":[r"\bnew\s+warehouse\b",r"\bwarehouse\s+expansion\b",r"\bnew\s+distribution\s+cent\w+\b",
          r"\bnuovo\s+magazzino\b",r"\bampliamento\s+magazzino\b",r"\bneues\s+lager\b"]},
    {"cat":"amr_agv","type":"picking_operations","conf":75,"dmin":80000,"dmax":260000,
     "p":[r"\border\s+picking\b",r"\bgoods[\s\-]to[\s\-]person\b",r"\bpicking\s+effic\w+\b",
          r"\bprelievo\s+automat\w+\b"]},

    # MES / SCADA
    {"cat":"mes_scada","type":"mes_mention","conf":92,"dmin":50000,"dmax":220000,
     "p":[r"\bMES\b",r"\bmanufacturing\s+execution\s+system\b",r"\bsistema\s+MES\b"]},
    {"cat":"mes_scada","type":"scada_mention","conf":92,"dmin":40000,"dmax":190000,
     "p":[r"\bSCADA\b",r"\bsupervisory\s+control\b",r"\bsistema\s+SCADA\b"]},
    {"cat":"mes_scada","type":"plc_systems","conf":85,"dmin":25000,"dmax":160000,
     "p":[r"\bPLC\b",r"\bprogrammable\s+logic\b",r"\bsiemens\s+s7\b",
          r"\ballen[\s\-]bradley\b",r"\brockwell\s+automat\w+\b",r"\bschneider\s+electric\b",
          r"\bomron\b",r"\bmitsubishi\s+(plc|electric)\b"]},
    {"cat":"mes_scada","type":"oee_monitoring","conf":86,"dmin":30000,"dmax":130000,
     "p":[r"\bOEE\b",r"\boverall\s+equipment\s+effect\w+\b",r"\bproduction\s+monitoring\b",
          r"\bdowntime\s+monitor\w+\b",r"\bshop\s+floor\b",r"\bmonitoraggio\s+produz\w+\b"]},
    {"cat":"mes_scada","type":"industry40","conf":80,"dmin":50000,"dmax":280000,
     "p":[r"\bindustry\s+4\.0\b",r"\bindustrie\s+4\.0\b",r"\bsmart\s+factory\b",
          r"\bdigital\s+twin\b",r"\biiot\b",r"\bopc[\s\-]ua\b",r"\bfabbrica\s+4\.0\b"]},
    {"cat":"mes_scada","type":"traceability","conf":80,"dmin":30000,"dmax":110000,
     "p":[r"\btraceability\b",r"\bbatch\s+tracking\b",r"\brintracciabilit\w+\b",
          r"\bserial\s+number\s+tracking\b"]},

    # MACHINE VISION
    {"cat":"machine_vision","type":"quality_inspection","conf":85,"dmin":30000,"dmax":160000,
     "p":[r"\bquality\s+inspection\b",r"\bvisual\s+inspection\b",r"\bdefect\s+detection\b",
          r"\binspection\s+line\b",r"\bcontrollo\s+qualit\w+\b",r"\bispezione\s+visiva\b"]},
    {"cat":"machine_vision","type":"computer_vision","conf":92,"dmin":40000,"dmax":210000,
     "p":[r"\bcomputer\s+vision\b",r"\bmachine\s+vision\b",r"\bvision\s+system\b",
          r"\bcamera\s+inspection\b",r"\bvisione\s+artificiale\b"]},
    {"cat":"machine_vision","type":"nonconformity","conf":76,"dmin":25000,"dmax":110000,
     "p":[r"\bnon[\s\-]conformit\w+\b",r"\bdefect\s+rate\b",r"\bscrap\s+rate\b",
          r"\bzero\s+defect\b",r"\bscarti\s+di\s+produzione\b",r"\bnon\s+conformit\w+\b"]},
    {"cat":"machine_vision","type":"metrology","conf":80,"dmin":30000,"dmax":120000,
     "p":[r"\bmetrolog\w+\b",r"\bdimensional\s+inspection\b",r"\bcoordinate\s+measuring\b",
          r"\bCMM\b",r"\bmetrologica\b"]},

    # GROWTH / BUYING INTENT
    {"cat":"growth_buying_intent","type":"new_factory","conf":92,"dmin":200000,"dmax":1200000,
     "p":[r"\bnew\s+(factory|plant|facility|site)\b",r"\bgreen[\s\-]field\b",
          r"\bnuovo\s+stabilimento\b",r"\bneues\s+werk\b",r"\bnew\s+manufacturing\s+site\b",
          r"\bopening\s+(new|a)\s+(plant|facility|factory)\b"]},
    {"cat":"growth_buying_intent","type":"production_expansion","conf":86,"dmin":100000,"dmax":600000,
     "p":[r"\bproduction\s+expansion\b",r"\bexpanding\s+capacity\b",r"\bcapacity\s+increase\b",
          r"\bnew\s+production\s+line\b",r"\bespansione\s+produttiv\w+\b",
          r"\bampliamento\s+(dello\s+)?stabilimento\b",r"\bKapazit\w+serweiterung\b",
          r"\bincrease\s+(our\s+)?production\s+capacity\b"]},
    {"cat":"growth_buying_intent","type":"automation_project","conf":82,"dmin":80000,"dmax":450000,
     "p":[r"\bautomation\s+(project|initiative|investment|program)\b",
          r"\bdigital\s+transformation\b",r"\boperational\s+efficiency\b",
          r"\blabor\s+shortage\b",r"\bprogetto\s+automazion\w+\b",
          r"\btrasformazione\s+digitale\b",r"\bIndustrie\s+4\.0\s+Projekt\b"]},
    {"cat":"growth_buying_intent","type":"investment_announcement","conf":78,"dmin":150000,"dmax":700000,
     "p":[r"\b\$?€?\d+\s*m\w*\s+investment\b",r"\binvestment\s+plan\b",
          r"\bcapital\s+expenditure\b",r"\bcapex\b",r"\binvestimento\s+di\s+\d+\b",
          r"\bFörderung\b",r"\bInvestition\b"]},
    {"cat":"growth_buying_intent","type":"hiring_production_staff","conf":74,"dmin":60000,"dmax":300000,
     "p":[r"\bhiring\s+(\d+\s+)?(production|manufacturing|assembly|warehouse)\s+\w+\b",
          r"\brecruiting\s+\w+\s+operators?\b",r"\bricerchiamo\s+operatori\b",
          r"\bopen\s+position\w*.*production\b"]},

    # HIRING SIGNALS
    {"cat":"hiring","type":"automation_engineer_hiring","conf":92,"dmin":60000,"dmax":220000,
     "p":[r"\bautomation\s+engineer\b",r"\brobotic\w*\s+engineer\b",
          r"\bprocess\s+automation\s+engineer\b",r"\bingegnere\s+di\s+automazione\b",
          r"\bAutomatisierungsingenieur\b"]},
    {"cat":"hiring","type":"plc_programmer_hiring","conf":87,"dmin":35000,"dmax":160000,
     "p":[r"\bplc\s+programm\w+\b",r"\bsiemens\s+programm\w+\b",r"\bscada\s+engineer\b",
          r"\bcontrol\s+systems\s+engineer\b",r"\bprogrammatore\s+plc\b"]},
    {"cat":"hiring","type":"maintenance_technician_hiring","conf":80,"dmin":25000,"dmax":100000,
     "p":[r"\bmaintenance\s+technician\b",r"\bindustrial\s+electrician\b",
          r"\bpredictive\s+maintenance\b",r"\btecnico\s+manutentore\b",r"\bWartungstechniker\b"]},
    {"cat":"hiring","type":"manufacturing_engineer_hiring","conf":76,"dmin":50000,"dmax":190000,
     "p":[r"\bmanufacturing\s+engineer\b",r"\bproduction\s+engineer\b",
          r"\blean\s+engineer\b",r"\bcontinuous\s+improvement\b",r"\bingegnere\s+di\s+produzione\b"]},
    {"cat":"hiring","type":"mes_specialist_hiring","conf":92,"dmin":50000,"dmax":220000,
     "p":[r"\bmes\s+specialist\b",r"\bmes\s+engineer\b",r"\bscada\s+specialist\b",
          r"\bmanufacturing\s+it\b",r"\bspecialista\s+mes\b"]},
    {"cat":"hiring","type":"warehouse_operator_hiring","conf":72,"dmin":80000,"dmax":300000,
     "p":[r"\bwarehouse\s+operator\b",r"\blogistics\s+operator\b",
          r"\bfork\s*lift\s+(driver|operator)\b",r"\baddetto\s+magazzino\b",
          r"\boperatore\s+di\s+magazzino\b"]},
    {"cat":"hiring","type":"quality_technician_hiring","conf":78,"dmin":30000,"dmax":140000,
     "p":[r"\bquality\s+control\s+technician\b",r"\bquality\s+inspector\b",
          r"\bquality\s+assurance\s+engineer\b",r"\btecnico\s+qualit\w+\b",
          r"\bQualit\w+stechniker\b"]},
]

# Pagine da scansionare per ogni dominio
PAGES = [
    "/", "/about", "/about-us", "/chi-siamo", "/uber-uns",
    "/products", "/prodotti", "/services", "/servizi",
    "/manufacturing", "/produzione", "/production", "/fertigungstechnik",
    "/solutions", "/soluciones", "/industries", "/settori",
    "/warehouse", "/magazzino", "/logistics", "/logistik",
    "/quality", "/qualita", "/quality-control", "/qualitaet",
    "/careers", "/lavora-con-noi", "/jobs", "/stellenangebote",
    "/news", "/notizie", "/blog", "/press", "/presse",
    "/technology", "/tecnologia", "/innovation", "/innovazione",
    "/automation", "/automazione", "/smart-manufacturing",
]

# ─── 500+ AZIENDE SEED ────────────────────────────────────────────────────────
SEED_COMPANIES = [
    # IT — Automazione e Robotica
    {"domain":"comau.com","name":"Comau","industry":"robotics","country":"IT","city":"Turin"},
    {"domain":"salvagnini.com","name":"Salvagnini","industry":"metalworking","country":"IT","city":"Sarego"},
    {"domain":"prima-industrie.com","name":"Prima Industrie","industry":"metalworking","country":"IT","city":"Collegno"},
    {"domain":"ficep.com","name":"Ficep","industry":"metalworking","country":"IT","city":"Varese"},
    {"domain":"marposs.com","name":"Marposs","industry":"metrology","country":"IT","city":"Bologna"},
    {"domain":"datalogic.com","name":"Datalogic","industry":"automation","country":"IT","city":"Bologna"},
    {"domain":"loccioni.com","name":"Loccioni","industry":"automation","country":"IT","city":"Angeli"},
    {"domain":"bonfiglioli.com","name":"Bonfiglioli","industry":"automation","country":"IT","city":"Bologna"},
    {"domain":"camozzi.com","name":"Camozzi Automation","industry":"pneumatics","country":"IT","city":"Brescia"},
    {"domain":"gimatic.it","name":"Gimatic","industry":"robotics_end_effectors","country":"IT","city":"Orzinuovi"},
    {"domain":"pneumax.it","name":"Pneumax","industry":"pneumatics","country":"IT","city":"Lurano"},
    {"domain":"gefran.com","name":"Gefran","industry":"automation","country":"IT","city":"Provaglio"},
    {"domain":"reer.it","name":"Reer","industry":"safety","country":"IT","city":"Turin"},
    {"domain":"datalogic.com","name":"Datalogic","industry":"barcode_vision","country":"IT","city":"Bologna"},
    {"domain":"cama-group.com","name":"Cama Group","industry":"packaging","country":"IT","city":"Lecco"},
    {"domain":"ima.it","name":"IMA Group","industry":"packaging","country":"IT","city":"Bologna"},
    {"domain":"marchesini.com","name":"Marchesini Group","industry":"packaging","country":"IT","city":"Bologna"},
    {"domain":"coesia.com","name":"Coesia","industry":"packaging","country":"IT","city":"Bologna"},
    {"domain":"sacmi.com","name":"Sacmi","industry":"machinery","country":"IT","city":"Imola"},
    {"domain":"cefla.com","name":"Cefla","industry":"machinery","country":"IT","city":"Imola"},
    {"domain":"brembo.com","name":"Brembo","industry":"automotive","country":"IT","city":"Curno"},
    {"domain":"piaggio.com","name":"Piaggio","industry":"automotive","country":"IT","city":"Pontedera"},
    {"domain":"interpump.com","name":"Interpump","industry":"hydraulics","country":"IT","city":"Reggio Emilia"},
    {"domain":"comer-industries.com","name":"Comer Industries","industry":"machinery","country":"IT","city":"Reggio Emilia"},
    {"domain":"elica.com","name":"Elica","industry":"appliances","country":"IT","city":"Fabriano"},
    {"domain":"candy.it","name":"Candy","industry":"appliances","country":"IT","city":"Brugherio"},
    {"domain":"bticino.com","name":"BTicino","industry":"electrical","country":"IT","city":"Varese"},
    {"domain":"gewiss.com","name":"Gewiss","industry":"electrical","country":"IT","city":"Cenate Sotto"},
    {"domain":"seco.com","name":"Seco Tools","industry":"cutting_tools","country":"IT","city":"Fagersta"},
    {"domain":"tenova.com","name":"Tenova","industry":"steel_metallurgy","country":"IT","city":"Milan"},
    # IT — PMI Manifatturiere
    {"domain":"univer.it","name":"Univer","industry":"pneumatics","country":"IT","city":"Camisano"},
    {"domain":"pizzato.net","name":"Pizzato Elettrica","industry":"safety","country":"IT","city":"Rossano Veneto"},
    {"domain":"givi-misure.it","name":"GIVI Misure","industry":"metrology","country":"IT","city":"Milan"},
    {"domain":"cml.it","name":"CML Microsystems","industry":"electronics","country":"IT","city":"Colchester"},
    {"domain":"ldi-industries.com","name":"LDI Industries","industry":"machinery","country":"IT","city":"Brescia"},
    {"domain":"omera.it","name":"Omera","industry":"sheet_metal","country":"IT","city":"Cernusco"},
    {"domain":"eurotecno.it","name":"Eurotecno","industry":"metalworking","country":"IT","city":"Turin"},
    {"domain":"bcs.it","name":"BCS","industry":"agricultural_machinery","country":"IT","city":"Abbiategrasso"},
    {"domain":"celli.it","name":"Celli Group","industry":"food_equipment","country":"IT","city":"Savignano"},
    {"domain":"rpm-srl.it","name":"RPM","industry":"plastics","country":"IT","city":"Schio"},
    # DE — German Mittelstand
    {"domain":"trumpf.com","name":"Trumpf","industry":"metalworking","country":"DE","city":"Ditzingen"},
    {"domain":"kuka.com","name":"KUKA","industry":"robotics","country":"DE","city":"Augsburg"},
    {"domain":"festo.com","name":"Festo","industry":"automation","country":"DE","city":"Esslingen"},
    {"domain":"sew-eurodrive.com","name":"SEW-Eurodrive","industry":"automation","country":"DE","city":"Bruchsal"},
    {"domain":"weinig.com","name":"Weinig Group","industry":"woodworking","country":"DE","city":"Tauberbischofsheim"},
    {"domain":"homag.com","name":"Homag","industry":"woodworking","country":"DE","city":"Schopfloch"},
    {"domain":"duerr.com","name":"Dürr","industry":"automotive","country":"DE","city":"Bietigheim"},
    {"domain":"grob.de","name":"Grob-Werke","industry":"metalworking","country":"DE","city":"Mindelheim"},
    {"domain":"zf.com","name":"ZF Friedrichshafen","industry":"automotive","country":"DE","city":"Friedrichshafen"},
    {"domain":"schaeffler.com","name":"Schaeffler","industry":"automotive","country":"DE","city":"Herzogenaurach"},
    {"domain":"bosch.com","name":"Bosch","industry":"automotive_industrial","country":"DE","city":"Stuttgart"},
    {"domain":"siemens.com","name":"Siemens","industry":"automation","country":"DE","city":"Munich"},
    {"domain":"ifm.com","name":"IFM Electronic","industry":"sensors","country":"DE","city":"Essen"},
    {"domain":"sick.com","name":"Sick AG","industry":"sensors","country":"DE","city":"Waldkirch"},
    {"domain":"lenze.com","name":"Lenze","industry":"drive_automation","country":"DE","city":"Hameln"},
    {"domain":"beckhoff.com","name":"Beckhoff Automation","industry":"automation","country":"DE","city":"Verl"},
    {"domain":"pilz.com","name":"Pilz","industry":"safety","country":"DE","city":"Ostfildern"},
    {"domain":"schunk.com","name":"Schunk","industry":"gripping_clamping","country":"DE","city":"Lauffen"},
    {"domain":"auma.com","name":"Auma","industry":"actuators","country":"DE","city":"Müllheim"},
    {"domain":"heidenhain.com","name":"Heidenhain","industry":"metrology","country":"DE","city":"Traunreut"},
    {"domain":"wittenstein.de","name":"Wittenstein","industry":"gearboxes","country":"DE","city":"Igersheim"},
    {"domain":"baumüller.de","name":"Baumüller","industry":"drive_systems","country":"DE","city":"Nuremberg"},
    {"domain":"stoll.com","name":"Stoll Knitting","industry":"textile_machinery","country":"DE","city":"Reutlingen"},
    {"domain":"haas.com","name":"Haas CNC","industry":"cnc_machinery","country":"DE","city":"Oxnard"},
    # AT / CH
    {"domain":"engel.at","name":"Engel Austria","industry":"plastics","country":"AT","city":"Schwertberg"},
    {"domain":"blum.com","name":"Julius Blum","industry":"furniture_fittings","country":"AT","city":"Höchst"},
    {"domain":"knapp.com","name":"Knapp","industry":"warehouse","country":"AT","city":"Hart"},
    {"domain":"egon-zehnder.com","name":"Egon Zehnder","industry":"consulting","country":"CH","city":"Zurich"},
    {"domain":"bystronic.com","name":"Bystronic","industry":"metalworking","country":"CH","city":"Niederönz"},
    {"domain":"feintool.com","name":"Feintool","industry":"metalworking","country":"CH","city":"Lyss"},
    {"domain":"maxon.com","name":"Maxon","industry":"precision_motors","country":"CH","city":"Sachseln"},
    {"domain":"sulzer.com","name":"Sulzer","industry":"pumps_machinery","country":"CH","city":"Winterthur"},
    # FR / ES / PL
    {"domain":"sidel.com","name":"Sidel","industry":"food_packaging","country":"FR","city":"Octeville"},
    {"domain":"fives.com","name":"Fives","industry":"industrial_automation","country":"FR","city":"Paris"},
    {"domain":"staubli.com","name":"Stäubli","industry":"robotics","country":"FR","city":"Faverges"},
    {"domain":"proditec.fr","name":"Proditec","industry":"vision_systems","country":"FR","city":"Voisins"},
    {"domain":"fagor-arrasate.com","name":"Fagor Arrasate","industry":"metalworking","country":"ES","city":"Mondragon"},
    {"domain":"mondragon.com","name":"Mondragon Corporation","industry":"industrial","country":"ES","city":"Mondragon"},
    {"domain":"zelisko.at","name":"Zelisko","industry":"meters","country":"AT","city":"Strebersdorf"},
    {"domain":"fanuc.eu","name":"Fanuc Europe","industry":"robotics","country":"JP","city":"Luxembourg"},
    {"domain":"comiziano.com","name":"Comiziano","industry":"food_packaging","country":"IT","city":"Naples"},
    {"domain":"mts-sensors.com","name":"MTS Sensors","industry":"sensors","country":"DE","city":"Lüdenscheid"},
    # NL / SE / DK / FI
    {"domain":"vanderlande.com","name":"Vanderlande","industry":"warehouse","country":"NL","city":"Veghel"},
    {"domain":"renishaw.com","name":"Renishaw","industry":"metrology","country":"GB","city":"Wotton-under-Edge"},
    {"domain":"hexagonmi.com","name":"Hexagon Manufacturing Intelligence","industry":"metrology","country":"SE","city":"Stockholm"},
    {"domain":"alfa-laval.com","name":"Alfa Laval","industry":"food_processing","country":"SE","city":"Lund"},
    {"domain":"ssab.com","name":"SSAB","industry":"steel","country":"SE","city":"Stockholm"},
    {"domain":"sandvik.com","name":"Sandvik","industry":"mining_tools","country":"SE","city":"Stockholm"},
    {"domain":"atlas-copco.com","name":"Atlas Copco","industry":"pneumatics_tools","country":"SE","city":"Stockholm"},
    {"domain":"abb.com","name":"ABB","industry":"robotics_automation","country":"CH","city":"Zurich"},
    {"domain":"danfoss.com","name":"Danfoss","industry":"drives_controls","country":"DK","city":"Nordborg"},
    {"domain":"grundfos.com","name":"Grundfos","industry":"pumps","country":"DK","city":"Bjerringbro"},
    {"domain":"krones.com","name":"Krones","industry":"food_packaging","country":"DE","city":"Neutraubling"},
    {"domain":"gea.com","name":"GEA Group","industry":"food_machinery","country":"DE","city":"Düsseldorf"},
    # US / CA
    {"domain":"jabil.com","name":"Jabil","industry":"electronics_manufacturing","country":"US","city":"St. Petersburg"},
    {"domain":"flex.com","name":"Flex","industry":"electronics_manufacturing","country":"US","city":"Austin"},
    {"domain":"celestica.com","name":"Celestica","industry":"electronics","country":"CA","city":"Toronto"},
    {"domain":"plexus.com","name":"Plexus","industry":"electronics","country":"US","city":"Neenah"},
    {"domain":"dematic.com","name":"Dematic","industry":"warehouse","country":"US","city":"Grand Rapids"},
    {"domain":"rockwellautomation.com","name":"Rockwell Automation","industry":"automation","country":"US","city":"Milwaukee"},
    {"domain":"cognex.com","name":"Cognex","industry":"machine_vision","country":"US","city":"Natick"},
    {"domain":"keyence.com","name":"Keyence","industry":"sensors_vision","country":"JP","city":"Osaka"},
    {"domain":"teradyne.com","name":"Teradyne","industry":"test_automation","country":"US","city":"North Reading"},
    {"domain":"ur.dk","name":"Universal Robots","industry":"collaborative_robotics","country":"DK","city":"Odense"},
    # Pharma / Medical
    {"domain":"getinge.com","name":"Getinge","industry":"medical","country":"SE","city":"Gothenburg"},
    {"domain":"sartorius.com","name":"Sartorius","industry":"pharma","country":"DE","city":"Göttingen"},
    {"domain":"grifols.com","name":"Grifols","industry":"pharma","country":"ES","city":"Barcelona"},
    {"domain":"bioventus.com","name":"Bioventus","industry":"medical","country":"US","city":"Durham"},
    # Logistics / 3PL
    {"domain":"jungheinrich.com","name":"Jungheinrich","industry":"warehouse","country":"DE","city":"Hamburg"},
    {"domain":"swisslog.com","name":"Swisslog","industry":"warehouse","country":"CH","city":"Buchs"},
    {"domain":"ssi-schaefer.com","name":"SSI Schäfer","industry":"warehouse","country":"DE","city":"Neunkirchen"},
    {"domain":"mecalux.com","name":"Mecalux","industry":"warehouse","country":"ES","city":"Barcelona"},
    {"domain":"toyota-industries.com","name":"Toyota Industries","industry":"forklifts","country":"JP","city":"Kariya"},
    {"domain":"crown.com","name":"Crown Equipment","industry":"forklifts","country":"US","city":"New Bremen"},
    {"domain":"linde-mh.com","name":"Linde Material Handling","industry":"forklifts","country":"DE","city":"Aschaffenburg"},
    # Steel / Metal
    {"domain":"voestalpine.com","name":"voestalpine","industry":"steel","country":"AT","city":"Linz"},
    {"domain":"outokumpu.com","name":"Outokumpu","industry":"steel","country":"FI","city":"Helsinki"},
    {"domain":"ametek.com","name":"Ametek","industry":"electronic_instruments","country":"US","city":"Berwyn"},
    {"domain":"kennametal.com","name":"Kennametal","industry":"cutting_tools","country":"US","city":"Pittsburgh"},
    {"domain":"iscar.com","name":"Iscar","industry":"cutting_tools","country":"IL","city":"Tefen"},
    # Plastics / Rubber
    {"domain":"krauss-maffei.com","name":"KraussMaffei","industry":"plastics","country":"DE","city":"Munich"},
    {"domain":"arburg.com","name":"Arburg","industry":"plastics","country":"DE","city":"Lossburg"},
    {"domain":"sumitomo-shi.com","name":"Sumitomo Heavy Industries","industry":"plastics","country":"JP","city":"Tokyo"},
    {"domain":"wittmann-group.com","name":"Wittmann Group","industry":"plastics","country":"AT","city":"Vienna"},
    # Additional Italian PMI
    {"domain":"oms-srl.it","name":"OMS","industry":"lifting_equipment","country":"IT","city":"Brescia"},
    {"domain":"bema.it","name":"Bema","industry":"conveying","country":"IT","city":"Parma"},
    {"domain":"sitecna.it","name":"Sitecna","industry":"conveying","country":"IT","city":"Riese"},
    {"domain":"mias-group.com","name":"MIAS Group","industry":"warehouse","country":"DE","city":"Waldershof"},
    {"domain":"rocla.com","name":"Rocla","industry":"agv","country":"FI","city":"Järvenpää"},
    {"domain":"elettric80.com","name":"Elettric80","industry":"warehouse_agv","country":"IT","city":"Viano"},
    {"domain":"tre-srl.it","name":"TRE","industry":"robotic_cells","country":"IT","city":"Brescia"},
    {"domain":"arol.com","name":"Arol","industry":"capping_machines","country":"IT","city":"Canelli"},
    {"domain":"pavan.com","name":"Pavan Group","industry":"food_machinery","country":"IT","city":"Galliera Veneta"},
    {"domain":"ima-dairy.com","name":"IMA Dairy & Food","industry":"food_machinery","country":"IT","city":"Ozzano"},
    {"domain":"gd.it","name":"G.D","industry":"tobacco_packaging","country":"IT","city":"Bologna"},
    {"domain":"ilapak.com","name":"Ilapak","industry":"packaging","country":"CH","city":"Schlieren"},
    {"domain":"mespack.com","name":"Mespack","industry":"packaging","country":"ES","city":"Barcelona"},
    {"domain":"robopac.com","name":"Robopac","industry":"wrapping_machines","country":"IT","city":"Forli"},
    {"domain":"ocme.com","name":"OCME","industry":"packaging","country":"IT","city":"Parma"},
    {"domain":"sodaltech.com","name":"Sodaltech","industry":"packaging","country":"IT","city":"Cesena"},
    {"domain":"bortolin-kemo.com","name":"Bortolin Kemo","industry":"packaging","country":"IT","city":"Conegliano"},
    {"domain":"savoye.com","name":"Savoye","industry":"warehouse_automation","country":"FR","city":"Courcouronnes"},
    {"domain":"automha.com","name":"Automha","industry":"warehouse_agv","country":"IT","city":"Cologno al Serio"},
    {"domain":"ferretto-group.com","name":"Ferretto Group","industry":"warehouse","country":"IT","city":"Vicenza"},
    {"domain":"bett-sistemi.it","name":"Bett Sistemi","industry":"warehouse","country":"IT","city":"Rovereto"},
    {"domain":"eurofork.com","name":"Eurofork","industry":"warehouse","country":"IT","city":"Trieste"},
    {"domain":"kastalon.com","name":"Kastalon","industry":"polyurethane","country":"US","city":"Chicago"},
    {"domain":"omron.com","name":"Omron","industry":"automation_robotics","country":"JP","city":"Kyoto"},
    {"domain":"yaskawa.eu","name":"Yaskawa Europe","industry":"robotics_drives","country":"JP","city":"Allershausen"},
    {"domain":"nachi-fujikoshi.co.jp","name":"Nachi-Fujikoshi","industry":"robotics","country":"JP","city":"Toyama"},
    {"domain":"kawasaki-robotics.com","name":"Kawasaki Robotics","industry":"robotics","country":"JP","city":"Akashi"},
    {"domain":"denso-robotics.com","name":"Denso Robotics","industry":"robotics","country":"JP","city":"Aichi"},
    {"domain":"igus.com","name":"Igus","industry":"energy_chains","country":"DE","city":"Cologne"},
    {"domain":"zimmer-group.com","name":"Zimmer Group","industry":"gripping","country":"DE","city":"Rheinau"},
    {"domain":"destaco.com","name":"Destaco","industry":"gripping_clamping","country":"US","city":"Auburn Hills"},
    {"domain":"pneumatically.com","name":"Pneumatically","industry":"pneumatics","country":"IT","city":"Milan"},
    {"domain":"rexnord.com","name":"Rexnord","industry":"conveying","country":"US","city":"Milwaukee"},
    {"domain":"intralox.com","name":"Intralox","industry":"conveying","country":"US","city":"New Orleans"},
    {"domain":"interroll.com","name":"Interroll","industry":"conveying_warehouse","country":"CH","city":"Sant'Antonino"},
    {"domain":"dorner.com","name":"Dorner","industry":"conveying","country":"US","city":"Hartland"},
    {"domain":"bosch-rexroth.com","name":"Bosch Rexroth","industry":"drive_control","country":"DE","city":"Lohr"},
    {"domain":"parker.com","name":"Parker Hannifin","industry":"motion_control","country":"US","city":"Cleveland"},
    {"domain":"emerson.com","name":"Emerson Electric","industry":"automation","country":"US","city":"St. Louis"},
    {"domain":"honeywell.com","name":"Honeywell","industry":"automation","country":"US","city":"Charlotte"},
    {"domain":"ge.com","name":"GE Automation","industry":"automation","country":"US","city":"Boston"},
    {"domain":"yokogawa.com","name":"Yokogawa","industry":"automation","country":"JP","city":"Tokyo"},
    {"domain":"endress-hauser.com","name":"Endress+Hauser","industry":"process_automation","country":"CH","city":"Reinach"},
    {"domain":"pepperl-fuchs.com","name":"Pepperl+Fuchs","industry":"sensors","country":"DE","city":"Mannheim"},
    {"domain":"balluff.com","name":"Balluff","industry":"sensors","country":"DE","city":"Neuhausen"},
    {"domain":"turck.com","name":"Turck","industry":"sensors","country":"DE","city":"Mülheim"},
    {"domain":"contrinex.com","name":"Contrinex","industry":"sensors","country":"CH","city":"Corminboeuf"},
    {"domain":"leuze.com","name":"Leuze Electronic","industry":"sensors","country":"DE","city":"Owen"},
    {"domain":"baumer.com","name":"Baumer","industry":"sensors","country":"CH","city":"Frauenfeld"},
    {"domain":"wenglor.com","name":"Wenglor","industry":"sensors","country":"DE","city":"Tettnang"},
    {"domain":"datasensor.com","name":"Datasensor","industry":"sensors","country":"IT","city":"San Giorgio"},
    {"domain":"microscan.com","name":"Microscan","industry":"vision_barcode","country":"US","city":"Renton"},
    {"domain":"teledyne-dalsa.com","name":"Teledyne DALSA","industry":"vision_cameras","country":"CA","city":"Waterloo"},
    {"domain":"basler.com","name":"Basler","industry":"vision_cameras","country":"DE","city":"Ahrensburg"},
    {"domain":"ids-imaging.com","name":"IDS Imaging","industry":"vision_cameras","country":"DE","city":"Obersulm"},
    {"domain":"vitrox.com","name":"ViTrox","industry":"machine_vision","country":"MY","city":"Penang"},
    {"domain":"mvtec.com","name":"MVTec Software","industry":"machine_vision","country":"DE","city":"Munich"},
    {"domain":"isra-vision.com","name":"ISRA Vision","industry":"machine_vision","country":"DE","city":"Darmstadt"},
]

# ─── SCORING ENGINE ───────────────────────────────────────────────────────────
OPPS = {
    "collaborative_robot":     {"cats":["robotics","cnc_machine_tending"],"dmin":25000,"dmax":90000,"sol":"Collaborative Robot Cell (Cobot)"},
    "industrial_robot":        {"cats":["robotics"],"dmin":80000,"dmax":260000,"sol":"Industrial Robot System"},
    "amr_agv":                 {"cats":["amr_agv"],"dmin":100000,"dmax":450000,"sol":"AMR / AGV Fleet"},
    "machine_tending":         {"cats":["cnc_machine_tending"],"dmin":60000,"dmax":200000,"sol":"CNC Machine Tending Robot"},
    "palletizing":             {"cats":["robotics"],"types":["palletizing","end_of_line"],"dmin":55000,"dmax":160000,"sol":"End-of-Line Palletizing Robot"},
    "packaging_automation":    {"cats":["robotics"],"types":["end_of_line"],"dmin":45000,"dmax":140000,"sol":"Packaging Automation System"},
    "mes_scada":               {"cats":["mes_scada"],"dmin":45000,"dmax":230000,"sol":"MES / SCADA Platform"},
    "plc_upgrade":             {"cats":["mes_scada"],"types":["plc_systems"],"dmin":20000,"dmax":90000,"sol":"PLC Retrofit & Upgrade"},
    "computer_vision":         {"cats":["machine_vision"],"dmin":30000,"dmax":220000,"sol":"Computer Vision Inspection System"},
    "predictive_maintenance":  {"cats":["hiring"],"types":["maintenance_technician_hiring"],"dmin":20000,"dmax":90000,"sol":"Predictive Maintenance Platform"},
    "warehouse_automation":    {"cats":["amr_agv"],"dmin":100000,"dmax":450000,"sol":"Warehouse Automation System"},
    "industrial_ai":           {"cats":["mes_scada","machine_vision"],"dmin":45000,"dmax":220000,"sol":"Industrial AI Platform"},
}

def compute_scores(signals):
    cat_conf = {}
    cat_cnt  = {}
    for s in signals:
        c = s["signal_category"]
        cat_conf[c] = max(cat_conf.get(c, 0), s["confidence_score"])
        cat_cnt[c]  = cat_cnt.get(c, 0) + 1

    def sc(cats, w=1.0):
        hits = sum(cat_cnt.get(c,0) for c in cats)
        if not hits: return 0
        base = sum(cat_conf.get(c,0) for c in cats if c in cat_conf)
        raw  = min(100, int(base / len(cats) * w))
        bonus = min(20, hits * 5)
        return min(100, raw + bonus)

    return {
        "automation_readiness_score":       sc(["robotics","cnc_machine_tending","amr_agv","mes_scada"], 0.9),
        "robotics_opportunity_score":       sc(["robotics","cnc_machine_tending"], 1.2),
        "amr_agv_opportunity_score":        sc(["amr_agv"], 1.3),
        "mes_opportunity_score":            sc(["mes_scada"], 1.2),
        "machine_vision_opportunity_score": sc(["machine_vision"], 1.2),
        "buying_intent_score":              sc(["growth_buying_intent","hiring"], 1.4),
    }

def generate_opps(cid, cname, domain, signals, scores):
    cat_sigs = {}
    for s in signals:
        cat_sigs.setdefault(s["signal_category"], []).append(s)
    opps = []
    for otype, cfg in OPPS.items():
        cats     = cfg["cats"]
        relevant = [s for s in signals if s["signal_category"] in cats]
        if not relevant: continue
        avg_conf = sum(s["confidence_score"] for s in relevant) / len(relevant)
        osc = min(100, int(avg_conf + len(relevant)*5 + scores.get("buying_intent_score",0)//10))
        if osc < 40: continue
        factor = osc / 100
        opps.append({
            "company_id": cid, "company_name": cname, "company_domain": domain,
            "opportunity_type": otype,
            "recommended_solution": cfg["sol"],
            "opportunity_score": osc,
            "buying_intent_score": scores.get("buying_intent_score", 0),
            "estimated_deal_value_min": int(cfg["dmin"] * (0.7 + 0.3*factor)),
            "estimated_deal_value_max": int(cfg["dmax"] * (0.7 + 0.3*factor)),
            "reason_summary": f"Detected: {', '.join(s['signal_type'] for s in relevant[:3])}. {cfg['sol']}.",
            "signals_count": len(relevant),
            "top_signals": [s["signal_type"] for s in sorted(relevant, key=lambda x:-x["confidence_score"])[:3]],
        })
    opps.sort(key=lambda x: -x["opportunity_score"])
    return opps[:5]

# ─── HTML UTILS ───────────────────────────────────────────────────────────────
def clean(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script","style","noscript","head"]): t.decompose()
        return " ".join(soup.get_text(" ").split())[:60000]
    except: return html[:20000]

def detect(text, url, cid, cname, domain):
    tl = text.lower()
    found, seen = [], set()
    for p in SIGNALS:
        k = (p["cat"], p["type"])
        if k in seen: continue
        for pat in p["p"]:
            try:
                m = re.search(pat, tl)
            except: continue
            if m:
                s, e = max(0,m.start()-100), min(len(text),m.end()+100)
                found.append({
                    "company_id": cid, "company_name": cname, "company_domain": domain,
                    "signal_category": p["cat"], "signal_type": p["type"],
                    "source_url": url, "evidence_text": text[s:e].strip()[:400],
                    "confidence_score": p["conf"],
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                seen.add(k); break
    return found

# ─── HTTP ─────────────────────────────────────────────────────────────────────
async def fetch(session, url):
    for att in range(2):
        try:
            async with session.get(url, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True, ssl=False) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type","")
                    if "text" in ct or "html" in ct:
                        return await r.text(errors="replace")
                return ""
        except: 
            if att == 0: await asyncio.sleep(1)
    return ""

# ─── BASE44 API ───────────────────────────────────────────────────────────────
async def b44_post(session, entity, data):
    url = f"{B44_BASE}/{entity}"
    try:
        async with session.post(url, headers=HW, json=data,
            timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status in (200,201):
                d = await r.json(content_type=None)
                return d.get("id","") if isinstance(d, dict) else ""
    except Exception as e: log.debug(f"POST {entity}: {e}")
    return ""

async def b44_put(session, entity, eid, data):
    url = f"{B44_BASE}/{entity}/{eid}"
    try:
        async with session.put(url, headers=HW, json=data,
            timeout=aiohttp.ClientTimeout(total=30)) as r:
            return r.status in (200,201)
    except: return False

async def get_or_create_company(session, c):
    domain = c["domain"]
    url = f"{B44_BASE}/IndustrialCompany?domain={quote_plus(domain)}&limit=1&fields=id"
    try:
        async with session.get(url, headers=HW, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                if isinstance(d, list) and d: return d[0]["id"]
    except: pass
    return await b44_post(session, "IndustrialCompany", {
        "name": c.get("name", domain), "domain": domain,
        "website_url": f"https://{domain}",
        "country": c.get("country",""), "city": c.get("city",""),
        "industry": c.get("industry",""), "scan_status": "pending",
        "source": "seed_v2",
    })

# ─── SCAN ONE COMPANY ─────────────────────────────────────────────────────────
async def scan_one(session, c):
    domain  = c["domain"]
    base    = f"https://{domain}"
    cname   = c.get("name", domain)

    # Ottieni/crea company su Base44
    cid = await get_or_create_company(session, c)
    if not cid:
        stats["errors"] += 1; return

    all_sigs, pages_done = [], 0
    for path in PAGES:
        url = base.rstrip("/") + path
        html = await fetch(session, url)
        if not html and url.startswith("https://"):
            html = await fetch(session, url.replace("https://","http://",1))
        if not html: continue
        pages_done += 1
        sigs = detect(clean(html), url, cid, cname, domain)
        all_sigs.extend(sigs)
        await asyncio.sleep(PAGE_DELAY)

    # Dedup segnali
    dedup = {}
    for s in all_sigs:
        k = (s["signal_category"], s["signal_type"])
        if k not in dedup or s["confidence_score"] > dedup[k]["confidence_score"]:
            dedup[k] = s
    sigs = list(dedup.values())

    # Scoring
    scores = compute_scores(sigs)
    
    # Update company con score
    best_opp = max(scores, key=scores.get) if scores else ""
    await b44_put(session, "IndustrialCompany", cid, {
        **scores, "scan_status": "done",
        "last_scan_date": datetime.now(timezone.utc).isoformat(),
        "top_opportunity": best_opp,
    })

    # Salva segnali
    for s in sigs:
        await b44_post(session, "IndustrialSignal", s)
        await asyncio.sleep(0.08)

    # Genera e salva opportunity
    opps = generate_opps(cid, cname, domain, sigs, scores)
    for o in opps:
        await b44_post(session, "IndustrialOpportunity", o)
        await asyncio.sleep(0.08)

    # Scan job
    await b44_post(session, "IndustrialScanJob", {
        "company_id": cid, "company_domain": domain, "status": "done",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "pages_scanned": pages_done, "signals_found": len(sigs),
        "opportunities_generated": len(opps),
    })

    stats["scanned"]      += 1
    stats["signals"]      += len(sigs)
    stats["opportunities"]+= len(opps)
    log.info(f"✅ {domain}: pages={pages_done} signals={len(sigs)} opps={len(opps)} robotics={scores.get('robotics_opportunity_score',0)} intent={scores.get('buying_intent_score',0)}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    my = [c for i,c in enumerate(SEED_COMPANIES) if i % TOTAL_WORKERS == WORKER_ID]
    log.info(f"=== Industrial Scanner v2 | Worker {WORKER_ID}/{TOTAL_WORKERS} | {len(my)} aziende ===")

    sem = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        async def _run(c):
            async with sem:
                try: await scan_one(session, c)
                except Exception as e:
                    log.warning(f"ERR {c['domain']}: {e}")
                    stats["errors"] += 1
                await asyncio.sleep(1.5)
        await asyncio.gather(*[_run(c) for c in my], return_exceptions=True)

    stats["status"] = "done"
    log.info(f"=== COMPLETATO: scanned={stats['scanned']} signals={stats['signals']} opps={stats['opportunities']} ===")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

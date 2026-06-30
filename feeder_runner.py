#!/usr/bin/env python3
"""
AgentSignal Industrial Feeder v5 — Railway Worker
- FASE 0: Pulizia DB (doppioni + spazzatura Wikipedia)
- FASE 1: Insert seed curati (aziende reali, country corretta)
- FASE 2: Loop Wikipedia con validazione nome + country detection
"""
import os, time, re, random, threading, requests
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler

BASE  = os.getenv("B44_API_BASE", "https://app.base44.com/api/apps/6a3a284ab0b87dfa27558bb6/entities")
TOKEN = os.getenv("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
HDRS  = {"api-key": TOKEN, "Content-Type": "application/json"}
DELAY = float(os.getenv("INSERT_DELAY", "0.15"))
PORT  = int(os.getenv("PORT", "8080"))

# ── Healthcheck ──────────────────────────────────────────────────────────────
status = {"inserted": 0, "deleted": 0, "phase": "init"}
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        msg = f"phase={status['phase']} inserted={status['inserted']} deleted={status['deleted']}"
        self.wfile.write(msg.encode())
    def log_message(self, *a): pass
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()
print(f"[Health] HTTP on :{PORT}", flush=True)

# ── Country detection dal dominio TLD ────────────────────────────────────────
TLD_COUNTRY = {
    ".de": "DE", ".it": "IT", ".fr": "FR", ".jp": "JP", ".co.jp": "JP",
    ".uk": "GB", ".co.uk": "GB", ".ch": "CH", ".at": "AT", ".se": "SE",
    ".fi": "FI", ".dk": "DK", ".nl": "NL", ".be": "BE", ".pl": "PL",
    ".es": "ES", ".pt": "PT", ".no": "NO", ".cz": "CZ", ".sk": "SK",
    ".hu": "HU", ".ro": "RO", ".cn": "CN", ".kr": "KR", ".co.kr": "KR",
    ".in": "IN", ".com.au": "AU", ".au": "AU", ".ca": "CA", ".mx": "MX",
    ".br": "BR", ".com.br": "BR", ".ru": "RU", ".tw": "TW", ".com.tw": "TW",
    ".sg": "SG", ".hk": "HK",
}

def country_from_domain(domain):
    d = domain.lower()
    for tld, cc in sorted(TLD_COUNTRY.items(), key=lambda x: -len(x[0])):
        if d.endswith(tld):
            return cc
    return "US"  # default .com → US

# ── Scores per settore ────────────────────────────────────────────────────────
SECTOR = {
    "Ind Rob":  (72,25,42,45,70,63), "AMR":    (38,80,38,35,68,65),
    "MachTool": (52,20,55,38,68,58), "Auto":   (55,35,62,38,72,63),
    "Pharma":   (48,22,62,35,68,60), "Food":   (42,22,52,38,65,57),
    "Pack":     (45,22,55,32,67,58), "Weld":   (50,20,45,28,65,56),
    "ProcAuto": (30,15,55,35,63,57), "Sensor": (25,12,40,55,58,52),
    "Drive":    (35,22,50,22,65,56), "Metro":  (28,12,42,78,62,56),
    "MES":      (8,10,85,18,62,70),  "Energy": (40,18,55,28,65,57),
    "Agri":     (42,20,50,28,63,55), "Mining": (42,18,52,28,63,55),
    "Plastic":  (52,22,60,35,70,62), "Crane":  (40,20,50,28,63,55),
    "Textile":  (32,12,45,25,60,52), "Wood":   (50,22,58,42,70,60),
    "Aero":     (45,20,62,48,68,60), "IIoT":   (15,15,75,30,63,67),
    "Fluid":    (30,15,48,22,62,54), "Safety": (32,20,48,30,63,55),
    "Print":    (38,15,50,42,63,56), "Laser":  (55,20,55,48,70,64),
    "Coat":     (42,15,50,35,65,57), "Addit":  (30,10,50,45,63,58),
    "Connect":  (25,12,45,20,60,52), "Test":   (22,10,42,38,58,52),
    "default":  (40,20,50,30,63,55),
}

def nd(u):
    u = re.sub(r'^https?://', '', str(u).lower().strip())
    return re.sub(r'^www\.', '', u).split('/')[0].strip()

def mkpayload(name, domain, country, sector, emp=500, desc=""):
    r,a,m,v,au,b = SECTOR.get(sector, SECTOR["default"])
    e = float(emp or 500)
    mult = 4.0 if e>50000 else 3.0 if e>10000 else 2.0 if e>2000 else 1.5 if e>500 else 1.0
    bd = (r*500+m*300+au*400+v*200)*mult
    scores = {"Ind Rob":r,"AMR":a,"MES":m,"Vision":v,"Automation":au}
    # Se country è XX, derivala dal dominio
    if not country or country == "XX":
        country = country_from_domain(domain)
    return {
        "name": name[:200], "domain": domain, "website_url": f"https://{domain}",
        "country": country[:2].upper(), "industry": sector,
        "employee_count": float(e),
        "description": (desc or f"{name} is an industrial company specializing in {sector}.")[:500],
        "robotics_opportunity_score": r, "amr_agv_opportunity_score": a,
        "mes_opportunity_score": m, "machine_vision_opportunity_score": v,
        "automation_readiness_score": au, "buying_intent_score": b,
        "top_opportunity": max(scores, key=scores.get),
        "estimated_deal_value_min": float(max(15000, int(bd*0.6))),
        "estimated_deal_value_max": float(max(60000, int(bd*2.2))),
        "pipeline_stage": "new", "source": "feeder_v5",
    }

# ── Filtro nomi spazzatura ────────────────────────────────────────────────────
JUNK_NAMES = re.compile(
    r'\b(liquor|whisky|whiskey|scotch|bottler|laing|grimes|stripped|'
    r'independent bottl|black liquor|paper machine|pulp mill|distillation|'
    r'cask|malt|brewery|distillery|winery|wine|beer|spirit)\b',
    re.I
)
JUNK_EXACT = re.compile(
    r'^(manufacturing operations management|manufacturing operations|'
    r'operations management|group holdings?|company limited|'
    r'the company|a company|new company)\s*$',
    re.I
)
GENERIC_SUFFIX_ONLY = re.compile(
    r'^(manufacturing|company|group|systems|solutions|technologies|'
    r'engineering|industries|international|holdings|services)\s*$',
    re.I
)

def is_valid_company(name):
    if not name or len(name) < 4:
        return False
    if JUNK_NAMES.search(name):
        return False
    if JUNK_EXACT.match(name.strip()):
        return False
    if GENERIC_SUFFIX_ONLY.match(name.strip()):
        return False
    words = name.strip().split()
    if len(words) < 2:
        return False
    return True

# ── DB helpers ────────────────────────────────────────────────────────────────
def load_all(fields="id,name,domain,country,source"):
    records, skip = [], 0
    while True:
        try:
            r = requests.get(f"{BASE}/IndustrialCompany?limit=500&skip={skip}&fields={fields}", headers=HDRS, timeout=25)
            if r.status_code != 200: break
            b = r.json()
            if not isinstance(b, list) or not b: break
            records.extend(b)
            if len(b) < 500: break
            skip += 500
        except: break
    return records

def delete_record(rid):
    try:
        r = requests.delete(f"{BASE}/IndustrialCompany/{rid}", headers=HDRS, timeout=10)
        if r.status_code == 429:
            time.sleep(30)
            r = requests.delete(f"{BASE}/IndustrialCompany/{rid}", headers=HDRS, timeout=10)
        return r.status_code in (200, 204)
    except: return False

def push(payload, existing):
    d = payload.get("domain","")
    if d in existing: return False
    try:
        r = requests.post(f"{BASE}/IndustrialCompany", json=payload, headers=HDRS, timeout=15)
        if r.status_code == 429:
            time.sleep(45)
            r = requests.post(f"{BASE}/IndustrialCompany", json=payload, headers=HDRS, timeout=15)
        ok = r.status_code in (200, 201)
        if ok: existing.add(d)
        return ok
    except: return False

# ── FASE 0: Pulizia DB ────────────────────────────────────────────────────────
def cleanup_db():
    status["phase"] = "cleanup"
    print("[v5] FASE 0: Pulizia DB...", flush=True)

    records = load_all()
    print(f"[v5] Totale record: {len(records)}", flush=True)

    to_delete = set()

    # 1. Doppioni per nome normalizzato — tieni quello con source != feeder_v4
    name_groups = defaultdict(list)
    for c in records:
        key = re.sub(r'\s+', ' ', (c.get("name") or "").lower().strip())[:60]
        name_groups[key].append(c)

    for name, recs in name_groups.items():
        if len(recs) > 1:
            # Tieni il "migliore": preferisci source con dati reali
            priority = ["feeder_v5", "watchdog_auto", "feeder_v4", "feeder_runner", "crawler_v2"]
            keeper = None
            for src in priority:
                found = next((r for r in recs if r.get("source") == src), None)
                if found: keeper = found; break
            if not keeper: keeper = recs[0]
            for r in recs:
                if r["id"] != keeper["id"]:
                    to_delete.add(r["id"])

    # 2. Nomi spazzatura (non sono aziende reali)
    for c in records:
        name = c.get("name") or ""
        if not is_valid_company(name):
            to_delete.add(c["id"])

    # 3. XX country + source feeder vecchio = generati male
    for c in records:
        if c.get("country") == "XX" and c.get("source") in ("feeder_v4", "feeder_runner"):
            name = c.get("name") or ""
            # Elimina se generati da Wikipedia con nomi dubbi
            if JUNK_NAMES.search(name) or len(name.split()) < 2:
                to_delete.add(c["id"])

    print(f"[v5] Da eliminare: {len(to_delete)}", flush=True)

    deleted = 0
    for rid in to_delete:
        if delete_record(rid):
            deleted += 1
            status["deleted"] = deleted
        time.sleep(DELAY)
        if deleted % 200 == 0 and deleted > 0:
            print(f"[v5] Eliminati: {deleted}/{len(to_delete)}", flush=True)

    print(f"[v5] Pulizia completata: {deleted} eliminati", flush=True)

    # 4. Fix country XX rimasti — aggiorna con TLD detection
    status["phase"] = "fix_country"
    print("[v5] Fix country XX...", flush=True)
    records2 = load_all(fields="id,domain,country")
    fixed = 0
    for c in records2:
        if (c.get("country") or "XX") == "XX":
            domain = c.get("domain") or ""
            if domain:
                cc = country_from_domain(domain)
                r = requests.put(f"{BASE}/IndustrialCompany/{c['id']}", json={"country": cc}, headers=HDRS, timeout=10)
                if r.status_code == 200: fixed += 1
                elif r.status_code == 429: time.sleep(30)
            time.sleep(DELAY)
    print(f"[v5] Country fixati: {fixed}", flush=True)

# ── Seeds curati (aziende reali, country corretta) ────────────────────────────
SEEDS = [
    # Robotics
    ("KUKA AG","kuka.com","DE","Ind Rob",14000,"KUKA is a global supplier of intelligent automation solutions and industrial robots."),
    ("FANUC Corporation","fanuc.com","JP","Ind Rob",8000,"FANUC is the world leader in CNC systems, robots and factory automation."),
    ("Yaskawa Electric","yaskawa.com","JP","Ind Rob",16000,"Yaskawa provides motion control, robotics and system engineering for manufacturing."),
    ("Universal Robots","universal-robots.com","DK","Ind Rob",1000,"Universal Robots is the world leader in collaborative robots for flexible manufacturing."),
    ("ABB Robotics","new.abb.com","CH","Ind Rob",105000,"ABB is a global leader in industrial robots and automation solutions."),
    ("Stäubli Robotics","staubli.com","CH","Ind Rob",5500,"Stäubli provides high-precision industrial and collaborative robots for demanding applications."),
    ("Epson Robots","robots.epson.com","JP","Ind Rob",80000,"Epson provides high-speed SCARA robots for electronics assembly and precision manufacturing."),
    ("Denso Robotics","densorobotics.com","JP","Ind Rob",160000,"Denso produces compact high-speed SCARA and articulated industrial robots."),
    ("Doosan Robotics","doosanrobotics.com","KR","Ind Rob",800,"Doosan Robotics provides collaborative robots for flexible manufacturing."),
    ("Franka Emika","franka.de","DE","Ind Rob",400,"Franka Emika manufactures sensitive collaborative robots for research and industry."),
    ("Neura Robotics","neura-robotics.com","DE","Ind Rob",300,"Neura Robotics develops cognitive humanoid robots for industrial applications."),
    ("Piab AB","piab.com","SE","Ind Rob",1100,"Piab provides vacuum-based gripping and conveying solutions for industrial automation."),
    ("Zimmer Group","zimmer-group.com","DE","Ind Rob",1500,"Zimmer Group provides grippers, braking and clamping technology for robotics."),
    ("Schunk GmbH","schunk.com","DE","Ind Rob",3500,"Schunk is the world leader in clamping technology and gripping systems for automation."),
    ("OnRobot","onrobot.com","DK","Ind Rob",600,"OnRobot provides end-of-arm tooling for collaborative robots."),
    ("Robotiq","robotiq.com","CA","Ind Rob",400,"Robotiq provides adaptive grippers and vision systems for collaborative robots."),
    ("ATI Industrial Automation","ati-ia.com","US","Ind Rob",500,"ATI provides robotic end-effectors including force/torque sensors and tool changers."),
    ("Comau SpA","comau.com","IT","Ind Rob",4000,"Comau is a world leader in industrial automation and advanced robotic systems."),
    ("Kawasaki Robotics","kawasakirobotics.com","JP","Ind Rob",35000,"Kawasaki Robotics provides industrial robots for welding, assembly and material handling."),
    ("Nachi Robotic Systems","nachi-robotic.com","JP","Ind Rob",6000,"Nachi provides industrial robots, machine tools and hydraulic equipment."),
    ("CMA Robotics","cmarobotics.com","IT","Ind Rob",200,"CMA Robotics provides welding robots and automated cells for metal fabrication."),
    # AMR / AGV
    ("Geek+","geekplus.com","CN","AMR",2000,"Geek+ provides intelligent logistics robots and autonomous mobile robot systems."),
    ("Exotec","exotec.com","FR","AMR",600,"Exotec provides the Skypod 3D robot for high-density warehouse automation."),
    ("AutoStore","autostoresystem.com","NO","AMR",800,"AutoStore provides cube-based automated storage and retrieval systems."),
    ("Daifuku","daifuku.com","JP","AMR",12000,"Daifuku is one of the world's largest material handling and logistics automation integrators."),
    ("Grenzebach","grenzebach.com","DE","AMR",2500,"Grenzebach provides AGVs and automation for glass and building materials industry."),
    ("Rocla AGV","rocla.com","FI","AMR",500,"Rocla provides automated guided vehicles and forklift AGVs for industrial logistics."),
    ("Kivnon","kivnon.com","ES","AMR",400,"Kivnon provides autonomous guided vehicles for intralogistics."),
    ("Agilox","agilox.net","AT","AMR",300,"Agilox provides swarm intelligence-based autonomous mobile robots for manufacturing."),
    ("Locus Robotics","locusrobotics.com","US","AMR",400,"Locus Robotics provides AMRs for order fulfillment in distribution warehouses."),
    ("Fetch Robotics","fetchrobotics.com","US","AMR",300,"Fetch Robotics provides AMRs and cloud robotics software for warehouse automation."),
    ("Dematic","dematic.com","DE","AMR",8000,"Dematic provides intelligent intralogistics and automation for warehouses."),
    ("Swisslog","swisslog.com","CH","AMR",3000,"Swisslog provides robotic solutions for warehouse automation and healthcare logistics."),
    ("Knapp AG","knapp.com","AT","AMR",6000,"Knapp provides intelligent warehouse and distribution systems including AMRs."),
    ("Vanderlande","vanderlande.com","NL","AMR",7500,"Vanderlande is the global market leader for logistic process automation at airports."),
    ("Kardex Group","kardex.com","CH","AMR",2200,"Kardex provides automated storage and retrieval systems for warehouses."),
    ("Modula SpA","modula.eu","IT","AMR",900,"Modula provides vertical automated storage lift modules for industrial parts storage."),
    ("Mecalux","mecalux.com","ES","AMR",4000,"Mecalux provides storage and intralogistics solutions including automated warehouses."),
    ("Interroll Group","interroll.com","CH","AMR",2500,"Interroll provides material handling products including conveyors and sorters."),
    # MachTool
    ("DMG Mori","dmgmori.com","DE","MachTool",12000,"DMG Mori is one of the world's largest CNC machine tool manufacturers."),
    ("Mazak Corporation","mazak.com","JP","MachTool",8000,"Yamazaki Mazak manufactures CNC machine tools including multi-tasking and 5-axis centers."),
    ("Okuma Corporation","okuma.com","JP","MachTool",4000,"Okuma manufactures CNC machine tools and controls for turning, milling and grinding."),
    ("Haas Automation","haascnc.com","US","MachTool",1400,"Haas Automation is the largest CNC machine tool builder in the western world."),
    ("Makino","makino.com","JP","MachTool",5000,"Makino provides high-performance machining centers for die-mold and aerospace."),
    ("Grob-Werke","grob.de","DE","MachTool",7000,"Grob-Werke provides machining centers and production systems for automotive."),
    ("Chiron Group","chiron-group.com","DE","MachTool",2500,"Chiron provides vertical machining centers for precision manufacturing."),
    ("Hermle AG","hermle.de","DE","MachTool",1200,"Hermle manufactures high-precision 5-axis machining centers."),
    ("Hurco","hurco.com","US","MachTool",1100,"Hurco provides CNC machine tools with proprietary WinMax control."),
    ("GF Machining Solutions","gfms.com","CH","MachTool",3200,"GF Machining provides EDM, milling, laser texturing and automation for toolmaking."),
    ("Mikron Group","mikron.com","CH","MachTool",1500,"Mikron provides high-speed machining centers and automation for mass production."),
    ("Tornos","tornos.com","CH","MachTool",1800,"Tornos manufactures Swiss-type turning centers for high-precision small parts."),
    ("Emag Group","emag.com","DE","MachTool",3000,"Emag provides manufacturing solutions for precision metal components."),
    ("Gleason Corporation","gleason.com","US","MachTool",2200,"Gleason provides gear production machinery including hobbing and grinding machines."),
    ("Klingelnberg","klingelnberg.com","CH","MachTool",1800,"Klingelnberg provides bevel gear cutting machines and precision measurement centers."),
    ("Liebherr Gear Technology","liebherr.com","DE","MachTool",46000,"Liebherr provides gear cutting, grinding and honing machines for precision gears."),
    ("Ficep SpA","ficep.com","IT","MachTool",900,"Ficep provides CNC drilling lines and sawing systems for structural steel fabrication."),
    ("Salvagnini","salvagnini.com","IT","MachTool",1800,"Salvagnini provides panel benders and flexible manufacturing systems for sheet metal."),
    ("Prima Power","primapower.com","IT","Laser",2500,"Prima Power provides laser cutting, punching and bending solutions for sheet metal."),
    ("Gasparini Industries","gasparini.com","IT","MachTool",400,"Gasparini provides hydraulic press brakes, shears and laser cutting machines."),
    # MES / Software
    ("Siemens Digital Industries","siemens.com","DE","MES",90000,"Siemens Digital Industries provides automation, MES and digital factory solutions."),
    ("Rockwell Automation","rockwellautomation.com","US","MES",25000,"Rockwell Automation provides industrial automation and MES solutions."),
    ("AVEVA","aveva.com","GB","MES",6500,"AVEVA provides industrial software including MES, SCADA and historian."),
    ("Inductive Automation","inductiveautomation.com","US","MES",600,"Inductive Automation creates Ignition, the most powerful SCADA and MES platform."),
    ("Plex Systems","plex.com","US","MES",1200,"Plex provides cloud-native manufacturing ERP and MES for manufacturers."),
    ("Critical Manufacturing","criticalmanufacturing.com","PT","MES",400,"Critical Manufacturing provides MES software for semiconductor manufacturing."),
    ("Tulip Interfaces","tulip.co","US","MES",500,"Tulip provides a frontline operations platform for manufacturing."),
    ("Beckhoff Automation","beckhoff.com","DE","MES",4500,"Beckhoff provides PC-based control technology including PLCs and servo drives."),
    ("B&R Automation","br-automation.com","AT","MES",3500,"B&R provides integrated automation systems including PLCs for machine builders."),
    ("COPA-DATA","copadata.com","AT","MES",600,"COPA-DATA provides zenon automation software for SCADA and energy management."),
    # Machine Vision / Sensors
    ("Cognex Corporation","cognex.com","US","Metro",2200,"Cognex is the world leader in machine vision providing barcode readers and vision sensors."),
    ("Keyence Corporation","keyence.com","JP","Metro",8500,"Keyence provides sensors, laser markers, microscopes and machine vision for automation."),
    ("SICK AG","sick.com","DE","Sensor",10000,"SICK provides sensor solutions for factory, logistics and process automation."),
    ("Teledyne DALSA","teledynedalsa.com","CA","Metro",2000,"Teledyne DALSA provides machine vision cameras, frame grabbers and image processing."),
    ("Basler AG","baslerweb.com","DE","Metro",800,"Basler manufactures high-quality digital cameras for industrial machine vision."),
    ("IFM Electronic","ifm.com","DE","Sensor",8000,"IFM provides sensors, controllers and systems for industrial automation."),
    ("Pepperl+Fuchs","pepperl-fuchs.com","DE","Sensor",6000,"Pepperl+Fuchs provides electronic sensors and components for automation."),
    ("Turck","turck.com","DE","Sensor",4500,"Turck provides sensors, connectivity and fieldbus components for automation."),
    ("Banner Engineering","bannerengineering.com","US","Sensor",1500,"Banner Engineering provides industrial sensors, safety devices and vision systems."),
    ("Balluff","balluff.com","DE","Sensor",4200,"Balluff provides sensor solutions for position, vision, fluid and RFID applications."),
    ("Leuze Electronic","leuze.com","DE","Safety",1600,"Leuze provides sensors, safety systems and identification solutions for automation."),
    ("Pilz GmbH","pilz.com","DE","Safety",2400,"Pilz provides safe automation technology including safety controllers and sensors."),
    ("Schmersal Group","schmersal.com","DE","Safety",2000,"Schmersal provides safety switching devices and systems for machine guarding."),
    ("Hexagon AB","hexagon.com","SE","Metro",21000,"Hexagon provides sensor, software and autonomous technologies for manufacturing."),
    ("Zeiss Industrial","zeiss.com","DE","Metro",35000,"Zeiss provides precision optics, metrology systems and industrial measurement."),
    ("Faro Technologies","faro.com","US","Metro",1800,"Faro provides 3D measurement and imaging solutions for manufacturing."),
    ("Mitutoyo","mitutoyo.com","JP","Metro",6000,"Mitutoyo provides precision measuring instruments including CMMs and micrometers."),
    ("Marposs","marposs.com","IT","Metro",3000,"Marposs provides measurement, testing and inspection equipment for manufacturing."),
    ("Renishaw","renishaw.com","GB","Metro",5000,"Renishaw provides metrology and motion control products for precision manufacturing."),
    # Laser / Welding
    ("Trumpf GmbH","trumpf.com","DE","Laser",16000,"Trumpf is the world technology leader in machine tools for sheet metal and laser technology."),
    ("Bystronic","bystronic.com","CH","Laser",3500,"Bystronic provides laser cutting, bending and automation solutions for sheet metal."),
    ("Han's Laser","hanslaser.com","CN","Laser",8000,"Han's Laser provides laser cutting, marking and welding systems for manufacturing."),
    ("IPG Photonics","ipgphotonics.com","US","Laser",4000,"IPG Photonics is the world leader in fiber lasers for industrial material processing."),
    ("Coherent Corp","coherent.com","US","Laser",15000,"Coherent provides laser technology for material processing and manufacturing."),
    # Packaging
    ("Krones AG","krones.com","DE","Pack",15000,"Krones provides filling and packaging technology for beverage and food industries."),
    ("Sidel Group","sidel.com","FR","Pack",5000,"Sidel provides equipment and services for packaging beverages and personal care products."),
    ("MULTIVAC","multivac.com","DE","Pack",6500,"MULTIVAC provides packaging solutions for food, medical and consumer goods."),
    ("Syntegon","syntegon.com","DE","Pack",6000,"Syntegon provides processing and packaging solutions for pharma and food."),
    ("Coesia Group","coesia.com","IT","Pack",8000,"Coesia provides industrial and packaging solutions for tobacco, pharma and food."),
    ("IMA Group","ima.it","IT","Pharma",5500,"IMA Group provides automatic machines for processing and packaging pharmaceuticals."),
    ("Marchesini Group","marchesini.com","IT","Pharma",2200,"Marchesini provides packaging lines for pharmaceutical and cosmetics industries."),
    ("Optima Packaging","optima-packaging.com","DE","Pack",2800,"Optima provides filling and packaging machines for pharma and consumer goods."),
    ("GEA Group","gea.com","DE","Food",18000,"GEA is one of the largest technology suppliers for food processing industries."),
    ("Tetra Pak","tetrapak.com","SE","Food",24000,"Tetra Pak provides food processing and packaging solutions for liquid foods."),
    ("Bühler Group","buhlergroup.com","CH","Food",13000,"Bühler provides technologies for grain milling, chocolate production and die casting."),
    ("Alfa Laval","alfalaval.com","SE","ProcAuto",16000,"Alfa Laval provides heat transfer, fluid handling and separation products."),
    # Drives / Motion
    ("SEW-Eurodrive","sew-eurodrive.com","DE","Drive",20000,"SEW-Eurodrive provides drive technology including geared motors and electronic drives."),
    ("Lenze SE","lenze.com","DE","Drive",4000,"Lenze provides drives, controls and motion automation for machine building."),
    ("Nord Drivesystems","nord.com","DE","Drive",4200,"Nord Drivesystems provides geared motors and frequency inverters for industry."),
    ("Bonfiglioli","bonfiglioli.com","IT","Drive",4000,"Bonfiglioli provides gearboxes, drive systems and inverters for industrial automation."),
    ("Maxon Group","maxongroup.com","CH","Drive",3000,"Maxon provides high-precision DC motors and drive systems for robotics."),
    ("Harmonic Drive","harmonicdrive.net","JP","Drive",1200,"Harmonic Drive provides precision strain wave gears for robotics."),
    ("Nabtesco","nabtesco.com","JP","Drive",4500,"Nabtesco provides precision reduction gears for industrial robots."),
    ("Wittenstein","wittenstein.de","DE","Drive",2600,"Wittenstein provides high-precision gearheads for industrial robots."),
    ("Parker Hannifin","parker.com","US","Fluid",57000,"Parker Hannifin provides motion and control technologies for precision engineering."),
    ("Bosch Rexroth","boschrexroth.com","DE","Fluid",32000,"Bosch Rexroth provides hydraulics, pneumatics and linear motion for industry."),
    ("Festo AG","festo.com","DE","Fluid",21000,"Festo provides pneumatic and electrical automation components and systems."),
    ("SMC Corporation","smcworld.com","JP","Fluid",26000,"SMC is the world's largest manufacturer of pneumatic automation components."),
    ("Atlas Copco","atlascopco.com","SE","Fluid",50000,"Atlas Copco provides sustainable productivity solutions for industrial markets."),
    ("Grundfos","grundfos.com","DK","Fluid",19000,"Grundfos is the world's largest pump manufacturer for water and industrial applications."),
    ("Sulzer AG","sulzer.com","CH","Fluid",14000,"Sulzer provides pumping solutions and rotating equipment services."),
    ("Endress+Hauser","endress.com","CH","ProcAuto",14000,"Endress+Hauser provides process instrumentation for measurement and analytics."),
    ("Yokogawa Electric","yokogawa.com","JP","ProcAuto",18000,"Yokogawa provides process automation and industrial automation solutions."),
    ("Emerson Process","emerson.com","US","ProcAuto",90000,"Emerson provides process control software and measurement instruments."),
    ("Honeywell Process","process.honeywell.com","US","ProcAuto",100000,"Honeywell Process provides DCS, SCADA and safety systems for process industries."),
    ("Valmet Corporation","valmet.com","FI","ProcAuto",17000,"Valmet provides technologies and automation for pulp, paper and energy industries."),
    ("Andritz AG","andritz.com","AT","ProcAuto",27000,"Andritz provides plants and equipment for hydropower, pulp and paper."),
    # Bearings / Linear
    ("SKF Group","skf.com","SE","Drive",45000,"SKF is the world leader in bearings, seals and lubrication services."),
    ("NSK Ltd","nsk.com","JP","Drive",30000,"NSK provides bearings, linear technology and steering systems for industry."),
    ("NTN Corporation","ntnglobal.com","JP","Drive",25000,"NTN provides bearings, driveshafts and precision equipment for industrial applications."),
    ("Schaeffler Group","schaeffler.com","DE","Auto",84000,"Schaeffler provides precision components for engines, transmissions and chassis."),
    ("THK","thk.com","JP","Drive",6500,"THK provides linear motion systems and ball screws for machine tools."),
    ("Hiwin Technologies","hiwin.com","TW","Drive",3000,"Hiwin provides linear motion systems, ball screws and robots for automation."),
    # Plastics
    ("Engel Austria","engel.at","AT","Plastic",7000,"Engel is one of the world's leading injection molding machine manufacturers."),
    ("Arburg","arburg.com","DE","Plastic",3400,"Arburg provides injection molding machines and additive manufacturing systems."),
    ("Wittmann Battenfeld","wittmann-group.com","AT","Plastic",3000,"Wittmann Battenfeld provides injection molding machines and automation systems."),
    ("KraussMaffei","kraussmaffei.com","DE","Plastic",5000,"KraussMaffei provides injection molding, extrusion and reaction process technology."),
    ("Husky Injection Molding","husky.ca","CA","Plastic",4000,"Husky provides injection molding equipment for plastics manufacturing."),
    ("Reifenhauser Group","reifenhauser.com","DE","Plastic",3000,"Reifenhauser provides extrusion systems for films, nonwovens and technical textiles."),
    # Woodworking
    ("SCM Group","scmgroup.com","IT","Wood",4500,"SCM Group provides woodworking machinery and integrated systems for furniture."),
    ("Biesse Group","biesse.com","IT","Wood",4000,"Biesse provides CNC machining centers and edgebanders for wood processing."),
    ("Homag Group","homag.com","DE","Wood",6000,"Homag provides woodworking machinery and production systems for furniture."),
    ("Weinig Group","weinig.com","DE","Wood",2200,"Weinig provides solid wood processing solutions including planing and profiling."),
    ("IMA Schelling","ima-schelling.com","AT","Wood",2000,"IMA Schelling provides panel dividing saws and CNC machining centers for furniture."),
    # Connectivity
    ("Phoenix Contact","phoenixcontact.com","DE","Connect",17000,"Phoenix Contact provides electrical connection and industrial automation solutions."),
    ("Weidmuller","weidmuller.com","DE","Connect",5000,"Weidmüller provides electronic components and network solutions for industry."),
    ("Harting Technology","harting.com","DE","Connect",4500,"Harting provides industrial connectors, data networks and device connectivity."),
    ("WAGO Corporation","wago.com","DE","Connect",8000,"WAGO provides electrical interconnection technology and I/O systems for automation."),
    ("Murrelektronik","murrelektronik.com","DE","Connect",2000,"Murrelektronik provides power supplies, fieldbus systems and industrial electronics."),
    ("Carlo Gavazzi","carlo-gavazzi.com","CH","Connect",700,"Carlo Gavazzi provides electronic components and IoT sensors for automation."),
    ("Finder SpA","findernet.com","IT","Connect",1200,"Finder provides relays, power contactors and time relays for industrial automation."),
    # Additive / Test
    ("EOS GmbH","eos.info","DE","Addit",1500,"EOS provides industrial 3D printing and additive manufacturing solutions."),
    ("Stratasys","stratasys.com","US","Addit",3000,"Stratasys provides FDM and PolyJet 3D printing solutions for manufacturing."),
    ("Materialise NV","materialise.com","BE","Addit",2500,"Materialise provides 3D printing software and services for industrial use."),
    ("Renishaw Additive","renishaw.com","GB","Addit",5000,"Renishaw provides metal additive manufacturing and metrology systems."),
    ("Nordson Corporation","nordson.com","US","Coat",7500,"Nordson provides precision dispensing equipment for adhesives and coatings."),
    ("Dürr AG","durr.com","DE","Coat",16000,"Dürr provides painting and finishing systems for automotive manufacturing."),
    ("National Instruments","ni.com","US","Test",7700,"NI provides test and measurement instruments and software for industry."),
    ("Keysight Technologies","keysight.com","US","Test",14000,"Keysight provides electronic test and measurement equipment."),
    ("Rohde & Schwarz","rohde-schwarz.com","DE","Test",13000,"Rohde & Schwarz provides test and measurement and cybersecurity solutions."),
    # Agriculture / Mining / Energy
    ("Claas KGaA","claas.com","DE","Agri",12000,"Claas is one of the world's leading manufacturers of combines and agricultural machinery."),
    ("AGCO Corporation","agcocorp.com","US","Agri",23000,"AGCO provides agricultural solutions through Fendt, Massey Ferguson and Challenger."),
    ("Kubota Corporation","kubota.com","JP","Agri",48000,"Kubota provides agricultural machinery, construction equipment and water technology."),
    ("Same Deutz-Fahr","sdf.com","IT","Agri",7000,"SDF Group provides tractors under SAME, Deutz-Fahr and Lamborghini brands."),
    ("Sandvik Mining","sandvik.com","SE","Mining",42000,"Sandvik provides equipment, tools and services for mining and construction."),
    ("Epiroc AB","epiroc.com","SE","Mining",15000,"Epiroc provides equipment for drilling and exploration in mining."),
    ("Metso Outotec","mogroup.com","FI","Mining",15000,"Metso Outotec provides equipment for minerals processing and metals refining."),
    ("Konecranes","konecranes.com","FI","Crane",16000,"Konecranes provides industrial cranes and lifting equipment for manufacturing."),
    ("Palfinger AG","palfinger.com","AT","Crane",12000,"Palfinger is the world leader in loader cranes and lifting solutions."),
    ("Vestas Wind Systems","vestas.com","DK","Energy",25000,"Vestas is the world's leading manufacturer of wind turbines."),
    ("Siemens Gamesa","siemensgamesa.com","ES","Energy",25000,"Siemens Gamesa provides renewable energy solutions including wind turbines."),
    ("Atlas Copco Tools","atlascopco.com","SE","Ind Rob",50000,"Atlas Copco provides industrial tools, assembly systems and compressors."),
]

# ── Wikipedia categories (solo per aziende manifatturiere, no articoli generici) ──
WIKI_CATS = [
    ("Category:Industrial_robot_manufacturers","Ind Rob"),
    ("Category:Collaborative_robot_manufacturers","Ind Rob"),
    ("Category:Machine_tool_manufacturers","MachTool"),
    ("Category:CNC_machine_manufacturers","MachTool"),
    ("Category:Grinding_machine_manufacturers","MachTool"),
    ("Category:Welding_equipment_manufacturers","Weld"),
    ("Category:Sensor_manufacturers","Sensor"),
    ("Category:Metrology_companies","Metro"),
    ("Category:Coordinate-measuring_machine_manufacturers","Metro"),
    ("Category:Automated_guided_vehicle_manufacturers","AMR"),
    ("Category:Warehouse_automation_companies","AMR"),
    ("Category:Forklift_manufacturers","AMR"),
    ("Category:Electric_motor_manufacturers","Drive"),
    ("Category:Servo_motor_manufacturers","Drive"),
    ("Category:Gearbox_manufacturers","Drive"),
    ("Category:Pneumatics_manufacturers","Fluid"),
    ("Category:Pump_manufacturers","Fluid"),
    ("Category:Valve_manufacturers","Fluid"),
    ("Category:Hydraulics_companies","Fluid"),
    ("Category:Safety_equipment_manufacturers","Safety"),
    ("Category:Packaging_machinery_manufacturers","Pack"),
    ("Category:Pharmaceutical_equipment_manufacturers","Pharma"),
    ("Category:Medical_device_manufacturers","Pharma"),
    ("Category:Food_processing_equipment_manufacturers","Food"),
    ("Category:Plastics_machinery_manufacturers","Plastic"),
    ("Category:Injection_moulding_machine_manufacturers","Plastic"),
    ("Category:Agricultural_machinery_manufacturers","Agri"),
    ("Category:Mining_equipment_manufacturers","Mining"),
    ("Category:Crane_manufacturers","Crane"),
    ("Category:Wind_turbine_manufacturers","Energy"),
    ("Category:Power_electronics_manufacturers","Energy"),
    ("Category:Textile_machinery_manufacturers","Textile"),
    ("Category:Woodworking_machine_manufacturers","Wood"),
    ("Category:3D_printing_companies","Addit"),
    ("Category:Laser_manufacturers","Laser"),
    ("Category:Laser_cutting_machine_manufacturers","Laser"),
    ("Category:Semiconductor_equipment_companies","MES"),
    ("Category:Manufacturing_execution_system_companies","MES"),
    ("Category:Test_equipment_manufacturers","Test"),
    ("Category:Electrical_connector_manufacturers","Connect"),
    ("Category:Electronic_component_manufacturers","Connect"),
    # Country-specific (solo manifatturiero)
    ("Category:Manufacturing_companies_of_Germany","MachTool"),
    ("Category:Manufacturing_companies_of_Italy","MachTool"),
    ("Category:Manufacturing_companies_of_Japan","MachTool"),
    ("Category:Manufacturing_companies_of_France","MachTool"),
    ("Category:Manufacturing_companies_of_Switzerland","MachTool"),
    ("Category:Manufacturing_companies_of_Austria","MachTool"),
    ("Category:Manufacturing_companies_of_Sweden","MachTool"),
    ("Category:Manufacturing_companies_of_Finland","MachTool"),
    ("Category:Manufacturing_companies_of_Netherlands","MachTool"),
    ("Category:Manufacturing_companies_of_Denmark","MachTool"),
    ("Category:Manufacturing_companies_of_Spain","MachTool"),
    ("Category:Manufacturing_companies_of_South_Korea","MachTool"),
    ("Category:Manufacturing_companies_of_the_United_States","MachTool"),
    ("Category:Manufacturing_companies_of_China","MachTool"),
    ("Category:Automotive_parts_manufacturers","Auto"),
    ("Category:Automotive_suppliers","Auto"),
    ("Category:Aerospace_manufacturers","Aero"),
]

def scrape_wiki_cat(cat, sector, existing, limit=30):
    results = []
    try:
        url = (f"https://en.wikipedia.org/w/api.php?action=query&list=categorymembers"
               f"&cmtitle={cat}&cmlimit=50&cmtype=page&format=json")
        r = requests.get(url, timeout=12, headers={"User-Agent": "IndustrialFeeder/5.0"})
        if r.status_code != 200: return results
        members = r.json().get("query", {}).get("categorymembers", [])
        random.shuffle(members)
        for m in members[:limit]:
            title = m.get("title", "")
            if not title or ":" in title: continue

            # Valida che sia un nome di azienda reale
            if not is_valid_company(title): continue

            # Rimuovi suffissi legali per generare il dominio
            clean = re.sub(
                r'\b(GmbH|AG|SpA|Srl|Ltd|Corp|Inc|BV|NV|AS|AB|Oy|SA|KG|PLC|LLC|SE)\b',
                '', title, flags=re.I
            )
            clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean).lower().strip()
            words = [w for w in clean.split() if len(w) > 2][:3]
            if not words or len(words) < 2: continue

            # Genera un solo dominio candidato — il più probabile
            dom = words[0] + words[1] + ".com"
            d = nd(dom)
            if d not in existing and len(d) > 6 and "." in d:
                results.append((title, d, sector))
    except Exception as e:
        print(f"[wiki] errore {cat}: {e}", flush=True)
    return results

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global status

    # FASE 0: Pulizia
    cleanup_db()

    # FASE 1: Seeds curati
    status["phase"] = "seeds"
    print("[v5] FASE 1: Seeds curati...", flush=True)
    existing = load_all(fields="domain")
    existing_set = set(nd(c.get("domain") or "") for c in existing)

    seed_queue = list(SEEDS)
    random.shuffle(seed_queue)
    for (name, domain, country, sector, emp, desc) in seed_queue:
        d = nd(domain)
        if d in existing_set: continue
        p = mkpayload(name, domain, country, sector, emp, desc)
        if push(p, existing_set):
            status["inserted"] += 1
            print(f"[seed] ✅ {name} | {country} ({status['inserted']})", flush=True)
        time.sleep(DELAY)

    # FASE 2: Loop Wikipedia
    status["phase"] = "wikipedia_loop"
    print("[v5] FASE 2: Wikipedia loop...", flush=True)
    cat_idx = 0
    while True:
        cat, sector = WIKI_CATS[cat_idx % len(WIKI_CATS)]
        cat_idx += 1

        entries = scrape_wiki_cat(cat, sector, existing_set)
        for (name, domain, sec) in entries:
            country = country_from_domain(domain)
            p = mkpayload(name, domain, country, sec)
            if push(p, existing_set):
                status["inserted"] += 1
                print(f"[wiki] ✅ {name} | {domain} | {country} | {sec} ({status['inserted']})", flush=True)
            time.sleep(DELAY)

        if cat_idx % 57 == 0:
            existing = load_all(fields="domain")
            existing_set = set(nd(c.get("domain") or "") for c in existing)
            print(f"[v5] Refresh: {len(existing_set)} domini esistenti", flush=True)

if __name__ == "__main__":
    main()

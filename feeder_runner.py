#!/usr/bin/env python3
"""
AgentSignal Industrial Feeder — Railway Worker
Healthcheck HTTP + loop infinito di inserimento aziende industriali su Base44.
"""
import os, time, re, random, threading, requests
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ──────────────────────────────────────────────────────────────────
BASE  = os.getenv("B44_API_BASE", "https://app.base44.com/api/apps/6a3a284ab0b87dfa27558bb6/entities")
TOKEN = os.getenv("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
HDRS  = {"api-key": TOKEN, "Content-Type": "application/json"}
DELAY = float(os.getenv("INSERT_DELAY", "0.2"))
PORT  = int(os.getenv("PORT", "8080"))

# ── Healthcheck HTTP (obbligatorio Railway) ──────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()
print(f"[Health] HTTP server on :{PORT}", flush=True)

# ── Helpers ──────────────────────────────────────────────────────────────────
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
    "default":  (40,20,50,30,63,55),
}

def nd(u):
    u = re.sub(r'^https?://', '', u.lower().strip())
    return re.sub(r'^www\.', '', u).split('/')[0]

def mkpayload(name, domain, country, sector, emp=500, desc=""):
    r,a,m,v,au,b = SECTOR.get(sector, SECTOR["default"])
    e = float(emp or 500)
    mult = 4.0 if e>50000 else 3.0 if e>10000 else 2.0 if e>2000 else 1.5 if e>500 else 1.0
    bd = (r*500+m*300+au*400+v*200)*mult
    scores = {"Ind Rob":r,"AMR":a,"MES":m,"Vision":v,"Automation":au}
    return {
        "name":name[:200],"domain":domain,"website_url":f"https://{domain}",
        "country":(country or "XX")[:2].upper(),"industry":sector,
        "employee_count":float(e),
        "description":(desc or f"{name} — industrial company in {sector}.")[:500],
        "robotics_opportunity_score":r,"amr_agv_opportunity_score":a,
        "mes_opportunity_score":m,"machine_vision_opportunity_score":v,
        "automation_readiness_score":au,"buying_intent_score":b,
        "top_opportunity":max(scores,key=scores.get),
        "estimated_deal_value_min":float(max(15000,int(bd*0.6))),
        "estimated_deal_value_max":float(max(60000,int(bd*2.2))),
        "pipeline_stage":"new","source":"feeder_runner",
    }

def load_existing():
    existing, skip = set(), 0
    while True:
        r = requests.get(f"{BASE}/IndustrialCompany?limit=500&skip={skip}&fields=domain", headers=HDRS, timeout=20)
        if r.status_code != 200: break
        batch = r.json()
        if not batch: break
        for c in batch:
            d = nd(c.get("domain") or "")
            if d: existing.add(d)
        if len(batch) < 500: break
        skip += 500
    return existing

def push(payload):
    try:
        r = requests.post(f"{BASE}/IndustrialCompany", json=payload, headers=HDRS, timeout=15)
        return r.status_code in (200, 201)
    except: return False

# ── Wikipedia category scraper ───────────────────────────────────────────────
WIKI_CATS = [
    ("Category:Industrial_robot_manufacturers","Ind Rob"),
    ("Category:Robot_manufacturers","Ind Rob"),
    ("Category:Machine_tool_manufacturers","MachTool"),
    ("Category:Machine_tool_builders","MachTool"),
    ("Category:Packaging_machinery_manufacturers","Pack"),
    ("Category:Food_processing_equipment_manufacturers","Food"),
    ("Category:Automation_companies","ProcAuto"),
    ("Category:Industrial_automation_companies","ProcAuto"),
    ("Category:Sensor_manufacturers","Sensor"),
    ("Category:Welding_equipment_manufacturers","Weld"),
    ("Category:Metrology_companies","Metro"),
    ("Category:Machine_vision_companies","Metro"),
    ("Category:Conveyor_manufacturers","AMR"),
    ("Category:Material_handling_equipment_manufacturers","AMR"),
    ("Category:Electric_motor_manufacturers","Drive"),
    ("Category:Gearbox_manufacturers","Drive"),
    ("Category:Pneumatics_manufacturers","Drive"),
    ("Category:Agricultural_machinery_manufacturers","Agri"),
    ("Category:Mining_equipment_manufacturers","Mining"),
    ("Category:Crane_manufacturers","Crane"),
    ("Category:Compressor_manufacturers","ProcAuto"),
    ("Category:Textile_machinery_manufacturers","Textile"),
    ("Category:Woodworking_machine_manufacturers","Wood"),
    ("Category:Pharmaceutical_equipment_manufacturers","Pharma"),
    ("Category:Plastics_machinery_manufacturers","Plastic"),
    ("Category:Semiconductor_equipment_companies","MES"),
    ("Category:Wind_turbine_manufacturers","Energy"),
    ("Category:Manufacturing_companies_of_Germany","MachTool"),
    ("Category:Manufacturing_companies_of_Italy","MachTool"),
    ("Category:Manufacturing_companies_of_Japan","MachTool"),
    ("Category:Manufacturing_companies_of_France","MachTool"),
    ("Category:Manufacturing_companies_of_Switzerland","MachTool"),
    ("Category:Manufacturing_companies_of_Austria","MachTool"),
    ("Category:Manufacturing_companies_of_Sweden","MachTool"),
    ("Category:Automotive_suppliers","Auto"),
    ("Category:Cutting_tool_manufacturers","MachTool"),
    ("Category:Hydraulics_companies","Drive"),
    ("Category:Pump_manufacturers","ProcAuto"),
    ("Category:Test_equipment_manufacturers","IIoT"),
    ("Category:Manufacturing_software","MES"),
    ("Category:CNC_machine_tool_manufacturers","MachTool"),
    ("Category:Grinding_machine_manufacturers","MachTool"),
    ("Category:Stamping_press_manufacturers","MachTool"),
    ("Category:Forging_press_manufacturers","MachTool"),
    ("Category:Injection_molding_machine_manufacturers","Plastic"),
    ("Category:Extrusion_machinery_manufacturers","Plastic"),
    ("Category:Pharmaceutical_machinery_manufacturers","Pharma"),
    ("Category:Filling_machine_manufacturers","Pack"),
    ("Category:Labeling_machine_manufacturers","Pack"),
    ("Category:Bottling_machinery_manufacturers","Pack"),
    ("Category:Printing_machinery_manufacturers","Pack"),
    ("Category:Safety_equipment_manufacturers","IIoT"),
    ("Category:Industrial_robot_manufacturers_of_Germany","Ind Rob"),
    ("Category:Industrial_robot_manufacturers_of_Japan","Ind Rob"),
    ("Category:Industrial_robot_manufacturers_of_Italy","Ind Rob"),
    ("Category:Hydraulic_machinery_manufacturers","Drive"),
    ("Category:Valve_manufacturers","Valve"),
    ("Category:Bearing_manufacturers","Bearing"),
    ("Category:Power_transmission_equipment_manufacturers","Drive"),
    ("Category:Measurement_instrument_manufacturers","Metro"),
    ("Category:Vision_system_manufacturers","Metro"),
    ("Category:Construction_equipment_manufacturers","Mining"),
    ("Category:Earthmoving_equipment_manufacturers","Mining"),
    ("Category:Lifting_equipment_manufacturers","Crane"),
    ("Category:Industrial_furnace_manufacturers","ProcAuto"),
    ("Category:Heat_treatment_companies","ProcAuto"),
    ("Category:Coating_equipment_manufacturers","Auto"),
    ("Category:Laser_cutting_companies","Weld"),
    ("Category:Fiber_laser_manufacturers","Weld"),
    ("Category:Powder_metallurgy_companies","MachTool"),
    ("Category:Metrology_instrument_manufacturers","Metro"),
    ("Category:Automation_companies_of_Germany","ProcAuto"),
    ("Category:Automation_companies_of_Italy","ProcAuto"),
    ("Category:Automation_companies_of_Japan","ProcAuto"),
    ("Category:Automation_companies_of_Sweden","ProcAuto"),
    ("Category:Automotive_component_manufacturers","Auto"),
    ("Category:Automotive_companies_of_South_Korea","Auto"),
    ("Category:Aerospace_companies_of_the_United_States","Aero"),
    ("Category:Defence_companies_of_Germany","Aero"),
    ("Category:Defence_companies_of_France","Aero"),
    ("Category:Defence_companies_of_Italy","Aero"),
]

def scrape_wiki_cat(cat, sector, existing, limit=25):
    results = []
    try:
        url = (f"https://en.wikipedia.org/w/api.php?action=query&list=categorymembers"
               f"&cmtitle={cat}&cmlimit=50&cmtype=page&format=json")
        r = requests.get(url, timeout=10, headers={"User-Agent":"IndustrialFeeder/2.0"})
        if r.status_code != 200: return results
        members = r.json().get("query",{}).get("categorymembers",[])
        random.shuffle(members)
        for m in members[:limit]:
            title = m.get("title","")
            if not title or ":" in title: continue
            clean = re.sub(r'\b(GmbH|AG|SpA|Srl|Ltd|Corp|Inc|BV|NV|AS|AB|SA|KG|Co|Group|Holding|International|Industries|Systems|Technologies|Engineering|Solutions)\b','',title,flags=re.IGNORECASE)
            clean = re.sub(r'[^a-zA-Z0-9\s]',' ',clean).lower().strip()
            words = [w for w in clean.split() if len(w)>2][:3]
            if not words: continue
            for dom in [words[0]+words[1]+".com" if len(words)>1 else None, "-".join(words[:2])+".com" if len(words)>1 else None, words[0]+".com"]:
                if not dom: continue
                d = nd(dom)
                if d not in existing and len(d)>5:
                    results.append((title, d, sector)); break
    except: pass
    return results

# ── Seed list (aziende reali curate) ─────────────────────────────────────────
SEEDS = [
    # ROBOTICA
    ("KUKA AG","kuka.com","DE","Ind Rob",14000,"KUKA is a global supplier of intelligent automation solutions."),
    ("FANUC Corporation","fanuc.com","JP","Ind Rob",8000,"FANUC is the world leader in CNC systems and factory automation."),
    ("Yaskawa Electric","yaskawa.com","JP","Ind Rob",16000,"Yaskawa provides motion control, robotics and system engineering."),
    ("Universal Robots","universal-robots.com","DK","Ind Rob",1000,"Universal Robots is the world leader in collaborative robots."),
    ("ABB Robotics","abb.com","CH","Ind Rob",105000,"ABB is a global leader in industrial robots and automation."),
    ("Kawasaki Robotics","kawasaki.com","JP","Ind Rob",35000,"Kawasaki manufactures industrial robots for welding and handling."),
    ("Stäubli","staubli.com","CH","Ind Rob",5500,"Stäubli provides high-precision industrial and collaborative robots."),
    ("Doosan Robotics","doosanrobotics.com","KR","Ind Rob",800,"Doosan Robotics provides collaborative robots for manufacturing."),
    ("Epson Robots","robots.epson.com","JP","Ind Rob",3000,"Epson is a world leader in SCARA robots for precision assembly."),
    ("Denso Robotics","densorobotics.com","JP","Ind Rob",5000,"Denso produces high-speed SCARA and articulated robots."),
    ("Franka Emika","franka.de","DE","Ind Rob",400,"Franka Emika manufactures sensitive collaborative robots."),
    ("OnRobot","onrobot.com","DK","Ind Rob",500,"OnRobot provides end-of-arm tooling for collaborative robots."),
    ("Schunk GmbH","schunk.com","DE","Ind Rob",3500,"Schunk is the world leader in clamping technology and grippers."),
    ("Piab AB","piab.com","SE","Ind Rob",1100,"Piab provides vacuum-based gripping solutions for automation."),
    ("Neura Robotics","neura-robotics.com","DE","Ind Rob",300,"Neura Robotics develops cognitive humanoid robots for industry."),
    # AMR / AGV
    ("Geek+","geekplus.com","CN","AMR",2000,"Geek+ provides intelligent logistics robots and AMR systems."),
    ("Exotec","exotec.com","FR","AMR",600,"Exotec provides the Skypod 3D robot for warehouse automation."),
    ("AutoStore","autostoresystem.com","NO","AMR",800,"AutoStore provides high-density cube storage automation."),
    ("Daifuku","daifuku.com","JP","AMR",12000,"Daifuku is one of the world's largest material handling integrators."),
    ("SSI Schaefer","ssi-schaefer.com","DE","AMR",10000,"SSI Schaefer manufactures storage and logistics systems."),
    ("Vanderlande","vanderlande.com","NL","AMR",7500,"Vanderlande provides logistic process automation for airports."),
    ("Dematic","dematic.com","DE","AMR",8000,"Dematic provides intelligent intralogistics automation."),
    ("Swisslog","swisslog.com","CH","AMR",3000,"Swisslog delivers robotic solutions for intralogistics."),
    ("Interroll","interroll.com","CH","AMR",5700,"Interroll provides conveyor systems and sorters for intralogistics."),
    ("Jungheinrich","jungheinrich.com","DE","AMR",18000,"Jungheinrich manufactures forklifts and warehouse automation."),
    ("STILL GmbH","still.de","DE","AMR",10000,"Still provides forklifts and automated guided vehicles."),
    ("Knapp AG","knapp.com","AT","AMR",5000,"KNAPP provides intelligent intralogistics automation systems."),
    ("Kardex Group","kardex.com","CH","AMR",2200,"Kardex provides automated storage and retrieval solutions."),
    # MACHINE TOOLS
    ("DMG Mori","dmgmori.com","DE","MachTool",12000,"DMG MORI is the global market leader in cutting machine tools."),
    ("Mazak Corporation","mazak.com","JP","MachTool",7000,"Yamazaki Mazak is one of the world's leading CNC manufacturers."),
    ("TRUMPF","trumpf.com","DE","MachTool",15000,"TRUMPF is a global leader in machine tools and laser technology."),
    ("Haas Automation","haascnc.com","US","MachTool",15000,"Haas Automation is the largest machine tool builder in the West."),
    ("Okuma","okuma.com","JP","MachTool",3200,"Okuma provides CNC machine tools with OSP control technology."),
    ("Makino","makino.com","JP","MachTool",4000,"Makino provides high-performance machining centers for aerospace."),
    ("GF Machining Solutions","gfms.com","CH","MachTool",3500,"GF Machining provides EDM, milling and laser machines."),
    ("Hermle AG","hermle.de","DE","MachTool",1300,"Hermle manufactures 5-axis machining centers for precision work."),
    ("EMAG Group","emag.com","DE","MachTool",3000,"EMAG provides turning, grinding and laser machines."),
    ("Gleason Corporation","gleason.com","US","MachTool",3500,"Gleason is the world leader in gear production machinery."),
    ("Ficep Group","ficep.it","IT","MachTool",1500,"Ficep is a world leader in structural steel fabrication machinery."),
    ("Breton SpA","breton.it","IT","MachTool",1000,"Breton manufactures CNC machining centers for metals."),
    ("SCM Group","scmgroup.com","IT","Wood",5000,"SCM Group is a world leader in woodworking machinery."),
    ("Biesse Group","biesse.com","IT","Wood",4500,"Biesse provides CNC machining centers for wood, glass and stone."),
    # WELDING
    ("Lincoln Electric","lincolnelectric.com","US","Weld",3000,"Lincoln Electric is the world leader in welding products."),
    ("Fronius","fronius.com","AT","Weld",5000,"Fronius is an innovation leader in welding technology."),
    ("ESAB","esab.com","SE","Weld",10000,"ESAB is a world leader in welding and cutting products."),
    ("Hypertherm","hypertherm.com","US","Weld",1800,"Hypertherm provides plasma and laser cutting systems."),
    ("Kemppi","kemppi.com","FI","Weld",1000,"Kemppi provides welding equipment and digital solutions."),
    ("IPG Photonics","ipgphotonics.com","US","Weld",4000,"IPG Photonics is the world leader in fiber lasers."),
    # FOOD
    ("Tetra Pak","tetrapak.com","CH","Food",25000,"Tetra Pak is the world leader in food processing and packaging."),
    ("GEA Group","gea.com","DE","Food",18000,"GEA is a top supplier for food and pharma processing."),
    ("Alfa Laval","alfalaval.com","SE","Food",22000,"Alfa Laval provides heat transfer and fluid handling."),
    ("Marel","marel.com","IS","Food",7500,"Marel provides advanced food processing equipment."),
    ("Bühler Group","buhlergroup.com","CH","Food",13000,"Bühler provides equipment for food and advanced materials."),
    ("Multivac","multivac.com","DE","Food",6500,"Multivac is a world leader in packaging solutions for food."),
    ("JBT Corporation","jbtc.com","US","Food",6000,"JBT provides technology solutions for food processing."),
    ("Handtmann","handtmann.de","DE","Food",3000,"Handtmann provides filling and portioning systems for food."),
    # PACKAGING
    ("Krones AG","krones.com","DE","Pack",14000,"Krones provides beverage filling and packaging technology."),
    ("Sidel Group","sidel.com","FR","Pack",5500,"Sidel provides complete PET and packaging lines."),
    ("IMA Group","ima.it","IT","Pack",7000,"IMA Group is the world leader in pharma packaging machines."),
    ("Marchesini Group","marchesini.com","IT","Pack",3000,"Marchesini designs automatic machines for pharma packaging."),
    ("Syntegon Technology","syntegon.com","DE","Pack",11000,"Syntegon provides processing and packaging technology."),
    ("Bobst Group","bobst.com","CH","Pack",5500,"Bobst provides equipment for packaging manufacturing."),
    ("Cama Group","camagroup.com","IT","Pack",600,"Cama Group provides robotic packaging systems."),
    ("Robopac","robopac.com","IT","Pack",2500,"Robopac provides stretch wrapping solutions for industry."),
    # PHARMA
    ("IMA Life","imalife.com","IT","Pharma",3000,"IMA Life provides filling machines for pharmaceutical products."),
    ("Körber Pharma","koerber-pharma.com","DE","Pharma",5000,"Körber Pharma provides integrated solutions for pharma packaging."),
    ("GEA Pharma","geapharma.com","DE","Pharma",5000,"GEA Pharma provides processing equipment for pharmaceuticals."),
    ("Getinge AB","getinge.com","SE","Pharma",11000,"Getinge provides products for operating rooms and sterile processing."),
    ("Glatt GmbH","glatt.com","DE","Pharma",4000,"Glatt is the world leader in pharmaceutical fluid bed equipment."),
    ("Fette Compacting","fette-compacting.com","DE","Pharma",2000,"Fette Compacting provides tablet press machines."),
    # SENSORS & METROLOGY
    ("Hexagon AB","hexagon.com","SE","Metro",24000,"Hexagon provides digital reality solutions for manufacturing."),
    ("Renishaw","renishaw.com","GB","Metro",5000,"Renishaw provides measurement and inspection solutions."),
    ("Cognex Corporation","cognex.com","US","Metro",2200,"Cognex is the world leader in machine vision."),
    ("Keyence Corporation","keyence.com","JP","Metro",8000,"Keyence manufactures sensors and measuring systems."),
    ("SICK AG","sick.com","DE","Sensor",18000,"SICK manufactures sensors for factory automation."),
    ("Balluff","balluff.com","DE","Sensor",3900,"Balluff provides sensors for industrial automation."),
    ("ifm electronic","ifm.com","DE","Sensor",8000,"ifm provides sensors and controls for automation."),
    ("Pepperl+Fuchs","pepperl-fuchs.com","DE","Sensor",6500,"Pepperl+Fuchs provides sensors for industrial automation."),
    ("Marposs","marposs.com","IT","Metro",3000,"Marposs is the world leader in measurement solutions."),
    # DRIVES
    ("SEW-Eurodrive","sew-eurodrive.com","DE","Drive",21000,"SEW-EURODRIVE provides drive technology and electronics."),
    ("Bosch Rexroth","boschrexroth.com","DE","Drive",32000,"Bosch Rexroth provides drive and control technologies."),
    ("Parker Hannifin","parker.com","US","Drive",55000,"Parker Hannifin is the global leader in motion control."),
    ("Danfoss","danfoss.com","DK","Drive",40000,"Danfoss provides drives and power solutions for industry."),
    ("Lenze SE","lenze.com","DE","Drive",4000,"Lenze is a specialist in drive solutions and automation."),
    ("Nord Drivesystems","nord.com","DE","Drive",4500,"Nord provides gear units, motors and inverters."),
    ("Bonfiglioli","bonfiglioli.com","IT","Drive",4000,"Bonfiglioli is a leader in gear motors and drive systems."),
    ("Festo AG","festo.com","DE","Drive",21000,"Festo provides pneumatic and electric automation."),
    ("SMC Corporation","smcworld.com","JP","Drive",20000,"SMC is the world's largest pneumatics manufacturer."),
    ("SKF Group","skf.com","SE","Drive",45000,"SKF is the leading supplier of bearings and seals."),
    ("Schaeffler Group","schaeffler.com","DE","Drive",83000,"Schaeffler provides precision bearings and components."),
    # PROCESS AUTOMATION
    ("Endress+Hauser","endress.com","CH","ProcAuto",16000,"Endress+Hauser provides measurement for process industries."),
    ("Yokogawa Electric","yokogawa.com","JP","ProcAuto",18000,"Yokogawa provides industrial automation and measurement."),
    ("Emerson Electric","emerson.com","US","ProcAuto",88000,"Emerson provides automation for process industries."),
    ("Honeywell Process","honeywellprocess.com","US","ProcAuto",110000,"Honeywell provides automation for oil, gas and chemicals."),
    ("Samson AG","samson.de","DE","ProcAuto",3500,"Samson provides control valves and instrumentation."),
    ("Rotork plc","rotork.com","GB","ProcAuto",3700,"Rotork provides flow control including electric actuators."),
    # MES / INDUSTRIAL SOFTWARE
    ("Infor Industrial","infor.com","US","MES",21000,"Infor provides industry-specific ERP software."),
    ("Epicor Software","epicor.com","US","MES",3500,"Epicor provides ERP and MES software for manufacturing."),
    ("Inductive Automation","inductiveautomation.com","US","MES",400,"Inductive Automation makes Ignition SCADA/MES."),
    ("PTC ThingWorx","ptc.com","US","IIoT",7500,"PTC ThingWorx provides industrial IoT analytics."),
    ("Augury Inc","augury.com","US","IIoT",400,"Augury provides AI-based machine health monitoring."),
    # AGRI
    ("Claas","claas.com","DE","Agri",12000,"Claas is a world leader in combines and agricultural machinery."),
    ("AGCO Corporation","agcocorp.com","US","Agri",23000,"AGCO provides Fendt, Massey Ferguson and Challenger brands."),
    ("Kubota Corporation","kubota.com","JP","Agri",48000,"Kubota provides agricultural machinery and construction equipment."),
    ("Same Deutz-Fahr","sdf.com","IT","Agri",7000,"SDF Group provides tractors under SAME and Deutz-Fahr brands."),
    ("Amazone GmbH","amazone.de","DE","Agri",2000,"Amazone manufactures seeding and crop protection equipment."),
    # ENERGY
    ("Vestas Wind Systems","vestas.com","DK","Energy",25000,"Vestas is the world's leading wind turbine manufacturer."),
    ("Siemens Gamesa","siemensgamesa.com","ES","Energy",25000,"Siemens Gamesa provides wind energy solutions."),
    ("Atlas Copco","atlascopco.com","SE","ProcAuto",50000,"Atlas Copco provides compressors and productivity solutions."),
    ("Kaeser Kompressoren","kaeser.com","DE","ProcAuto",6500,"Kaeser provides compressed air systems for industry."),
    # PLASTICS
    ("Engel Austria","engelglobal.com","AT","Plastic",6500,"Engel is a world leader in injection molding machines."),
    ("KraussMaffei","kraussmaffei.com","DE","Plastic",5000,"KraussMaffei provides injection molding and extrusion."),
    ("Arburg GmbH","arburg.com","DE","Plastic",3500,"Arburg provides injection molding machines."),
    # HEAVY INDUSTRY
    ("SMS Group","sms-group.com","DE","MachTool",14000,"SMS Group provides machinery for the steel industry."),
    ("Danieli Group","danieli.com","IT","MachTool",10000,"Danieli is a world-leading manufacturer for metals."),
    ("Sandvik Mining","sandvik.com","SE","Mining",42000,"Sandvik provides equipment for mining and construction."),
    ("Epiroc AB","epiroc.com","SE","Mining",15000,"Epiroc provides equipment for drilling in mining."),
    ("Konecranes","konecranes.com","FI","Crane",16000,"Konecranes provides industrial cranes for manufacturing."),
    ("Palfinger AG","palfinger.com","AT","Crane",12000,"Palfinger is the world leader in loader cranes."),
    # AUTOMOTIVE
    ("Magna International","magna.com","CA","Auto",170000,"Magna is one of the world's largest automotive suppliers."),
    ("ZF Friedrichshafen","zf.com","DE","Auto",165000,"ZF provides technology for mobility solutions."),
    ("Valeo SA","valeo.com","FR","Auto",100000,"Valeo provides components for smart and electrified mobility."),
    ("Benteler","benteler.com","DE","Auto",30000,"Benteler is a global automotive supplier."),
    ("Gestamp","gestamp.com","ES","Auto",42000,"Gestamp designs metal components for automotive."),
    # TEXTILE
    ("Groz-Beckert KG","groz-beckert.com","DE","Textile",9000,"Groz-Beckert provides tools for textile production machines."),
    ("Karl Mayer Group","karlmayer.com","DE","Textile",3000,"Karl Mayer is the world leader in warp knitting machines."),
    ("Picanol NV","picanol.be","BE","Textile",2200,"Picanol provides weaving machines for textiles."),
    # CUTTING TOOLS
    ("Sandvik Coromant","coromant.sandvik.com","SE","MachTool",8000,"Sandvik Coromant is the world's top supplier of cutting tools."),
    ("ISCAR","iscar.com","IL","MachTool",5000,"ISCAR manufactures carbide cutting tools."),
    ("Kennametal","kennametal.com","US","MachTool",8500,"Kennametal provides tooling and advanced materials."),
    ("Walter AG","walter-tools.com","DE","MachTool",3500,"Walter AG manufactures precision cutting tools."),
    ("Guhring","guhring.com","DE","MachTool",5000,"Gühring provides rotary cutting tools and clamping."),
    ("Seco Tools","secotools.com","SE","MachTool",4000,"Seco Tools provides metal cutting solutions."),
    ("Mapal","mapal.com","DE","MachTool",5000,"Mapal provides precision tools for machining."),
]

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("="*55, flush=True)
    print("AgentSignal Industrial Feeder — START", flush=True)
    print("="*55, flush=True)

    loop = 0
    total = 0

    while True:
        loop += 1
        print(f"\n[Loop #{loop}] Loading existing DB...", flush=True)
        existing = load_existing()
        print(f"[Loop #{loop}] DB: {len(existing)} companies", flush=True)

        ins = 0

        # Fase 1: seed list
        for (name, domain, country, sector, emp, desc) in SEEDS:
            d = nd(domain)
            if d in existing: continue
            if push(mkpayload(name, d, country, sector, emp, desc)):
                existing.add(d); ins += 1; total += 1
                if total % 20 == 0:
                    print(f"  [+{total}] {name}", flush=True)
            time.sleep(DELAY)

        print(f"[Loop #{loop}] Seeds: +{ins}", flush=True)

        # Fase 2: Wikipedia categories
        wiki_ins = 0
        cats = WIKI_CATS.copy()
        random.shuffle(cats)
        for cat, sector in cats[:15]:
            for name, domain, sec in scrape_wiki_cat(cat, sector, existing):
                if domain in existing: continue
                country = "DE"
                if domain.endswith(".it"): country="IT"
                elif domain.endswith(".fr"): country="FR"
                elif domain.endswith(".jp"): country="JP"
                elif domain.endswith(".uk"): country="GB"
                elif domain.endswith(".us"): country="US"
                elif domain.endswith(".se"): country="SE"
                elif domain.endswith(".ch"): country="CH"
                elif domain.endswith(".at"): country="AT"
                elif domain.endswith(".dk"): country="DK"
                elif domain.endswith(".nl"): country="NL"
                elif domain.endswith(".es"): country="ES"
                if push(mkpayload(name, domain, country, sec)):
                    existing.add(domain); wiki_ins += 1; total += 1
                    if total % 20 == 0:
                        print(f"  [WIKI +{total}] {name[:40]}", flush=True)
                time.sleep(DELAY)
            time.sleep(1)

        print(f"[Loop #{loop}] Wikipedia: +{wiki_ins}", flush=True)
        print(f"[Loop #{loop}] ✅ Loop total: +{ins+wiki_ins} | Grand total: {total}", flush=True)

        wait = 60 if ins+wiki_ins == 0 else 30
        print(f"[Loop #{loop}] Waiting {wait}s...", flush=True)
        time.sleep(wait)

if __name__ == "__main__":
    main()

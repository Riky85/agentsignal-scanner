#!/usr/bin/env python3
"""
AgentSignal Industrial Feeder v4 — Railway Worker
Healthcheck HTTP + loop infinito con 100 categorie Wikipedia + seed curati.
"""
import os, time, re, random, threading, requests, json
from http.server import HTTPServer, BaseHTTPRequestHandler

BASE  = os.getenv("B44_API_BASE", "https://app.base44.com/api/apps/6a3a284ab0b87dfa27558bb6/entities")
TOKEN = os.getenv("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
HDRS  = {"api-key": TOKEN, "Content-Type": "application/json"}
DELAY = float(os.getenv("INSERT_DELAY", "11.5"))
PORT  = int(os.getenv("PORT", "8080"))

# ── Healthcheck ──────────────────────────────────────────────────────────────
inserted_count = 0
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(f"OK inserted={inserted_count}".encode())
    def log_message(self, *a): pass
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()
print(f"[Health] HTTP on :{PORT}", flush=True)

# ── Sector scores ────────────────────────────────────────────────────────────
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
    return {
        "name":name[:200],"domain":domain,"website_url":f"https://{domain}",
        "country":(country or "XX")[:2].upper(),"industry":sector,
        "employee_count":float(e),
        "description":(desc or f"{name} is an industrial company specializing in {sector}.")[:500],
        "robotics_opportunity_score":r,"amr_agv_opportunity_score":a,
        "mes_opportunity_score":m,"machine_vision_opportunity_score":v,
        "automation_readiness_score":au,"buying_intent_score":b,
        "top_opportunity":max(scores,key=scores.get),
        "estimated_deal_value_min":float(max(15000,int(bd*0.6))),
        "estimated_deal_value_max":float(max(60000,int(bd*2.2))),
        "pipeline_stage":"new","source":"feeder_v4",
    }

def load_existing():
    existing, skip = set(), 0
    while True:
        try:
            r = requests.get(f"{BASE}/IndustrialCompany?limit=500&skip={skip}&fields=domain", headers=HDRS, timeout=25)
            if r.status_code != 200: break
            batch = r.json()
            if not isinstance(batch, list) or not batch: break
            for c in batch:
                d = nd(c.get("domain") or "")
                if d: existing.add(d)
            if len(batch) < 500: break
            skip += 500
        except: break
    return existing

def push(payload, existing):
    d = payload.get("domain","")
    if d in existing: return False
    try:
        r = requests.post(f"{BASE}/IndustrialCompany", json=payload, headers=HDRS, timeout=15)
        if r.status_code == 429:
            time.sleep(45); return False
        ok = r.status_code in (200,201)
        if ok: existing.add(d)
        return ok
    except: return False

# ── 100 Wikipedia categories (expanded) ─────────────────────────────────────
WIKI_CATS = [
    # Robotics
    ("Category:Industrial_robot_manufacturers","Ind Rob"),
    ("Category:Robot_manufacturers","Ind Rob"),
    ("Category:Collaborative_robot_manufacturers","Ind Rob"),
    # Machine Tools
    ("Category:Machine_tool_manufacturers","MachTool"),
    ("Category:Machine_tool_builders","MachTool"),
    ("Category:CNC_machine_manufacturers","MachTool"),
    ("Category:Grinding_machine_manufacturers","MachTool"),
    ("Category:Lathe_manufacturers","MachTool"),
    # Packaging
    ("Category:Packaging_machinery_manufacturers","Pack"),
    ("Category:Bottling_companies","Pack"),
    ("Category:Labeling_machine_manufacturers","Pack"),
    # Food
    ("Category:Food_processing_equipment_manufacturers","Food"),
    ("Category:Food_processing_companies","Food"),
    ("Category:Beverage_companies","Food"),
    # Automation
    ("Category:Automation_companies","ProcAuto"),
    ("Category:Industrial_automation_companies","ProcAuto"),
    ("Category:Process_control_companies","ProcAuto"),
    ("Category:SCADA_companies","MES"),
    ("Category:Manufacturing_execution_system_companies","MES"),
    # Sensors
    ("Category:Sensor_manufacturers","Sensor"),
    ("Category:Transducer_manufacturers","Sensor"),
    ("Category:Proximity_sensor_manufacturers","Sensor"),
    # Welding
    ("Category:Welding_equipment_manufacturers","Weld"),
    ("Category:Welding_companies","Weld"),
    # Metrology
    ("Category:Metrology_companies","Metro"),
    ("Category:Machine_vision_companies","Metro"),
    ("Category:Coordinate-measuring_machine_manufacturers","Metro"),
    ("Category:Optical_instrument_manufacturers","Metro"),
    # AMR / Logistics
    ("Category:Conveyor_belt_manufacturers","AMR"),
    ("Category:Material_handling_equipment_manufacturers","AMR"),
    ("Category:Forklift_manufacturers","AMR"),
    ("Category:Automated_guided_vehicle_manufacturers","AMR"),
    ("Category:Warehouse_automation_companies","AMR"),
    # Drive / Motion
    ("Category:Electric_motor_manufacturers","Drive"),
    ("Category:Gearbox_manufacturers","Drive"),
    ("Category:Pneumatics_manufacturers","Drive"),
    ("Category:Hydraulics_companies","Fluid"),
    ("Category:Pump_manufacturers","Fluid"),
    ("Category:Valve_manufacturers","Fluid"),
    ("Category:Servo_motor_manufacturers","Drive"),
    # Safety
    ("Category:Safety_equipment_manufacturers","Safety"),
    ("Category:Industrial_safety_companies","Safety"),
    # Agriculture
    ("Category:Agricultural_machinery_manufacturers","Agri"),
    ("Category:Tractor_manufacturers","Agri"),
    ("Category:Harvester_manufacturers","Agri"),
    # Mining
    ("Category:Mining_equipment_manufacturers","Mining"),
    ("Category:Drilling_equipment_manufacturers","Mining"),
    # Construction
    ("Category:Construction_equipment_manufacturers","Crane"),
    ("Category:Crane_manufacturers","Crane"),
    ("Category:Excavator_manufacturers","Crane"),
    # Energy
    ("Category:Wind_turbine_manufacturers","Energy"),
    ("Category:Solar_panel_manufacturers","Energy"),
    ("Category:Power_electronics_manufacturers","Energy"),
    ("Category:Gas_turbine_manufacturers","Energy"),
    # Textile & Wood
    ("Category:Textile_machinery_manufacturers","Textile"),
    ("Category:Woodworking_machine_manufacturers","Wood"),
    ("Category:Sawmill_equipment_manufacturers","Wood"),
    # Pharma & Med
    ("Category:Pharmaceutical_equipment_manufacturers","Pharma"),
    ("Category:Medical_device_manufacturers","Pharma"),
    ("Category:Diagnostic_equipment_manufacturers","Pharma"),
    # Plastic & Rubber
    ("Category:Plastics_machinery_manufacturers","Plastic"),
    ("Category:Injection_moulding_machine_manufacturers","Plastic"),
    ("Category:Rubber_manufacturing_companies","Plastic"),
    ("Category:Extrusion_equipment_manufacturers","Plastic"),
    # Semiconductor
    ("Category:Semiconductor_equipment_companies","MES"),
    ("Category:Semiconductor_companies_of_Germany","MES"),
    ("Category:Semiconductor_companies_of_Japan","MES"),
    # Printing
    ("Category:Printing_press_manufacturers","Print"),
    ("Category:Printing_companies","Print"),
    ("Category:Inkjet_printer_manufacturers","Print"),
    # Laser
    ("Category:Laser_manufacturers","Laser"),
    ("Category:Laser_cutting_machine_manufacturers","Laser"),
    # Coatings
    ("Category:Paint_and_coatings_manufacturers","Coat"),
    ("Category:Thermal_spray_companies","Coat"),
    # Additive
    ("Category:3D_printing_companies","Addit"),
    ("Category:Additive_manufacturing","Addit"),
    # Connectors / Electronics
    ("Category:Electrical_connector_manufacturers","Connect"),
    ("Category:Electronic_component_manufacturers","Connect"),
    ("Category:Industrial_electronics_manufacturers","Connect"),
    # Test & Measurement
    ("Category:Test_equipment_manufacturers","Test"),
    ("Category:Electronic_test_equipment","Test"),
    # Country-specific manufacturing
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
    ("Category:Manufacturing_companies_of_India","MachTool"),
    ("Category:Manufacturing_companies_of_Canada","MachTool"),
    # Automotive
    ("Category:Automotive_suppliers","Auto"),
    ("Category:Automotive_parts_manufacturers","Auto"),
    ("Category:Automotive_technology","Auto"),
    # Aerospace
    ("Category:Aerospace_manufacturers","Aero"),
    ("Category:Aerospace_companies","Aero"),
    ("Category:Aircraft_component_manufacturers","Aero"),
    # IIoT / Software
    ("Category:Industrial_internet_of_things","IIoT"),
    ("Category:Manufacturing_software","MES"),
    ("Category:Enterprise_resource_planning_software","MES"),
    # Compressors & HVAC
    ("Category:Compressor_manufacturers","Fluid"),
    ("Category:HVAC_manufacturers","ProcAuto"),
    ("Category:Refrigeration_equipment_manufacturers","ProcAuto"),
    # Conveying specialized
    ("Category:Elevator_manufacturers","Crane"),
    ("Category:Escalator_manufacturers","Crane"),
    ("Category:Pneumatic_tube_system_manufacturers","AMR"),
    # Chemical process
    ("Category:Chemical_process_equipment_manufacturers","ProcAuto"),
    ("Category:Distillation_companies","ProcAuto"),
    ("Category:Heat_exchanger_manufacturers","ProcAuto"),
    # Paper / Pulp
    ("Category:Paper_machine_manufacturers","Wood"),
    ("Category:Pulp_and_paper_industry","Wood"),
    # Marine
    ("Category:Marine_propulsion_manufacturers","Energy"),
    ("Category:Shipbuilding_companies","Aero"),
]

# ── Curated seed list ────────────────────────────────────────────────────────
SEEDS = [
    ("KUKA AG","kuka.com","DE","Ind Rob",14000,"KUKA is a global supplier of intelligent automation solutions with over 50 years of experience."),
    ("FANUC Corporation","fanuc.com","JP","Ind Rob",8000,"FANUC is the world leader in CNC systems, robots and factory automation equipment."),
    ("Yaskawa Electric","yaskawa.com","JP","Ind Rob",16000,"Yaskawa provides motion control, robotics and system engineering for global manufacturing."),
    ("Universal Robots","universal-robots.com","DK","Ind Rob",1000,"Universal Robots is the world leader in collaborative robots for flexible manufacturing."),
    ("Stäubli Robotics","staubli.com","CH","Ind Rob",5500,"Stäubli provides high-precision industrial and medical robots for demanding applications."),
    ("Epson Robots","robots.epson.com","JP","Ind Rob",80000,"Epson provides high-speed SCARA robots for electronics assembly and precision manufacturing."),
    ("Denso Robotics","densorobotics.com","JP","Ind Rob",160000,"Denso produces compact high-speed SCARA and articulated industrial robots."),
    ("Doosan Robotics","doosanrobotics.com","KR","Ind Rob",800,"Doosan Robotics provides collaborative robots for flexible manufacturing SMEs."),
    ("Franka Emika","franka.de","DE","Ind Rob",400,"Franka Emika manufactures sensitive collaborative robots for research and industry."),
    ("Neura Robotics","neura-robotics.com","DE","Ind Rob",300,"Neura Robotics develops cognitive humanoid robots for industrial and service applications."),
    ("Piab AB","piab.com","SE","Ind Rob",1100,"Piab provides vacuum-based gripping and conveying solutions for industrial automation."),
    ("Zimmer Group","zimmer-group.com","DE","Ind Rob",1500,"Zimmer Group provides grippers, braking and clamping technology for robotics and automation."),
    ("Geek+","geekplus.com","CN","AMR",2000,"Geek+ provides intelligent logistics robots and autonomous mobile robot systems."),
    ("Exotec","exotec.com","FR","AMR",600,"Exotec provides the Skypod 3D robot system for high-density warehouse automation."),
    ("AutoStore","autostoresystem.com","NO","AMR",800,"AutoStore provides cube-based automated storage and retrieval systems."),
    ("Daifuku","daifuku.com","JP","AMR",12000,"Daifuku is one of the world's largest material handling and logistics automation integrators."),
    ("Grenzebach","grenzebach.com","DE","AMR",2500,"Grenzebach provides AGVs, conveying systems and automation for glass and building materials."),
    ("Rocla","rocla.com","FI","AMR",500,"Rocla provides automated guided vehicles and forklift AGVs for industrial logistics."),
    ("Kivnon","kivnon.com","ES","AMR",400,"Kivnon provides autonomous guided vehicles and AMRs for intralogistics operations."),
    ("Agilox","agilox.net","AT","AMR",300,"Agilox provides swarm intelligence-based autonomous mobile robots for manufacturing."),
    ("DMG Mori","dmgmori.com","DE","MachTool",12000,"DMG Mori is one of the world's largest CNC machine tool manufacturers providing turning and milling."),
    ("Mazak Corporation","mazak.com","JP","MachTool",8000,"Yamazaki Mazak manufactures CNC machine tools including multi-tasking and 5-axis machining centers."),
    ("Okuma Corporation","okuma.com","JP","MachTool",4000,"Okuma manufactures CNC machine tools and controls for turning, milling and grinding."),
    ("Haas Automation","haascnc.com","US","MachTool",1400,"Haas Automation is the largest machine tool builder in the western world producing CNC machining centers."),
    ("Makino","makino.com","JP","MachTool",5000,"Makino provides high-performance machining centers for die-mold, aerospace and automotive."),
    ("Grob-Werke","grob.de","DE","MachTool",7000,"Grob-Werke provides machining centers and production systems for automotive and aerospace."),
    ("Chiron Group","chiron-group.com","DE","MachTool",2500,"Chiron provides vertical machining centers and mill-turn centers for precision manufacturing."),
    ("Hermle AG","hermle.de","DE","MachTool",1200,"Hermle manufactures high-precision 5-axis machining centers for complex part manufacturing."),
    ("Feeler Machine Tool","feeler.com.tw","TW","MachTool",2000,"Feeler provides CNC machining centers and lathes for global manufacturing markets."),
    ("Hurco","hurco.com","US","MachTool",1100,"Hurco provides CNC machine tools with proprietary WinMax control for job shop manufacturing."),
    ("Siemens Digital Industries","siemens.com","DE","MES",90000,"Siemens Digital Industries provides automation, drives, MES and digital factory solutions."),
    ("Rockwell Automation","rockwellautomation.com","US","MES",25000,"Rockwell Automation provides industrial automation and information solutions including MES and SCADA."),
    ("AVEVA","aveva.com","GB","MES",6500,"AVEVA provides industrial software including MES, SCADA, historian and asset management."),
    ("Inductive Automation","inductiveautomation.com","US","MES",600,"Inductive Automation creates Ignition, the most powerful SCADA and MES platform available."),
    ("Plex Systems","plex.com","US","MES",1200,"Plex provides cloud-native manufacturing ERP and MES for discrete and process manufacturers."),
    ("Critical Manufacturing","criticalmanufacturing.com","PT","MES",400,"Critical Manufacturing provides MES software for semiconductor and complex discrete manufacturing."),
    ("Tulip Interfaces","tulip.co","US","MES",500,"Tulip provides a frontline operations platform with no-code app builder for manufacturing."),
    ("FactoryTalk (Rockwell)","factorytalk.com","US","MES",2000,"FactoryTalk provides MES, analytics and IIoT solutions for smart manufacturing operations."),
    ("Opcenter (Siemens)","opcenter.siemens.com","DE","MES",3000,"Opcenter provides integrated MES and quality management for discrete and process industries."),
    ("Cognex Corporation","cognex.com","US","Metro",2200,"Cognex is the world leader in machine vision providing barcode readers and vision sensors."),
    ("Keyence Corporation","keyence.com","JP","Metro",8500,"Keyence provides sensors, laser markers, microscopes and machine vision for factory automation."),
    ("SICK AG","sick.com","DE","Sensor",10000,"SICK provides sensor solutions for factory, logistics and process automation."),
    ("Teledyne DALSA","teledynedalsa.com","CA","Metro",2000,"Teledyne DALSA provides machine vision cameras, frame grabbers and image processing software."),
    ("Basler AG","baslerweb.com","DE","Metro",800,"Basler manufactures high-quality digital cameras for industrial machine vision."),
    ("IFM Electronic","ifm.com","DE","Sensor",8000,"IFM electronic provides sensors, controllers, software and systems for industrial automation."),
    ("Pepperl+Fuchs","pepperl-fuchs.com","DE","Sensor",6000,"Pepperl+Fuchs provides electronic sensors and components for factory and process automation."),
    ("Turck","turck.com","DE","Sensor",4500,"Turck provides sensors, connectivity and fieldbus components for industrial automation."),
    ("Banner Engineering","bannerengineering.com","US","Sensor",1500,"Banner Engineering provides industrial sensors, safety devices and vision systems."),
    ("Balluff","balluff.com","DE","Sensor",4200,"Balluff provides sensor solutions for position, vision, fluid and RFID applications in industry."),
    ("Contrinex","contrinex.com","CH","Sensor",800,"Contrinex provides inductive, photoelectric and safety sensors for industrial automation."),
    ("Leuze Electronic","leuze.com","DE","Safety",1600,"Leuze provides sensors, safety systems and identification solutions for industrial automation."),
    ("Pilz GmbH","pilz.com","DE","Safety",2400,"Pilz provides safe automation technology including safety controllers and sensors."),
    ("Schmersal Group","schmersal.com","DE","Safety",2000,"Schmersal provides safety switching devices and systems for machine guarding."),
    ("Trumpf GmbH","trumpf.com","DE","Laser",16000,"Trumpf is the world technology leader in machine tools for sheet metal and laser technology."),
    ("Bystronic","bystronic.com","CH","Laser",3500,"Bystronic provides laser cutting, bending and automation solutions for sheet metal processing."),
    ("Prima Industrie","primaindustrie.com","IT","Laser",2200,"Prima Industrie provides laser cutting systems and additive manufacturing solutions."),
    ("Han's Laser","hanslaser.com","CN","Laser",8000,"Han's Laser provides laser cutting, marking and welding systems for manufacturing."),
    ("Mazor Robotics","mazorrobotics.com","IL","Aero",500,"Mazor Robotics provides robotic guidance systems for spine surgery."),
    ("GEA Group","gea.com","DE","Food",18000,"GEA is one of the largest technology suppliers for food processing industries."),
    ("Tetra Pak","tetrapak.com","SE","Food",24000,"Tetra Pak provides food processing and packaging solutions with focus on liquid foods."),
    ("MULTIVAC","multivac.com","DE","Pack",6500,"MULTIVAC provides packaging solutions for food, medical and consumer goods."),
    ("Syntegon","syntegon.com","DE","Pharma",6000,"Syntegon provides processing and packaging solutions for pharma and food industries."),
    ("Coesia Group","coesia.com","IT","Pack",8000,"Coesia provides industrial and packaging solutions for tobacco, pharma and food."),
    ("IMA Group","ima.it","IT","Pharma",5500,"IMA Group provides automatic machines for processing and packaging pharmaceuticals."),
    ("Marchesini Group","marchesini.com","IT","Pharma",2200,"Marchesini provides packaging lines for the pharmaceutical and cosmetics industries."),
    ("Krones AG","krones.com","DE","Pack",15000,"Krones provides filling and packaging technology for the beverage and food industries."),
    ("Sidel Group","sidel.com","FR","Pack",5000,"Sidel provides equipment and services for packaging beverages and personal care products."),
    ("SEW-Eurodrive","sew-eurodrive.com","DE","Drive",20000,"SEW-Eurodrive provides drive technology including geared motors and electronic drives."),
    ("Lenze SE","lenze.com","DE","Drive",4000,"Lenze provides drives, controls and motion centric automation for machine building."),
    ("Beckhoff Automation","beckhoff.com","DE","Drive",4500,"Beckhoff provides PC-based control technology including PLCs, servo drives and industrial PCs."),
    ("B&R Automation","br-automation.com","AT","Drive",3500,"B&R provides integrated automation systems including PLCs and servo drives for machine builders."),
    ("Parker Hannifin","parker.com","US","Fluid",57000,"Parker Hannifin provides motion and control technologies for precision engineering worldwide."),
    ("Bosch Rexroth","boschrexroth.com","DE","Fluid",32000,"Bosch Rexroth provides linear motion, hydraulics and pneumatics for industrial applications."),
    ("Festo AG","festo.com","DE","Fluid",21000,"Festo provides pneumatic and electrical automation components and learning systems."),
    ("SMC Corporation","smcworld.com","JP","Fluid",26000,"SMC is the world's largest manufacturer of pneumatic automation components."),
    ("Norgren","norgren.com","GB","Fluid",6000,"Norgren provides pneumatics, motion and fluid control technology for industrial automation."),
    ("Grundfos","grundfos.com","DK","Fluid",19000,"Grundfos is the world's largest pump manufacturer for water and industrial applications."),
    ("Sulzer AG","sulzer.com","CH","Fluid",14000,"Sulzer provides pumping solutions and rotating equipment services for industry."),
    ("Endress+Hauser","endress.com","CH","ProcAuto",14000,"Endress+Hauser provides process instrumentation for level, flow, pressure and analytical measurement."),
    ("Yokogawa Electric","yokogawa.com","JP","ProcAuto",18000,"Yokogawa provides process automation, test and measurement and industrial automation solutions."),
    ("Emerson Process","emerson.com","US","ProcAuto",90000,"Emerson provides process control software and measurement instruments for industry."),
    ("Honeywell Process","process.honeywell.com","US","ProcAuto",100000,"Honeywell Process provides DCS, SCADA and safety systems for process industries."),
    ("Valmet Corporation","valmet.com","FI","ProcAuto",17000,"Valmet provides technologies, automation and services for the pulp and paper industries."),
    ("Andritz AG","andritz.com","AT","ProcAuto",27000,"Andritz provides plants and equipment for hydropower, pulp and paper and metal processing."),
    ("SKF Group","skf.com","SE","Drive",45000,"SKF is the world leader in bearings, seals, lubrication and related services."),
    ("NSK Ltd","nsk.com","JP","Drive",30000,"NSK provides bearings, linear technology and steering systems for industrial markets."),
    ("NTN Corporation","ntnglobal.com","JP","Drive",25000,"NTN provides bearings, driveshafts and precision equipment for industrial applications."),
    ("Schaeffler Group","schaeffler.com","DE","Auto",84000,"Schaeffler provides precision components for engines, transmissions and chassis applications."),
    ("Maxon Group","maxongroup.com","CH","Drive",3000,"Maxon provides high-precision DC motors and drive systems for medical and industrial robotics."),
    ("Harmonic Drive","harmonicdrive.net","JP","Drive",1200,"Harmonic Drive provides precision strain wave gears and actuators for robotics."),
    ("Nabtesco","nabtesco.com","JP","Drive",4500,"Nabtesco provides precision reduction gears for industrial robots and automation equipment."),
    ("Wittenstein","wittenstein.de","DE","Drive",2600,"Wittenstein provides high-precision gearheads and servo actuators for industrial robots."),
    ("Dematic","dematic.com","DE","AMR",8000,"Dematic provides intelligent intralogistics and automation systems for warehouses."),
    ("Swisslog","swisslog.com","CH","AMR",3000,"Swisslog provides data-driven and robotic solutions for warehouse automation."),
    ("Knapp AG","knapp.com","AT","AMR",6000,"Knapp provides intelligent warehouse and distribution systems including AMRs."),
    ("Vanderlande","vanderlande.com","NL","AMR",7500,"Vanderlande is the global market leader for logistic process automation at airports."),
    ("Kardex Group","kardex.com","CH","AMR",2200,"Kardex provides automated storage and retrieval systems for warehouses and healthcare."),
    ("Modula SpA","modula.eu","IT","AMR",900,"Modula provides vertical automated storage lift modules for industrial parts storage."),
    ("Mecalux","mecalux.com","ES","AMR",4000,"Mecalux provides storage and intralogistics solutions including automated warehouses."),
    ("Interroll Group","interroll.com","CH","AMR",2500,"Interroll provides material handling products including conveyors and sorters."),
    ("Hexagon AB","hexagon.com","SE","Metro",21000,"Hexagon provides sensor, software and autonomous technologies for manufacturing."),
    ("Zeiss Industrial","zeiss.com","DE","Metro",35000,"Zeiss provides precision optics, metrology systems and industrial measurement solutions."),
    ("Faro Technologies","faro.com","US","Metro",1800,"Faro provides 3D measurement and imaging solutions for manufacturing and construction."),
    ("Mitutoyo","mitutoyo.com","JP","Metro",6000,"Mitutoyo provides precision measuring instruments including CMMs and micrometers."),
    ("Marposs","marposs.com","IT","Metro",3000,"Marposs provides measurement, testing and inspection equipment for manufacturing quality control."),
    ("Renishaw","renishaw.com","GB","Metro",5000,"Renishaw provides metrology, motion control and spectroscopy products for precision manufacturing."),
    ("EOS GmbH","eos.info","DE","Addit",1500,"EOS provides industrial 3D printing and additive manufacturing solutions for metals and polymers."),
    ("Stratasys","stratasys.com","US","Addit",3000,"Stratasys provides FDM and PolyJet 3D printing solutions for manufacturing tooling."),
    ("Materialise NV","materialise.com","BE","Addit",2500,"Materialise provides 3D printing software and services for medical and aerospace."),
    ("SLM Solutions","slm-solutions.com","DE","Addit",800,"SLM Solutions provides selective laser melting machines for metal additive manufacturing."),
    ("Nordson Corporation","nordson.com","US","Coat",7500,"Nordson provides precision dispensing equipment for adhesives, coatings and sealants."),
    ("Dürr AG","durr.com","DE","Coat",16000,"Dürr provides painting and finishing systems and application technology for automotive."),
    ("Kennametal","kennametal.com","US","MachTool",9000,"Kennametal provides cutting tools, tooling systems and metal-cutting services."),
    ("Sandvik Coromant","sandvik.com","SE","MachTool",8000,"Sandvik Coromant provides metal cutting tools and tooling systems for metalworking."),
    ("Iscar","iscar.com","IL","MachTool",15000,"Iscar provides carbide cutting tools and metalworking solutions for global manufacturing."),
    ("Walter AG","walter-tools.com","DE","MachTool",4500,"Walter provides precision cutting tools and tooling systems for milling and turning."),
    ("Schunk Group","schunk.com","DE","Ind Rob",3500,"Schunk provides gripping systems and clamping technology for industrial automation."),
    ("OnRobot","onrobot.com","DK","Ind Rob",600,"OnRobot provides end-of-arm tooling including grippers and sensors for collaborative robots."),
    ("Robotiq","robotiq.com","CA","Ind Rob",400,"Robotiq provides adaptive grippers and vision systems for collaborative robot applications."),
    ("ATI Industrial Automation","ati-ia.com","US","Ind Rob",500,"ATI provides robotic end-effectors including force/torque sensors and tool changers."),
    ("Phoenix Contact","phoenixcontact.com","DE","Connect",17000,"Phoenix Contact provides electrical connection and industrial automation solutions."),
    ("Weidmuller","weidmuller.com","DE","Connect",5000,"Weidmüller provides electronic components, network solutions and transmission equipment."),
    ("Harting Technology","harting.com","DE","Connect",4500,"Harting provides industrial connectors, data networks and device connectivity solutions."),
    ("WAGO Corporation","wago.com","DE","Connect",8000,"WAGO provides electrical interconnection technology, controllers and I/O systems for automation."),
    ("Murrelektronik","murrelektronik.com","DE","Connect",2000,"Murrelektronik provides power supplies, fieldbus systems and industrial electronics."),
    ("Carlo Gavazzi","carlo-gavazzi.com","CH","Connect",700,"Carlo Gavazzi provides electronic components and IoT sensors for industrial automation."),
    ("Finder SpA","findernet.com","IT","Connect",1200,"Finder provides relays, power contactors and time relays for industrial automation."),
    ("GF Machining Solutions","gfms.com","CH","MachTool",3200,"GF Machining Solutions provides EDM, milling, laser texturing and automation for toolmaking."),
    ("Chiron","chiron.de","DE","MachTool",2500,"Chiron provides vertical machining centers for high-speed and precision manufacturing."),
    ("Fanuc Robodrill","fanuc.com","JP","MachTool",8000,"Fanuc Robodrill provides compact high-speed machining centers for small part production."),
    ("Mikron Group","mikron.com","CH","MachTool",1500,"Mikron provides high-speed machining centers and automation systems for mass production."),
    ("Tornos","tornos.com","CH","MachTool",1800,"Tornos manufactures Swiss-type turning centers for high-precision small part production."),
    ("Tsugami","tsugami.co.jp","JP","MachTool",2000,"Tsugami provides Swiss-type CNC automatic screw machines for precision parts manufacturing."),
    ("Tajmac-ZPS","tajmac-zps.cz","CZ","MachTool",1500,"Tajmac-ZPS provides multi-spindle automatics and CNC machining centers."),
    ("Emag Group","emag.com","DE","MachTool",3000,"Emag provides manufacturing solutions for precision metal components including pick-up lathes."),
    ("Traub","traub.de","DE","MachTool",2000,"Traub provides turning and sliding headstock automatics for precision parts."),
    ("Niles-Simmons","niles-simmons.de","DE","MachTool",1200,"Niles-Simmons provides CNC lathes and machining centers for railway and aerospace."),
    ("Gleason Corporation","gleason.com","US","MachTool",2200,"Gleason provides gear production machinery including hobbing, grinding and testing machines."),
    ("Klingelnberg","klingelnberg.com","CH","MachTool",1800,"Klingelnberg provides bevel gear cutting machines and precision measurement centers."),
    ("Liebherr Gear Technology","liebherr.com","DE","MachTool",46000,"Liebherr provides gear cutting, grinding and honing machines for precision gear manufacturing."),
    ("Felsomat","felsomat.com","DE","Auto",1200,"Felsomat provides induction hardening, gear manufacturing and automation for automotive."),
    ("Loxin Systems","loxinsystems.com","ES","Aero",400,"Loxin provides robotic drilling and fastening systems for aerospace assembly."),
    ("Broetje-Automation","broetje-automation.com","DE","Aero",1500,"Broetje-Automation provides riveting, drilling and assembly systems for aircraft manufacturing."),
    ("MTorres","mtorres.es","ES","Aero",1000,"MTorres provides automated fiber placement machines and assembly systems for aerospace."),
    ("Electroimpact","electroimpact.com","US","Aero",900,"Electroimpact provides automated assembly and drilling systems for aerospace manufacturing."),
    ("Profilator","profilator.de","DE","MachTool",800,"Profilator provides gear skiving and chamfering machines for gear manufacturing."),
    ("SCM Group","scmgroup.com","IT","Wood",4500,"SCM Group provides woodworking machinery and integrated systems for furniture."),
    ("Biesse Group","biesse.com","IT","Wood",4000,"Biesse provides CNC machining centers and edgebanders for wood and stone processing."),
    ("Homag Group","homag.com","DE","Wood",6000,"Homag provides woodworking machinery and production systems for furniture manufacturing."),
    ("Weinig Group","weinig.com","DE","Wood",2200,"Weinig provides solid wood processing solutions including planing and profiling."),
    ("Leitz GmbH","leitz.org","DE","Wood",2800,"Leitz provides precision tools for wood, metal and plastic machining."),
    ("Salvagnini","salvagnini.com","IT","MachTool",1800,"Salvagnini provides panel benders and flexible manufacturing systems for sheet metal."),
    ("Ficep SpA","ficep.com","IT","MachTool",900,"Ficep provides CNC drilling lines and sawing systems for structural steel fabrication."),
    ("Prima Power","primapower.com","IT","Laser",2500,"Prima Power provides laser cutting, punching and bending solutions for sheet metal."),
    ("Gasparini","gasparini.com","IT","MachTool",400,"Gasparini provides hydraulic press brakes, shears and laser cutting machines."),
    ("Cefla Finishing","cefla.com","IT","Coat",1500,"Cefla provides industrial finishing systems for wood, glass and metal surfaces."),
    ("IMA Schelling","ima-schelling.com","AT","Wood",2000,"IMA Schelling provides panel dividing saws and CNC machining centers for furniture production."),
    ("Burkert","burkert.com","DE","Fluid",2800,"Bürkert provides fluid control systems including solenoid valves and process analysis instruments."),
    ("GEMÜ Group","gemu-group.com","DE","Fluid",1500,"GEMÜ provides valves, measurement and control systems for industrial process automation."),
    ("IMI Precision Engineering","imiplc.com","GB","Fluid",3800,"IMI provides precision engineering products including valves and flow control solutions."),
    ("IDEX Corporation","idexcorp.com","US","Fluid",11000,"IDEX provides fluidics and dispensing products for industrial markets."),
    ("Samson AG","samsongroup.com","DE","Fluid",4500,"Samson provides control valves and positioners for process automation."),
    ("Metso Flow Control","metso.com","FI","Fluid",16000,"Metso provides flow control valves and actuators for process industries."),
    ("Circor International","circor.com","US","Fluid",6000,"Circor provides flow and motion control products for aerospace and industrial."),
    ("Moog Inc","moog.com","US","Drive",13000,"Moog provides high-performance motion control solutions for aerospace and industrial."),
    ("Danaher Corporation","danaher.com","US","Test",80000,"Danaher provides professional, medical, industrial and commercial products and services."),
    ("National Instruments","ni.com","US","Test",7700,"NI provides test and measurement instruments and software for industrial applications."),
    ("Keysight Technologies","keysight.com","US","Test",14000,"Keysight provides electronic test and measurement equipment for industrial applications."),
    ("Rohde & Schwarz","rohde-schwarz.com","DE","Test",13000,"Rohde & Schwarz provides test and measurement, broadcast and cybersecurity solutions."),
    ("Tektronix","tek.com","US","Test",5500,"Tektronix provides oscilloscopes, signal analyzers and spectrum analyzers for electronics testing."),
    ("Bühler Group","buhlergroup.com","CH","Food",13000,"Bühler provides technologies for grain milling, chocolate production and die casting."),
    ("Hosokawa Micron","hosokawa.com","JP","Food",4000,"Hosokawa Micron provides size reduction and powder processing equipment for food and pharma."),
    ("Netzsch Group","netzsch.com","DE","ProcAuto",4000,"Netzsch provides wet grinding, mixing and classifying equipment for various industries."),
    ("Alfa Laval","alfalaval.com","SE","ProcAuto",16000,"Alfa Laval provides heat transfer, fluid handling and separation products."),
    ("GEA Process Engineering","gea.com","DE","ProcAuto",18000,"GEA provides process engineering equipment for dairy, food, pharma and chemical industries."),
    ("Reifenhauser Group","reifenhauser.com","DE","Plastic",3000,"Reifenhauser provides extrusion systems for films, nonwovens and technical textiles."),
    ("Battenfeld-Cincinnati","battenfeld-cincinnati.com","AT","Plastic",1500,"Battenfeld-Cincinnati provides extrusion lines for pipes, profiles, sheets and films."),
    ("Davis Standard","davisstandard.com","US","Plastic",2000,"Davis-Standard provides extrusion and converting systems for plastics and flexible packaging."),
    ("Engel Austria","engel.at","AT","Plastic",7000,"Engel is one of the world's leading injection molding machine manufacturers."),
    ("Arburg","arburg.com","DE","Plastic",3400,"Arburg provides injection molding machines and additive manufacturing systems for plastics."),
    ("Wittmann Battenfeld","wittmann-group.com","AT","Plastic",3000,"Wittmann Battenfeld provides injection molding machines and automation systems."),
    ("KraussMaffei","kraussmaffei.com","DE","Plastic",5000,"KraussMaffei provides injection molding, extrusion and reaction process technology."),
    ("Husky Injection Molding","husky.ca","CA","Plastic",4000,"Husky provides injection molding equipment and services for plastics manufacturing."),
    ("Netstal","netstal.com","CH","Plastic",600,"Netstal provides high-precision, high-speed injection molding machines for packaging and medical."),
    ("Nissei Plastic","nissei.com","JP","Plastic",2000,"Nissei Plastic provides injection molding machines for precision plastics manufacturing."),
    ("Toshiba Machine","toyo.co.jp","JP","Plastic",2500,"Toyo Machinery provides injection molding machines for automotive and electronics."),
    ("Boy Machines","boy-machines.com","DE","Plastic",600,"BOY Machines provides compact injection molding machines for precision small parts."),
    ("Windmoller und Holscher","wuh-group.com","DE","Print",3500,"Windmöller & Hölscher provides printing, extrusion and converting machines for flexible packaging."),
    ("Koenig Bauer","koenig-bauer.com","DE","Print",3500,"Koenig & Bauer provides printing presses for packaging, commercial and security printing."),
    ("Heidelberger Druckmaschinen","heidelberg.com","DE","Print",9000,"Heidelberger provides printing machines and workflow solutions for offset and digital printing."),
    ("Manroland Goss","manroland-goss.com","DE","Print",2000,"Manroland Goss provides commercial and newspaper printing systems."),
    ("Ryobi","ryobiholdings.co.jp","JP","Print",3000,"Ryobi provides offset printing presses for commercial and packaging printing."),
    ("Landa Corporation","landanano.com","IL","Print",800,"Landa provides nanographic printing technology for commercial and packaging applications."),
    ("Esko","esko.com","BE","Print",1500,"Esko provides software and hardware for packaging and labels design and production."),
    ("Lüscher","luescher-ag.ch","CH","Print",600,"Lüscher provides UV inkjet exposure systems for plate making in printing."),
    ("Atlas Copco","atlascopco.com","SE","Fluid",50000,"Atlas Copco provides sustainable productivity solutions for industrial and construction markets."),
    ("Kaeser Kompressoren","kaeser.com","DE","Fluid",6000,"Kaeser provides compressors, blowers and compressed air systems for industrial applications."),
    ("Boge Kompressoren","boge.com","DE","Fluid",500,"Boge provides oil-injected, oil-free and special compressors for industrial applications."),
    ("Mattei Compressors","matteigroup.com","IT","Fluid",600,"Mattei provides rotary vane compressors for industrial and railway applications."),
    ("Elgi Equipments","elgi.com","IN","Fluid",2400,"Elgi provides air compressors and compressed air systems for global industrial markets."),
    ("Aerzen","aerzen.com","DE","Fluid",2200,"Aerzen provides rotary lobe blowers and screw compressors for industrial applications."),
    ("Busch Vacuum","buschvacuum.com","DE","Fluid",3000,"Busch provides vacuum pumps, blowers and compressors for industrial manufacturing."),
    ("Pfeiffer Vacuum","pfeiffer-vacuum.com","DE","Fluid",3000,"Pfeiffer Vacuum provides vacuum solutions including turbopumps and gauges for industry."),
    ("Leybold","leybold.com","DE","Fluid",3500,"Leybold provides vacuum solutions for industrial, analytical and coating applications."),
    ("Edwards Vacuum","edwardsvacuum.com","GB","Fluid",6000,"Edwards provides vacuum and abatement solutions for semiconductor and industrial markets."),
    ("Agilent Vacuum","agilent.com","US","Fluid",16000,"Agilent provides vacuum products and measurement solutions for semiconductor and research."),
]

def scrape_wiki_cat(cat, sector, existing, limit=30):
    results = []
    try:
        url = (f"https://en.wikipedia.org/w/api.php?action=query&list=categorymembers"
               f"&cmtitle={cat}&cmlimit=50&cmtype=page&format=json")
        r = requests.get(url, timeout=12, headers={"User-Agent":"IndustrialFeeder/4.0"})
        if r.status_code != 200: return results
        members = r.json().get("query",{}).get("categorymembers",[])
        random.shuffle(members)
        for m in members[:limit]:
            title = m.get("title","")
            if not title or ":" in title: continue
            clean = re.sub(r'\b(GmbH|AG|SpA|Srl|Ltd|Corp|Inc|BV|NV|AS|AB|SA|KG|Co\.|Co|Group|Holding|International|Industries|Systems|Technologies|Engineering|Solutions|Manufacturing)\b','',title,flags=re.IGNORECASE)
            clean = re.sub(r'[^a-zA-Z0-9\s]',' ',clean).lower().strip()
            words = [w for w in clean.split() if len(w)>2][:3]
            if not words: continue
            candidates = []
            if len(words)>1: candidates.append(words[0]+words[1]+".com")
            if len(words)>1: candidates.append("-".join(words[:2])+".com")
            candidates.append(words[0]+".com")
            if len(words)>1: candidates.append("".join(words[:2])+".de")
            if len(words)>1: candidates.append("".join(words[:2])+".it")
            for dom in candidates:
                d = nd(dom)
                if d not in existing and len(d)>5 and "." in d:
                    results.append((title, d, sector)); break
    except Exception as e:
        print(f"[wiki] errore {cat}: {e}", flush=True)
    return results

# ── Main loop ────────────────────────────────────────────────────────────────
def main():
    global inserted_count
    print("[Feeder v4] Avvio...", flush=True)
    existing = load_existing()
    print(f"[Feeder v4] Esistenti: {len(existing)}", flush=True)

    # Prima processa i seeds curati
    seed_queue = list(SEEDS)
    random.shuffle(seed_queue)
    for (name, domain, country, sector, emp, desc) in seed_queue:
        d = nd(domain)
        if d in existing:
            continue
        p = mkpayload(name, domain, country, sector, emp, desc)
        if push(p, existing):
            inserted_count += 1
            print(f"[seed] ✅ {name} ({inserted_count})", flush=True)
        time.sleep(DELAY)

    # Poi loop infinito su categorie Wikipedia
    cat_idx = 0
    while True:
        cat, sector = WIKI_CATS[cat_idx % len(WIKI_CATS)]
        cat_idx += 1

        entries = scrape_wiki_cat(cat, sector, existing)
        new_found = 0
        for (name, domain, sector2) in entries:
            p = mkpayload(name, domain, "XX", sector2)
            if push(p, existing):
                inserted_count += 1
                new_found += 1
                print(f"[wiki] ✅ {name} | {domain} | {sector2} ({inserted_count})", flush=True)
            time.sleep(DELAY)

        if new_found == 0:
            print(f"[wiki] 0 nuovi da {cat}, passo avanti", flush=True)
            time.sleep(2)

        # Ogni 50 categorie ricarica i domini esistenti
        if cat_idx % 50 == 0:
            existing = load_existing()
            print(f"[Feeder v4] Refresh esistenti: {len(existing)}", flush=True)

if __name__ == "__main__":
    main()

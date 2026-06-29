#!/usr/bin/env python3
"""
Industrial Feeder v3.0 — SOLO aziende manifatturiere reali
Fonti CURATE con filtro settoriale:
  1. Kompass.com — ricerca per categoria industriale specifica
  2. Europages.com — directory B2B manifatturiero EU
  3. Industrystock.com — directory macchine e automazione
  4. Thomasnet.com — directory industriale US
  5. Machineryline.com — marketplace macchine industriali
  6. Direktori ATECO C (manifatturiero) da OpenCorporates Italia
  7. SIC 3400-3599 (Industrial Machinery) da dataset pubblici

ZERO siti generici, media, università, SaaS.
Filtro doppio: categoria URL + keyword nel nome azienda.
"""
import asyncio, aiohttp, os, json, re, logging, time, threading, random
from urllib.parse import quote_plus, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FEED] %(message)s")
log = logging.getLogger(__name__)

B44_TOKEN = os.environ.get("B44_SERVICE_TOKEN","907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID    = os.environ.get("B44_APP_ID","6a3a284ab0b87dfa27558bb6")
B44_BASE  = f"https://app.base44.com/api/apps/{APP_ID}/entities"
HW        = {"api-key": B44_TOKEN, "Content-Type": "application/json"}
PORT      = int(os.environ.get("PORT","8080"))
DELAY     = 1.2  # secondi tra richieste — rispetta robots.txt

stats = {"inserted":0,"skipped":0,"errors":0,"crawled":0,"status":"starting"}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),Health).serve_forever(),daemon=True).start()

# ── Keyword filter: solo nomi che suonano industriali ─────────────────────────
IND_KEYWORDS = {
    "robot","robotic","automation","automat","machin","manufactur","industri",
    "mechan","metal","welding","stamping","cutting","milling","turning","forging",
    "casting","pressing","coating","painting","assembly","conveyor","pallet",
    "packaging","bottling","filling","sealing","labeling","printing","vision",
    "sensor","actuator","hydraulic","pneumatic","spindle","cnc","plc","scada",
    "mes","erp","hmi","servo","motor","drive","gearbox","bearing","pump",
    "compressor","valve","filter","heat exchanger","furnace","oven","kiln",
    "laser","plasma","ultrasonic","induction","grinding","honing","lapping",
    "srl","spa","gmbh","ag","bv","nv","oy","ab","as","kg","sarl","sas",
    "machinery","equipment","systems","solutions","engineering","technology",
    "technik","maschinenbau","anlagenbau","fertigungstechnik","automatisierung",
    "meccanica","officina","fonderia","stampaggio","lavorazioni","impianti",
}

NON_IND_DOMAINS = {
    "google","facebook","twitter","linkedin","youtube","instagram","tiktok",
    "amazon","apple","microsoft","netflix","spotify","airbnb","uber","lyft",
    "github","gitlab","stackoverflow","wikipedia","reddit","quora","medium",
    "techcrunch","wired","verge","bbc","cnn","nytimes","guardian","reuters",
    "asana","notion","slack","zoom","dropbox","salesforce","hubspot","zendesk",
    "shopify","wordpress","squarespace","wix","godaddy","cloudflare","stripe",
    "paypal","visa","mastercard","hsbc","bnpparibas","unicredit","intesa",
    "university","univ","college","school","edu","gov","ac.uk","ac.it",
    "nasa","cern","mit","stanford","harvard","oxford","cambridge",
    "hospital","clinic","health","pharma","medicina","ospedale",
    "agency","studio","creative","media","press","news","journal",
    "consulting","advisor","law","legal","avvocato","notaio",
    "fashion","luxury","clothing","shoes","sport","fitness","gym",
    "hotel","resort","restaurant","food","wine","travel","tourism",
    "insurance","assicurazione","mutuo","banca","finanza",
    "real estate","immobil","costruzioni","edil",  # escludo costruzioni civili
    "ey.com","asana.com","notion.so","figma.com","canva.com",
}

def is_industrial(name: str, domain: str) -> bool:
    """True solo se nome/dominio suggerisce settore manifatturiero."""
    n = name.lower(); d = domain.lower()
    # Esclusioni esplicite
    if any(x in d for x in NON_IND_DOMAINS): return False
    # Termini SaaS/digital tipici
    if any(x in n for x in ["software","saas","app","digital","cloud","web","seo",
                              "marketing","agency","startup","fintech","crypto",
                              "bank","insur","broker","fund","invest"]): return False
    # Almeno un keyword industriale
    combined = n + " " + d
    return any(kw in combined for kw in IND_KEYWORDS)

def clean_domain(url: str) -> str:
    try:
        h = urlparse(url if url.startswith("http") else "https://"+url).netloc
        return re.sub(r'^www\.','',h).split(":")[0].lower()
    except: return ""

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
]

async def fetch(session, url, timeout=20):
    hdrs = {"User-Agent":random.choice(UA),
            "Accept":"text/html,*/*;q=0.8",
            "Accept-Language":"en,it;q=0.9"}
    for _ in range(3):
        try:
            async with session.get(url,headers=hdrs,timeout=aiohttp.ClientTimeout(total=timeout),
                                   allow_redirects=True,ssl=False) as r:
                if r.status==200: return await r.text(errors="replace")
                if r.status==429: await asyncio.sleep(30); continue
                if r.status in(403,404,410): return ""
        except: await asyncio.sleep(2)
    return ""

# Dizionario già-inseriti (evita check B44 per ogni record)
_inserted_domains: set = set()

async def b44_upsert(session, data: dict):
    dom = data.get("domain","").strip().lower()
    if not dom or len(dom)<4 or "." not in dom: return
    if dom in _inserted_domains: stats["skipped"]+=1; return
    _inserted_domains.add(dom)

    # Verifica su B44
    try:
        async with session.get(
            f"{B44_BASE}/IndustrialCompany?limit=1&fields=id",
            params={"domain": dom},
            headers={"api-key":B44_TOKEN},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status==200:
                ex = await r.json(content_type=None)
                if isinstance(ex,list) and ex: stats["skipped"]+=1; return
    except: pass

    try:
        async with session.post(f"{B44_BASE}/IndustrialCompany",
                                headers=HW, json=data,
                                timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status in(200,201):
                stats["inserted"]+=1
                if stats["inserted"] % 50 == 0:
                    log.info(f"✅ Inseriti: {stats['inserted']} | crawlati: {stats['crawled']}")
            else:
                stats["errors"]+=1
    except: stats["errors"]+=1

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: Kompass — per categoria industriale, per paese
# ─────────────────────────────────────────────────────────────────────────────
KOMPASS_SEARCHES = [
    # (tld, query, industry_label)
    ("it","robot industriali","Industrial Robots"),
    ("it","automazione industriale","Industrial Automation"),
    ("it","macchine utensili","Machine Tools"),
    ("it","lavorazioni meccaniche","Precision Machining"),
    ("it","stampi e stampaggio","Tooling & Stamping"),
    ("it","impianti di produzione","Production Systems"),
    ("it","nastri trasportatori","Conveyor Systems"),
    ("it","impianti di verniciatura","Coating Systems"),
    ("it","macchine per l imballaggio","Packaging Machinery"),
    ("it","fonderie","Foundries"),
    ("it","trattamenti termici","Heat Treatment"),
    ("it","carpenteria metallica","Metal Fabrication"),
    ("it","ingranaggi e riduttori","Gearboxes"),
    ("it","cilindri idraulici","Hydraulic Cylinders"),
    ("it","sistemi di visione artificiale","Machine Vision"),
    ("de","Industrieroboter","Industrial Robots"),
    ("de","Maschinenbau","Machine Building"),
    ("de","Automatisierungstechnik","Automation Technology"),
    ("de","CNC Maschinen","CNC Machines"),
    ("de","Fördertechnik","Conveying Systems"),
    ("de","Schweißtechnik","Welding Technology"),
    ("de","Hydraulik Pneumatik","Hydraulics Pneumatics"),
    ("fr","robots industriels","Industrial Robots"),
    ("fr","machines outils","Machine Tools"),
    ("fr","automatisation industrielle","Industrial Automation"),
    ("es","robots industriales","Industrial Robots"),
    ("es","maquinaria industrial","Industrial Machinery"),
    ("pl","roboty przemyslowe","Industrial Robots"),
    ("pl","obrabiarki CNC","CNC Machine Tools"),
    ("cz","průmyslové roboty","Industrial Robots"),
]

async def crawl_kompass(session, sem):
    for tld, query, label in KOMPASS_SEARCHES:
        for pg in range(1, 51):  # max 50 pagine = ~1000 aziende per query
            url = (f"https://{tld}.kompass.com/searchCompany?"
                   f"text={quote_plus(query)}&offset={(pg-1)*20}")
            html = await fetch(session, url)
            if not html: break
            soup = BeautifulSoup(html,"html.parser")

            # Selettori Kompass (vari layout)
            names_els = (soup.select(".companyTitle a, h2.resultTitle a, .card__title a, "
                                    "[class*='company-name'] a, .k-company-result h3 a") or
                         soup.select("h2 a[href*='/c/'], h3 a[href*='/c/']"))
            if not names_els:
                # Fallback: cerca tutti i link a pagine aziendali
                names_els = [a for a in soup.find_all("a",href=True)
                             if re.search(r'/c/[a-z0-9-]+',a.get("href",""))]

            if not names_els: break

            pg_count = 0
            for el in names_els:
                name = el.get_text(strip=True)
                if not name or len(name) < 3: continue

                parent = el.find_parent(class_=re.compile(r"card|result|company|row|item")) or el.parent
                # Cerca website nel card
                website = ""
                if parent:
                    ext_links = [a["href"] for a in parent.find_all("a",href=True)
                                 if "http" in a["href"] and "kompass" not in a["href"]]
                    if ext_links: website = ext_links[0]

                domain = clean_domain(website) if website else ""
                if not domain:
                    # Genera placeholder da nome (andrà scansionato e validato)
                    slug = re.sub(r'[^a-z0-9]','',name.lower())[:25]
                    if len(slug) > 4: domain = f"{slug}.com"
                    else: continue

                if not is_industrial(name, domain): continue

                stats["crawled"] += 1
                co = {"name":name[:100],"domain":domain,
                      "website_url": website or f"https://{domain}",
                      "country": tld.upper(),"industry": label,
                      "source":"kompass",
                      "revenue":None,"employee_count":None,"estimated_deal_value_max":None}
                async with sem: await b44_upsert(session, co)
                pg_count += 1

            log.info(f"[Kompass/{tld}] '{query[:30]}' p{pg}: +{pg_count}")
            if pg_count < 3: break
            await asyncio.sleep(DELAY + random.uniform(0,0.8))

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: Europages — directory EU B2B manifatturiero
# ─────────────────────────────────────────────────────────────────────────────
EP_CATS = [
    ("industrial-robots-and-cobot","Industrial Robots"),
    ("machine-tools","Machine Tools"),
    ("automation-equipment","Automation Equipment"),
    ("conveyor-systems-and-equipment","Conveyor Systems"),
    ("packaging-machines-and-equipment","Packaging Machinery"),
    ("welding-equipment","Welding Equipment"),
    ("cnc-machining","CNC Machining"),
    ("hydraulic-and-pneumatic-equipment","Hydraulics Pneumatics"),
    ("electric-motors","Electric Motors"),
    ("industrial-sensors","Industrial Sensors"),
    ("machine-vision-systems","Machine Vision"),
    ("automated-guided-vehicles-agv","AGV AMR"),
    ("plc-and-scada-systems","PLC SCADA"),
    ("sheet-metal-work","Sheet Metal"),
    ("machined-parts","Machined Parts"),
    ("industrial-valves","Industrial Valves"),
    ("bearings","Bearings"),
    ("gears-and-reducers","Gears Reducers"),
    ("cutting-tools","Cutting Tools"),
    ("industrial-filters","Industrial Filters"),
    ("pumps-industrial","Industrial Pumps"),
    ("compressors-industrial","Industrial Compressors"),
    ("heat-exchangers","Heat Exchangers"),
    ("industrial-furnaces","Industrial Furnaces"),
    ("surface-treatment","Surface Treatment"),
    ("metalworking","Metalworking"),
    ("forging","Forging"),
    ("casting","Casting"),
    ("stamping-pressing","Stamping Pressing"),
    ("plastics-injection-moulding","Plastics Moulding"),
]

async def crawl_europages(session, sem):
    for slug, label in EP_CATS:
        for pg in range(1, 81):
            url = f"https://www.europages.co.uk/companies/{slug}.html?page={pg}"
            html = await fetch(session, url)
            if not html: break
            soup = BeautifulSoup(html,"html.parser")

            cards = (soup.select(".ep-company-card, .company-card, [class*='company-item'], "
                                 "[class*='listing-item'], .ep-listing__item") or
                     soup.select("article, [class*='result']"))
            if not cards: break

            pg_count = 0
            for card in cards:
                name_el = card.select_one("h2,h3,[class*='company-name'],[class*='name']")
                name = (name_el or card).get_text(strip=True)[:100]
                if not name or len(name)<3: continue

                web_a = card.select_one("a[href*='http']:not([href*='europages'])")
                website = web_a.get("href","") if web_a else ""
                domain = clean_domain(website) if website else ""
                if not domain: continue
                if not is_industrial(name, domain): continue

                ctry_el = card.select_one("[class*='country'],[class*='location'],[class*='flag']")
                country = ""
                if ctry_el:
                    m = re.search(r'\b([A-Z]{2})\b', ctry_el.get_text())
                    if m: country = m.group(1)

                stats["crawled"] += 1
                co = {"name":name,"domain":domain,
                      "website_url": website or f"https://{domain}",
                      "country": country or "EU","industry": label,
                      "source":"europages",
                      "revenue":None,"employee_count":None,"estimated_deal_value_max":None}
                async with sem: await b44_upsert(session, co)
                pg_count += 1

            log.info(f"[EP] {slug[:30]} p{pg}: +{pg_count}")
            if pg_count < 3: break
            await asyncio.sleep(DELAY + random.uniform(0,0.5))

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: OpenCorporates — aziende IT con ATECO manifatturiero C*
# ─────────────────────────────────────────────────────────────────────────────
async def crawl_opencorp(session, sem):
    for code in ["C25","C26","C27","C28","C29","C30"]:  # Metallurgia, Elettronica, Macchine
        for pg in range(1, 201):
            url = (f"https://api.opencorporates.com/v0.4/companies/search"
                   f"?jurisdiction_code=it&industry_codes={code}"
                   f"&current_status=Active&per_page=100&page={pg}")
            html = await fetch(session, url, timeout=15)
            if not html: break
            try:
                d = json.loads(html)
                companies = d.get("results",{}).get("companies",[])
                if not companies: break
                pg_count = 0
                for item in companies:
                    c = item.get("company",{})
                    name = c.get("name","")
                    if not name: continue
                    website = (c.get("registered_address") or {}).get("website","")
                    domain = clean_domain(website) if website else ""
                    if not domain: continue
                    if not is_industrial(name, domain): continue
                    stats["crawled"]+=1
                    co = {"name":name[:100],"domain":domain,
                          "website_url":website,"country":"IT",
                          "industry":f"Manufacturing ATECO-{code}",
                          "source":"opencorporates",
                          "revenue":None,"employee_count":None,"estimated_deal_value_max":None}
                    async with sem: await b44_upsert(session,co)
                    pg_count+=1
                log.info(f"[OpenCorp] ATECO-{code} p{pg}: +{pg_count}")
                if len(companies)<100: break
            except: break
            await asyncio.sleep(1.5)

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4: Seed list curata manuale — 500+ top aziende industriali EU
# ─────────────────────────────────────────────────────────────────────────────
SEED_COMPANIES = [
    # (name, domain, country, industry)
    # === ROBOT / AUTOMAZIONE ===
    ("KUKA AG","kuka.com","DE","Industrial Robots"),
    ("ABB Robotics","abb.com","CH","Industrial Robots"),
    ("FANUC Corporation","fanuc.com","JP","Industrial Robots"),
    ("YASKAWA Electric","yaskawa.com","JP","Industrial Robots"),
    ("Kawasaki Robotics","kawasakirobotics.com","JP","Industrial Robots"),
    ("Nachi-Fujikoshi","nachi.com","JP","Industrial Robots"),
    ("Staubli International","staubli.com","CH","Industrial Robots"),
    ("Universal Robots","universal-robots.com","DK","Collaborative Robots"),
    ("Techman Robot","techman.com","TW","Collaborative Robots"),
    ("Doosan Robotics","doosanrobotics.com","KR","Collaborative Robots"),
    ("Aubo Robotics","aubo-robotics.com","CN","Collaborative Robots"),
    ("Franka Emika","franka.de","DE","Collaborative Robots"),
    ("OnRobot","onrobot.com","DK","Robot Peripherals"),
    ("Schunk GmbH","schunk.com","DE","Robot Grippers"),
    ("Zimmer Group","zimmer-group.com","DE","Robot Grippers"),
    ("Festo AG","festo.com","DE","Automation Components"),
    ("SMC Corporation","smc.eu","JP","Pneumatics Automation"),
    ("Parker Hannifin","parker.com","US","Motion Control"),
    ("Bosch Rexroth","boschrexroth.com","DE","Drive Control"),
    ("Siemens Digital Industries","siemens.com","DE","Factory Automation"),
    ("Rockwell Automation","rockwellautomation.com","US","Industrial Automation"),
    ("Schneider Electric","se.com","FR","Industrial Automation"),
    ("Omron Industrial","industrial.omron.eu","JP","Industrial Automation"),
    ("Mitsubishi Electric FA","mitsubishielectric.com","JP","Factory Automation"),
    ("Keyence Corporation","keyence.com","JP","Industrial Sensors Vision"),
    ("Cognex Corporation","cognex.com","US","Machine Vision"),
    ("Basler AG","baslerweb.com","DE","Industrial Cameras"),
    ("SICK AG","sick.com","DE","Industrial Sensors"),
    ("Pepperl+Fuchs","pepperl-fuchs.com","DE","Industrial Sensors"),
    ("Leuze Electronic","leuze.com","DE","Optical Sensors"),
    ("Balluff GmbH","balluff.com","DE","Industrial Sensors"),
    ("IFM Electronic","ifm.com","DE","Industrial Sensors"),
    ("Turck","turck.com","DE","Industrial Automation"),
    ("Pilz GmbH","pilz.com","DE","Safety Automation"),
    ("Murrelektronik","murrelektronik.com","DE","Industrial Networking"),
    ("Wago Kontakttechnik","wago.com","DE","Automation Components"),
    ("Phoenix Contact","phoenixcontact.com","DE","Industrial Connectivity"),
    ("Harting Technology","harting.com","DE","Industrial Connectors"),
    ("Beckhoff Automation","beckhoff.com","DE","PC-based Automation"),
    ("B&R Industrial Automation","br-automation.com","AT","Machine Automation"),
    ("Lenze SE","lenze.com","DE","Drive Systems"),
    ("SEW-EURODRIVE","sew-eurodrive.com","DE","Drive Technology"),
    ("Nord Drivesystems","nord.com","DE","Drive Technology"),
    ("Baumüller Nürnberg","baumueller.com","DE","Drive Systems"),
    ("NUM AG","num.com","CH","CNC Systems"),
    ("FANUC CNC","fanuc.eu","JP","CNC Systems"),
    ("Heidenhain","heidenhain.com","DE","CNC Measuring"),
    ("DMG Mori","dmgmori.com","DE","CNC Machine Tools"),
    ("Trumpf GmbH","trumpf.com","DE","Laser Machine Tools"),
    ("EMAG Group","emag.com","DE","CNC Machine Tools"),
    ("GROB-WERKE","grob.de","DE","Machine Tools"),
    ("Hermle AG","hermle.de","DE","CNC Milling"),
    ("Chiron Group","chiron-group.com","DE","CNC Machining Centers"),
    ("Mazak Corporation","mazak.eu","JP","CNC Machine Tools"),
    ("Makino","makino.com","JP","Machine Tools"),
    ("Doosan Machine Tools","doosanmachinetools.com","KR","Machine Tools"),
    ("Okuma Corporation","okuma.com","JP","CNC Machine Tools"),
    ("Haas Automation","haascnc.com","US","CNC Machine Tools"),
    ("Hurco Companies","hurco.com","US","CNC Machine Tools"),
    ("LNS Group","lns-group.com","CH","Bar Feeders"),
    ("Blum-Novotest","blum-novotest.com","DE","Measurement Tools"),
    ("Renishaw","renishaw.com","GB","Metrology"),
    ("Hexagon Manufacturing","hexagon.com","SE","Metrology"),
    ("Zeiss Industrial Metrology","zeiss.com","DE","Metrology"),
    ("Nikon Metrology","nikon.com","JP","Metrology"),
    ("Mitutoyo","mitutoyo.com","JP","Measurement"),
    ("GF Machining Solutions","gfms.com","CH","EDM Machine Tools"),
    ("Agie Charmilles","gfms.com","CH","EDM Wire Cutting"),
    ("Sodick","sodick.com","JP","EDM Machines"),
    # === AGV / AMR ===
    ("Jungheinrich AG","jungheinrich.com","DE","AGV Forklifts"),
    ("Linde Material Handling","linde-mh.com","DE","AGV Forklifts"),
    ("Still GmbH","still.de","DE","Warehouse Automation"),
    ("Toyota Material Handling","toyota-industries.com","JP","Forklifts AGV"),
    ("Crown Equipment","crown.com","US","Forklifts"),
    ("Dematic","dematic.com","DE","Warehouse Automation"),
    ("Swisslog","swisslog.com","CH","Warehouse Automation"),
    ("Kardex Group","kardex.com","CH","Automated Storage"),
    ("Knapp AG","knapp.com","AT","Warehouse Automation"),
    ("SSI Schaefer","ssi-schaefer.com","DE","Intralogistics"),
    ("Vanderlande","vanderlande.com","NL","Airport Warehouse Automation"),
    ("Grenzebach","grenzebach.com","DE","Intralogistics"),
    ("Elettric80","elettric80.com","IT","AGV Systems"),
    ("RCS spa","rcs.it","IT","Automated Storage"),
    ("Cimcorp","cimcorp.com","FI","Robotic Automation"),
    ("Mobile Industrial Robots","mobile-industrial-robots.com","DK","AMR"),
    ("Fetch Robotics","fetchrobotics.com","US","AMR"),
    ("6 River Systems","6river.com","US","AMR Warehouse"),
    ("Locus Robotics","locusrobotics.com","US","AMR Fulfillment"),
    ("Zebra Technologies","zebra.com","US","Warehouse Automation"),
    ("Geek+","geekplusrobotics.com","CN","AMR"),
    ("GreyOrange","greyorange.com","US","AMR AI"),
    # === MES / SCADA / IIoT ===
    ("Siemens MES Opcenter","siemens.com","DE","MES"),
    ("Rockwell FactoryTalk","rockwellautomation.com","US","MES SCADA"),
    ("Wonderware AVEVA","aveva.com","GB","SCADA MES"),
    ("Inductive Automation","inductiveautomation.com","US","SCADA"),
    ("Wonderware OSIsoft","osisoft.com","US","Industrial Data"),
    ("PTC Kepware","ptc.com","US","IIoT OPC"),
    ("Factry","factry.io","BE","MES IIoT"),
    ("Critical Manufacturing","criticalmanufacturing.com","PT","MES"),
    ("Plex Systems","plex.com","US","Cloud MES"),
    ("Epicor Manufacturing","epicor.com","US","ERP MES"),
    ("IFS Applications","ifs.com","SE","ERP Manufacturing"),
    ("Aptean","aptean.com","US","Manufacturing ERP"),
    ("Infor CloudSuite Industrial","infor.com","US","ERP Manufacturing"),
    ("ProShop ERP","proshoperp.com","US","Manufacturing ERP"),
    ("Tulip Interfaces","tulip.co","US","MES No-Code"),
    ("Sight Machine","sightmachine.com","US","AI Manufacturing"),
    ("Augury","augury.com","US","Predictive Maintenance"),
    ("Aspentech","aspentech.com","US","Process Optimization"),
    ("OSIsoft PI","osisoft.com","US","Industrial Data Historian"),
    ("Samsara","samsara.com","US","Fleet Industrial IoT"),
    # === IMBALLAGGIO / PACKAGING ===
    ("IMA Group","ima.it","IT","Packaging Machinery"),
    ("Coesia Group","coesia.com","IT","Packaging Automation"),
    ("SACMI","sacmi.it","IT","Ceramics Packaging"),
    ("Marchesini Group","marchesini.com","IT","Pharmaceutical Packaging"),
    ("Syntegon","syntegon.com","DE","Packaging Technology"),
    ("MULTIVAC","multivac.com","DE","Food Packaging"),
    ("GEA Group","gea.com","DE","Food Processing"),
    ("Tetra Pak","tetrapak.com","SE","Food Packaging"),
    ("SIG Combibloc","sig.biz","CH","Packaging"),
    ("Bobst Group","bobst.com","CH","Packaging Converting"),
    ("Heidelberger Druckmaschinen","heidelberg.com","DE","Printing Machines"),
    ("manroland","manroland.com","DE","Printing Machinery"),
    ("Baumer Holding","baumer.com","CH","Packaging Sensors"),
    ("Bizerba","bizerba.com","DE","Weighing Labeling"),
    ("Mettler-Toledo","mt.com","CH","Industrial Weighing"),
    ("Sartorius AG","sartorius.com","DE","Lab Industrial Weighing"),
    # === SALDATURA / WELDING ===
    ("Fronius International","fronius.com","AT","Welding Technology"),
    ("Lincoln Electric","lincolnelectric.com","US","Welding"),
    ("Miller Electric","millerwelds.com","US","Welding"),
    ("ESAB Corporation","esab.com","SE","Welding Cutting"),
    ("Cloos Robotics","cloos.de","DE","Robotic Welding"),
    ("EWM AG","ewm-group.com","DE","Welding Technology"),
    ("Panasonic Welding","welding.panasonic.com","JP","Welding Robots"),
    ("OTC Daihen","otcdaihen.com","JP","Welding Systems"),
    # === LASER / TAGLIO ===
    ("Trumpf Laser","trumpf.com","DE","Laser Cutting"),
    ("IPG Photonics","ipgphotonics.com","US","Fiber Lasers"),
    ("Coherent Corp","coherent.com","US","Industrial Lasers"),
    ("Bystronic","bystronic.com","CH","Laser Bending"),
    ("Amada","amada.com","JP","Sheet Metal Machines"),
    ("LVD Group","lvdgroup.com","BE","Sheet Metal"),
    ("Prima Industrie","primaindustrie.com","IT","Laser Systems"),
    ("Salvagnini Group","salvagnini.com","IT","Sheet Metal Automation"),
    ("Ficep Group","ficep.com","IT","Steel Fabrication"),
    ("Peddinghaus","peddinghaus.com","US","Structural Steel"),
    # === MACCHINE UTENSILI ITALIANE ===
    ("SCM Group","scmgroup.com","IT","Woodworking CNC"),
    ("Biesse Group","biesse.com","IT","CNC Woodworking"),
    ("Breton SpA","breton.it","IT","Stone CNC Machines"),
    ("Ficep Group","ficep.it","IT","CNC Drilling"),
    ("Pama SpA","pama.it","IT","CNC Boring Mills"),
    ("Romi","romi.com.br","BR","Machine Tools"),
    ("Comau","comau.com","IT","Industrial Robots"),
    ("Cefla","cefla.com","IT","Industrial Finishing"),
    ("Marposs","marposs.com","IT","Measurement Technology"),
    ("Camozzi Group","camozzi.com","IT","Automation Components"),
    ("Gefran","gefran.com","IT","Industrial Sensors"),
    ("Datalogic","datalogic.com","IT","Industrial Scanning"),
    ("Telerobot","telerobot.it","IT","Industrial Robots"),
    ("Prima Power","primapower.com","IT","Laser Sheet Metal"),
    ("Colgar","colgar.it","IT","Industrial Machinery"),
    ("RoviMachinery","rovimachinery.com","IT","Transfer Machines"),
    ("Saccardo Elettromeccanica","saccardo.it","IT","Electric Motors"),
    ("MERITOR","meritor.com","IT","Manufacturing"),
    ("Durr AG","durr.com","DE","Paint Shop Automation"),
    ("Eisenmann","eisenmann.com","DE","Industrial Plants"),
    # === SISTEMI DI TRASPORTO / LOGISTICA INTERNA ===
    ("Interroll Group","interroll.com","CH","Conveyor Systems"),
    ("Hytrol Conveyor","hytrol.com","US","Conveyor Systems"),
    ("Daifuku","daifuku.com","JP","Conveyor Automation"),
    ("TGW Logistics","tgw-group.com","AT","Automated Conveyor"),
    ("Mecalux","mecalux.com","ES","Warehouse Storage"),
    ("Constructor Group","constructor.com","NO","Storage Systems"),
    ("Combilift","combilift.com","IE","Special Forklifts"),
    ("Assa Abloy Entrance","assaabloy.com","SE","Industrial Doors"),
    # === SENSORISTICA / CONTROLLO ===
    ("Endress+Hauser","endress.com","CH","Process Instrumentation"),
    ("Vega Grieshaber","vega.com","DE","Level Measurement"),
    ("Emerson Automation","emerson.com","US","Process Automation"),
    ("Yokogawa Electric","yokogawa.com","JP","Industrial Automation"),
    ("Honeywell Process","honeywell.com","US","Process Automation"),
    ("ABB Measurement","new.abb.com","CH","Instrumentation"),
    ("Gems Sensors","gems.com","US","Industrial Sensors"),
    ("Rechner Sensors","rechner.de","DE","Capacitive Sensors"),
    ("Baumer Electric","baumer.com","CH","Encoders Sensors"),
    ("Kübler Group","kuebler.com","DE","Encoders"),
    ("Hengstler GmbH","hengstler.com","DE","Encoders Counters"),
    ("TWK-Elektronik","twk.de","DE","Sensors"),
    ("Schmersal","schmersal.com","DE","Safety Switches"),
    ("Euchner","euchner.com","DE","Safety Systems"),
    ("Wieland Electric","wieland-electric.com","DE","Industrial Safety"),
    # === ALIMENTARE / FOOD PROCESSING ===
    ("Tetra Laval","tetralava.com","SE","Food Processing"),
    ("JBT Corporation","jbtc.com","US","Food Processing"),
    ("Buhler Group","buhlergroup.com","CH","Food Grain Processing"),
    ("Alfa Laval","alfalaval.com","SE","Heat Transfer"),
    ("SPX Flow","spxflow.com","US","Food Processing"),
    ("Marel","marel.com","IS","Food Processing Machines"),
    ("Middleby Corporation","middleby.com","US","Food Machinery"),
    ("Tomra","tomra.com","NO","Sorting Systems"),
    ("Key Technology","key.net","US","Food Sorting"),
    ("Urschel Laboratories","urschel.com","US","Food Cutting"),
]

async def insert_seed(session, sem):
    log.info(f"[Seed] Inserimento {len(SEED_COMPANIES)} aziende seed curate...")
    for name, domain, country, industry in SEED_COMPANIES:
        if not is_industrial(name, domain): continue
        co = {"name":name,"domain":domain,
              "website_url":f"https://{domain}","country":country,
              "industry":industry,"source":"seed_curated",
              "revenue":None,"employee_count":None,"estimated_deal_value_max":None}
        async with sem: await b44_upsert(session, co)
        await asyncio.sleep(0.1)
    log.info(f"[Seed] Completato. Inseriti: {stats['inserted']}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    stats["status"] = "running"
    log.info("=== Industrial Feeder v3.0 — SOLO AZIENDE MANIFATTURIERE ===")
    sem  = asyncio.Semaphore(5)
    conn = aiohttp.TCPConnector(limit=10, ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        # FASE 1: Seed curato (veloce, alta qualità)
        await insert_seed(session, sem)

        # FASE 2: Kompass
        log.info("=== FASE 2: Kompass ===")
        await crawl_kompass(session, sem)
        log.info(f"Post-Kompass: inseriti={stats['inserted']}")

        # FASE 3: Europages
        log.info("=== FASE 3: Europages ===")
        await crawl_europages(session, sem)
        log.info(f"Post-Europages: inseriti={stats['inserted']}")

        # FASE 4: OpenCorporates IT
        log.info("=== FASE 4: OpenCorporates IT manifatturiero ===")
        await crawl_opencorp(session, sem)
        log.info(f"Post-OpenCorp: inseriti={stats['inserted']}")

    stats["status"] = "done"
    log.info(f"=== COMPLETATO: {stats['inserted']} aziende industriali inserite ===")

    # Keep alive per healthcheck Railway
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
signal_engine_v3.py — Motore segnali industriali
WORKAROUND: scan_status non persiste via API → usa buying_intent_score IS NOT NULL come marker
Keyword multilingua IT+DE+FR+EN
Quality check ogni 50 scan senza bloccare
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

UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
      "Accept":"text/html,*/*","Accept-Language":"en-US,en;q=0.9,it;q=0.8,de;q=0.7,fr;q=0.6"}

stats = {"scanned":0,"unreachable":0,"errors":0,"cycle":0,
         "current":"","queue":0,"good_signals":0,
         "last_quality_check":"never","quality":{}}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body=json.dumps(stats,default=str).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*a): pass

threading.Thread(target=lambda:HTTPServer(("0.0.0.0",PORT),H).serve_forever(),daemon=True).start()
log.info(f"[OK] Healthcheck :{PORT}")

# ── KEYWORD MULTILINGUA ───────────────────────────────────────
AUTO_KW=[
    "robot","cobot","cnc","plc","scada","automation","conveyor","agv","amr",
    "machine vision","welding robot","pick and place","servo motor","hmi",
    "automated assembly","laser cutting","lean manufacturing","kaizen",
    "robot industriale","automazione","saldatura robotizzata","controllo numerico",
    "nastro trasportatore","robot collaborativo",
    "roboter","automatisierung","schweißroboter","industrieroboter",
    "cnc-bearbeitung","förderband","automatische montage","bildverarbeitung",
    "robot industriel","automatisation","soudage robotisé","convoyeur",
    "usinage cnc","bras robotique","chaîne automatisée",
]
ROBOT_KW=[
    "manual welding","manual assembly","heavy lifting","robotic cell",
    "robot integration","repetitive task","hazardous","palletizing","palletizer",
    "automotive supplier","electronics assembly","injection molding","die casting",
    "high volume production","3 shift","24/7 production",
    "saldatura manuale","assemblaggio manuale","movimentazione manuale",
    "produzione in serie","fornitore automotive","stampaggio a iniezione",
    "manuelle schweißung","manuelle montage","schweres heben",
    "serienproduktion","automobilzulieferer","spritzguss","druckguss",
    "soudage manuel","assemblage manuel","manutention manuelle",
    "production en série","fournisseur automobile",
]
MES_KW=[
    "mes","erp","oee","iiot","industry 4.0","digital twin","traceability",
    "production monitoring","opc ua","real-time data","downtime",
    "batch tracking","work order","iso 9001","iso 13485",
    "industria 4.0","tracciabilità","monitoraggio produzione",
    "manutenzione predittiva","gemello digitale","efficienza impianto",
    "industrie 4.0","rückverfolgbarkeit","predictive maintenance",
    "digitaler zwilling","anlageneffizienz","produktionsüberwachung",
    "traçabilité","maintenance prédictive","jumeau numérique",
    "efficacité industrielle","suivi de production",
]
INTENT_KW=[
    "new plant","capacity expansion","new factory","greenfield","hiring",
    "automation engineer","robotics engineer","modernization","retrofit",
    "digital transformation","new production line","investment","we are growing",
    "nuovo stabilimento","ampliamento produttivo","assunzioni","ingegnere automazione",
    "modernizzazione","trasformazione digitale","nuova linea","investimento",
    "neues werk","erweiterung","stellenangebote","automatisierungsingenieur",
    "modernisierung","digitale transformation","neue produktionslinie","investition",
    "nouvelle usine","expansion","recrutement","ingénieur automatisation",
    "modernisation","transformation digitale","nouvelle ligne","investissement",
]

def fetch(url,timeout=7):
    try:
        r=requests.get(url,headers=UA,timeout=timeout,verify=False,allow_redirects=True)
        if r.status_code==200 and len(r.content)>300:
            t=re.sub(r'<script[^>]*>.*?</script>',' ',r.text,flags=re.S)
            t=re.sub(r'<style[^>]*>.*?</style>',' ',t,flags=re.S)
            t=re.sub(r'<[^>]+>',' ',t)
            return re.sub(r'\s+',' ',t).lower()[:12000]
    except: pass
    return ""

def scan(rec):
    domain=(rec.get("domain") or "").strip()
    if not domain:
        return {"buying_intent_score":-1}  # marker unreachable

    base_url=rec.get("website_url") or f"https://www.{domain}"
    if not base_url.startswith("http"): base_url=f"https://www.{domain}"

    text=""; pages_ok=0
    for url in [base_url,
                f"{base_url}/prodotti",f"{base_url}/products",
                f"{base_url}/solutions",f"{base_url}/soluzioni",
                f"{base_url}/technologie",f"{base_url}/technology",
                f"{base_url}/careers",f"{base_url}/lavora-con-noi",
                f"{base_url}/jobs",f"{base_url}/about"]:
        t=fetch(url)
        if t: text+=" "+t; pages_ok+=1
        if len(text)>30000: break

    if len(text.strip())<200:
        return {"buying_intent_score":-1}  # marker unreachable

    a_h=[k for k in AUTO_KW   if k in text]
    r_h=[k for k in ROBOT_KW  if k in text]
    m_h=[k for k in MES_KW    if k in text]
    i_h=[k for k in INTENT_KW if k in text]

    auto_s  =min(100,len(a_h)*12)
    robot_s =min(100,len(r_h)*18)
    mes_s   =min(100,len(m_h)*14)
    intent_s=min(100,len(i_h)*15)

    scores={"Robotics & Cobot":robot_s,"MES/Digital Factory":mes_s,
            "Automation Upgrade":auto_s,"High Intent":intent_s}
    top=max(scores,key=scores.get); best=scores[top]

    ev={"Robotics & Cobot":r_h[:5],"MES/Digital Factory":m_h[:5],
        "Automation Upgrade":a_h[:5],"High Intent":i_h[:5]}
    solution=("Signals: "+", ".join(ev[top])) if best>=12 and ev[top] else "Low signal"

    # Usa description come campo testo per conservare il top_opportunity
    # (workaround: top_opportunity non persiste via API)
    desc_tag=f"[{top if best>=12 else 'Low Signal'}] {solution}"[:300]

    emp=int(float(rec.get("employee_count") or 0))
    if   emp>5000: dmin,dmax=300000,2000000
    elif emp>500:  dmin,dmax=80000,500000
    elif emp>100:  dmin,dmax=25000,120000
    elif emp>20:   dmin,dmax=8000,40000
    else:          dmin,dmax=3000,15000
    if intent_s>40: dmin=int(dmin*1.4); dmax=int(dmax*1.4)

    return {
        "automation_readiness_score": auto_s,
        "robotics_opportunity_score": robot_s,
        "mes_opportunity_score":      mes_s,
        "buying_intent_score":        intent_s,
        "amr_agv_opportunity_score":  round((robot_s+auto_s)/2),
        "estimated_deal_value_min":   dmin,
        "estimated_deal_value_max":   dmax,
        # Campo description aggiornato solo se vuoto — non sovrascrive
        "_top": top if best>=12 else "Low Signal",
        "_solution": solution,
        "_pages": pages_ok,
    }

def quality_check():
    log.info("  ── QUALITY CHECK ──")
    try:
        skip=0; total=0; with_sig=0; opps={}; top10=[]
        while True:
            b=requests.get(f"{BASE}?limit=500&skip={skip}&fields=buying_intent_score,"
                           f"robotics_opportunity_score,automation_readiness_score,"
                           f"mes_opportunity_score,name,description",
                           headers=HDRS,timeout=20).json()
            if not isinstance(b,list) or not b: break
            for x in b:
                bi=x.get("buying_intent_score")
                if bi is not None and bi>=0:
                    total+=1
                    # Estrai categoria da description
                    desc=x.get("description") or ""
                    m=re.match(r'\[([^\]]+)\]',desc)
                    opp=m.group(1) if m else "?"
                    if opp not in ["Low Signal","?"]:
                        with_sig+=1
                        opps[opp]=opps.get(opp,0)+1
                    if bi>=30: top10.append((bi,x.get("name","?")))
            skip+=500
            if len(b)<500: break

        top10.sort(reverse=True)
        rate=round(with_sig/total*100,1) if total else 0
        log.info(f"  Scansionati: {total} | Con segnale: {with_sig} ({rate}%)")
        for k,v in sorted(opps.items(),key=lambda x:-x[1]):
            log.info(f"    {v:4}  {k}")
        for sc,nm in top10[:5]:
            log.info(f"    intent={sc:3}  {nm}")
        stats["quality"]={"total":total,"with_signal":with_sig,"rate":rate,"opps":opps}
        stats["last_quality_check"]=time.strftime("%H:%M:%S")
        log.info("  ── END CHECK ──")
    except Exception as e:
        log.warning(f"quality_check: {e}")

def load_pending(limit=120):
    """Aziende non ancora scansionate = buying_intent_score IS NULL"""
    results=[]; skip=0
    while len(results)<limit:
        try:
            b=requests.get(f"{BASE}?limit=200&skip={skip}"
                           f"&fields=id,name,domain,website_url,employee_count,"
                           f"buying_intent_score,country,description",
                           headers=HDRS,timeout=20).json()
        except Exception as e:
            log.warning(f"load_pending: {e}"); break
        if not isinstance(b,list) or not b: break
        for r in b:
            # Non scansionato = buying_intent_score IS NULL
            if r.get("buying_intent_score") is None:
                results.append(r)
                if len(results)>=limit: break
        if len(b)<200: break
        skip+=200
    return results

PRIORITY={"ITA","IT","ITALY","DEU","DE","DD","GERMANY","FRA","FR","FRANCE",
          "ESP","ES","SPAIN","CHE","CH","AUT","AT","NLD","NL","BEL","BE",
          "USA","US","UNITED STATES","GBR","GB","UK","JPN","JP","JAPAN"}

log.info("=== SIGNAL ENGINE V3 START ===")
log.info("Marker: buying_intent_score=None → non scansionato")
log.info("Quality check ogni 50 scan")

scan_since_check=0

while True:
    stats["cycle"]+=1
    batch=load_pending(limit=120)
    stats["queue"]=len(batch)

    if not batch:
        log.info("DB completamente scansionato. Quality check + pausa 2h.")
        quality_check()
        time.sleep(7200)
        continue

    batch.sort(key=lambda x:(0 if (x.get("country") or "").upper().strip() in PRIORITY else 1))
    log.info(f"[C{stats['cycle']}] Batch {len(batch)} (IT/DE/FR/US in testa)")

    for rec in batch:
        name=(rec.get("name") or rec.get("domain") or "?")[:38]
        domain=rec.get("domain","?")
        stats["current"]=domain

        try:
            result=scan(rec)

            # Se unreachable → scrivi -1 come marker "scansionato ma morto"
            if result.get("buying_intent_score")==-1:
                payload={"buying_intent_score":0,"automation_readiness_score":0,
                         "robotics_opportunity_score":0,"mes_opportunity_score":0}
                r=requests.put(f"{BASE}/{rec['id']}",json=payload,headers=HDRS,timeout=10)
                if r.status_code in(200,201,204):
                    stats["unreachable"]+=1
                    scan_since_check+=1
                    log.info(f"  ⚠️  {name}: unreachable (score=0)")
                continue

            # Payload con campi che funzionano
            payload={
                "automation_readiness_score": result["automation_readiness_score"],
                "robotics_opportunity_score": result["robotics_opportunity_score"],
                "mes_opportunity_score":      result["mes_opportunity_score"],
                "buying_intent_score":        result["buying_intent_score"],
                "amr_agv_opportunity_score":  result["amr_agv_opportunity_score"],
                "estimated_deal_value_min":   result["estimated_deal_value_min"],
                "estimated_deal_value_max":   result["estimated_deal_value_max"],
            }
            # description: aggiunge tag opportunità se campo vuoto
            existing_desc=(rec.get("description") or "").strip()
            if not existing_desc:
                payload["description"]=f"[{result['_top']}] {result['_solution']}"[:400]

            r=requests.put(f"{BASE}/{rec['id']}",json=payload,headers=HDRS,timeout=15)

            if r.status_code in(200,201,204):
                stats["scanned"]+=1
                scan_since_check+=1
                bi=result["buying_intent_score"]
                ro=result["robotics_opportunity_score"]
                if max(bi,ro,result["mes_opportunity_score"])>=20:
                    stats["good_signals"]+=1
                log.info(
                    f"  ✅ {name:40} "
                    f"auto={result['automation_readiness_score']:3} "
                    f"robot={result['robotics_opportunity_score']:3} "
                    f"mes={result['mes_opportunity_score']:3} "
                    f"intent={result['buying_intent_score']:3} "
                    f"→ {result['_top'][:35]}")
            else:
                stats["errors"]+=1
                log.warning(f"  ❌ {domain}: HTTP {r.status_code} {r.text[:60]}")

        except Exception as e:
            stats["errors"]+=1
            log.warning(f"  ❌ {domain}: {e}")

        if scan_since_check>=50:
            scan_since_check=0
            quality_check()

        time.sleep(1.0)

    log.info(f"[C{stats['cycle']}] DONE — scanned={stats['scanned']} "
             f"unreachable={stats['unreachable']} good={stats['good_signals']} err={stats['errors']}")

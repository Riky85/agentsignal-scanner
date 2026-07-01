#!/usr/bin/env python3
"""
feeder_v5_clean.py — Feeder industriale robusto per Railway H24.
Fonte: Wikidata SPARQL (P856=sito ufficiale), multi-country multi-industry.
Valida ogni URL via HTTP live check prima dell'insert.
Scrive con schema corretto: source, scan_status='pending', scanned=False.
Gira in loop continuo, un batch ogni ciclo, poi pausa.
Include healthcheck HTTP server su $PORT per evitare il kill di Railway.
"""
import requests, time, re, warnings, json, os, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler

warnings.filterwarnings("ignore")

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS_WD = {"User-Agent": "AgentSignalBot/1.0 (industrial scanner; contact: ops@agentsignal.io)",
              "Accept": "application/json"}

BASE44_API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
BASE44_APP_ID  = os.environ.get("BASE44_APP_ID", "6a3a284ab0b87dfa27558bb6")
HDRS_B = {"api-key": BASE44_API_KEY}
BASE   = f"https://app.base44.com/api/apps/{BASE44_APP_ID}/entities/IndustrialCompany"
UA     = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}
PORT   = int(os.environ.get("PORT", 8080))

stats = {"cycle": 0, "inserted_total": 0, "last_cycle_inserted": 0, "status": "starting"}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()

def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

log(f"[OK] healthcheck server su :{PORT}")

COUNTRIES = {
    "wd:Q38":"IT","wd:Q183":"DE","wd:Q142":"FR","wd:Q39":"CH","wd:Q40":"AT",
    "wd:Q55":"NL","wd:Q31":"BE","wd:Q34":"SE","wd:Q35":"DK","wd:Q20":"NO",
    "wd:Q33":"FI","wd:Q29":"ES","wd:Q36":"PL","wd:Q30":"US","wd:Q145":"GB",
    "wd:Q17":"JP","wd:Q16":"CA","wd:Q408":"AU","wd:Q45":"PT","wd:Q27":"IE",
    "wd:Q213":"CZ","wd:Q28":"HU",
}
def norm_country(name):
    return {
        "italy":"IT","germany":"DE","france":"FR","switzerland":"CH","austria":"AT",
        "netherlands":"NL","belgium":"BE","sweden":"SE","denmark":"DK","norway":"NO",
        "finland":"FI","spain":"ES","poland":"PL","united states":"US","united states of america":"US",
        "united kingdom":"GB","japan":"JP","canada":"CA","australia":"AU","portugal":"PT",
        "ireland":"IE","czech republic":"CZ","hungary":"HU","czechia":"CZ",
    }.get((name or "").lower().strip(), (name or "")[:2].upper())

INDUSTRIES = [
    "Q187939","Q11012","Q179818","Q83471","Q131723","Q170790","Q82604","Q11032",
    "Q28179","Q132541","Q184358","Q106239","Q28294","Q17162","Q898364","Q11661",
    "Q13235160","Q210167","Q1341478","Q1128557","Q327738","Q622188","Q1341433",
]
COUNTRY_STR  = ",".join(COUNTRIES.keys())
INDUSTRY_STR = ",".join(f"wd:{i}" for i in INDUSTRIES)

def build_query(offset, limit=1000):
    return f"""
    SELECT DISTINCT ?company ?companyLabel ?website ?countryLabel ?employees ?industryLabel WHERE {{
      ?company wdt:P31 wd:Q4830453 .
      ?company wdt:P856 ?website .
      ?company wdt:P17 ?country .
      FILTER(?country IN ({COUNTRY_STR}))
      ?company wdt:P452 ?industry .
      FILTER(?industry IN ({INDUSTRY_STR}))
      OPTIONAL {{ ?company wdt:P1128 ?employees }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,it,de,fr" .
        ?company rdfs:label ?companyLabel .
        ?country rdfs:label ?countryLabel .
        ?industry rdfs:label ?industryLabel .
      }}
    }}
    LIMIT {limit}
    OFFSET {offset}
    """

NON_IND = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|'
    r'immobilienmakler|insurance broker|restaurant|ristorante|hotel|albergo|'
    r'software development company|web agency|digital marketing agency|'
    r'university|hospital|school|charity|non.?profit|ngo|onlus|'
    r'yarn|garn|textile fashion|underwear|lingerie|furniture retailer|'
    r'hunting|gun shop|weapon|pistol|amusement ride|newspaper publisher|'
    r'winery|brewery|dairy|museum|bank\b|insurance\b)\b', re.I)

def fetch_batch(offset, retries=4):
    q = build_query(offset, 1000)
    for attempt in range(retries):
        try:
            r = requests.get(SPARQL_URL, params={"query": q, "format":"json"},
                             headers=HEADERS_WD, timeout=100)
            if r.status_code == 200:
                return r.json().get("results",{}).get("bindings",[])
            log(f"  SPARQL HTTP {r.status_code} offset {offset} tentativo {attempt+1}")
        except Exception as e:
            log(f"  SPARQL errore offset {offset} tentativo {attempt+1}: {str(e)[:100]}")
        time.sleep(8*(attempt+1))
    return []

def parse_rows(rows):
    out = []
    for row in rows:
        url  = row.get("website",{}).get("value","").strip().rstrip("/")
        name = row.get("companyLabel",{}).get("value","")
        if not url or not name or len(name) > 90: continue
        if re.match(r'^Q\d+$', name): continue
        domain = re.sub(r'^https?://(www\.)?','',url).split('/')[0].lower()
        if not domain or '.' not in domain: continue
        if NON_IND.search(f"{name} {domain}"): continue
        emp = 0
        try: emp = int(row.get("employees",{}).get("value","0"))
        except: pass
        out.append({
            "name": name[:100], "domain": domain,
            "website_url": f"https://www.{domain}",
            "country": norm_country(row.get("countryLabel",{}).get("value","")),
            "industry": row.get("industryLabel",{}).get("value","Manufacturing")[:60],
            "employee_count": emp, "source": "wikidata_P856",
            "scan_status": "pending", "scanned": False,
        })
    return out

def check_url(c):
    for u in [f"https://www.{c['domain']}", f"https://{c['domain']}"]:
        try:
            r = requests.get(u, headers=UA, timeout=6, verify=False, allow_redirects=True)
            if r.status_code < 400:
                c["website_url"] = u
                return c
        except: continue
    return None

def get_existing_domains():
    domains = set(); skip=0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=domain", headers=HDRS_B, timeout=25).json()
        except Exception as e:
            log(f"  get_existing errore: {e}"); break
        if not isinstance(b,list) or not b: break
        for x in b:
            d = (x.get("domain") or "").lower().strip()
            if d: domains.add(d)
        skip += 500
        if len(b) < 500: break
    return domains

def run_cycle():
    log("=== CICLO FEEDER v5 — inizio ===")
    stats["status"] = "harvesting"
    existing = get_existing_domains()
    log(f"Domini esistenti nel DB: {len(existing)}")

    all_new = {}
    empty_streak = 0
    for offset in range(0, 20000, 1000):
        rows = fetch_batch(offset)
        if not rows:
            empty_streak += 1
            log(f"  offset {offset}: 0 righe (streak {empty_streak})")
            if empty_streak >= 3:
                log("  3 offset vuoti di fila, fine harvesting per questo ciclo")
                break
            continue
        empty_streak = 0
        parsed = parse_rows(rows)
        new_here = 0
        for c in parsed:
            if c["domain"] not in existing and c["domain"] not in all_new:
                all_new[c["domain"]] = c
                new_here += 1
        log(f"  offset {offset}: {len(rows)} righe, {new_here} nuovi (tot ciclo {len(all_new)})")
        if len(rows) < 1000:
            log("  ultima pagina raggiunta")
            break
        time.sleep(2)

    candidates = list(all_new.values())
    log(f"Candidati nuovi totali: {len(candidates)}")
    if not candidates:
        log("Nessun nuovo candidato questo ciclo.")
        stats["last_cycle_inserted"] = 0
        return 0

    stats["status"] = "validating"
    log("Validazione HTTP live (25 worker)...")
    live = []
    with ThreadPoolExecutor(max_workers=25) as ex:
        futs = {ex.submit(check_url, c): c for c in candidates}
        done = 0
        for f in as_completed(futs):
            done += 1
            r = f.result()
            if r: live.append(r)
            if done % 200 == 0:
                log(f"  validati {done}/{len(candidates)} — live: {len(live)}")

    log(f"Live e validati: {len(live)}/{len(candidates)}")

    stats["status"] = "inserting"
    inserted = 0; errors = 0
    for i, c in enumerate(live):
        payload = {k:v for k,v in c.items() if v not in [None,0,""]}
        try:
            r = requests.post(BASE, json=payload, headers=HDRS_B, timeout=10)
            if r.status_code in (200,201): inserted += 1
            else: errors += 1
        except Exception:
            errors += 1
        if (i+1) % 100 == 0:
            log(f"  insert progresso: {i+1}/{len(live)} inseriti={inserted} errori={errors}")
        time.sleep(0.15)

    log(f"=== CICLO COMPLETATO: inseriti {inserted}, errori {errors} ===")
    stats["last_cycle_inserted"] = inserted
    stats["inserted_total"] += inserted
    return inserted

def main():
    log("Feeder v5 avviato — loop continuo H24")
    while True:
        try:
            stats["cycle"] += 1
            n = run_cycle()
        except Exception as e:
            log(f"ERRORE CICLO: {e}")
            n = 0
        stats["status"] = "sleeping"
        pausa = 3600 if n == 0 else 1800
        log(f"Pausa {pausa//60} minuti prima del prossimo ciclo...")
        # sleep a piccoli step cosi il main thread resta reattivo e l'HTTP server (in thread separato) risponde comunque
        slept = 0
        while slept < pausa:
            time.sleep(min(30, pausa - slept))
            slept += 30

if __name__ == "__main__":
    main()

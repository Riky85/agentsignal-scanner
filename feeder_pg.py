#!/usr/bin/env python3
"""
feeder_pg.py — Feeder industriale v6, scrive su Postgres (Railway) invece che su Base44.
Base44 non viene MAI chiamato da qui: la sync verso Base44 e' un job separato (sync_to_base44.py).
Stessa logica di raccolta/validazione di feeder_v5_clean.py (Wikidata P856, validazione HTTP live,
21 categorie industriali, copertura mondiale bilanciata su 35 paesi), ma:
  - dedup e insert vanno su Postgres (istantaneo, gratis, nessun limite di rate)
  - l'offset per paese e' persistito su Postgres (feeder_state), non piu' in memoria:
    prima si azzerava ad ogni redeploy, ora sopravvive ai restart.
"""
import requests, time, re, warnings, json, os, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
import psycopg2
from psycopg2.extras import execute_values

warnings.filterwarnings("ignore")

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS_WD = {"User-Agent": "AgentSignalBot/1.0 (industrial scanner; contact: ops@agentsignal.io)",
              "Accept": "application/json"}
UA = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}
PORT = int(os.environ.get("PORT", 8080))

PG_DSN = os.environ.get("DATABASE_URL") or os.environ.get(
    "PG_DSN",
    "postgresql://agent:AgentSignal2026!@postgres-db.railway.internal:5432/agentsignal"
)

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

def get_conn():
    return psycopg2.connect(PG_DSN, connect_timeout=15)

COUNTRIES = {
    # Europa
    "wd:Q38":"IT","wd:Q183":"DE","wd:Q142":"FR","wd:Q39":"CH","wd:Q40":"AT",
    "wd:Q55":"NL","wd:Q31":"BE","wd:Q34":"SE","wd:Q35":"DK","wd:Q20":"NO",
    "wd:Q33":"FI","wd:Q29":"ES","wd:Q36":"PL","wd:Q145":"GB","wd:Q45":"PT",
    "wd:Q27":"IE","wd:Q213":"CZ","wd:Q28":"HU","wd:Q218":"RO","wd:Q214":"SK",
    "wd:Q215":"SI",
    # Nord America
    "wd:Q30":"US","wd:Q16":"CA","wd:Q96":"MX",
    # Asia
    "wd:Q17":"JP","wd:Q668":"IN","wd:Q148":"CN","wd:Q884":"KR","wd:Q334":"SG",
    "wd:Q865":"TW","wd:Q869":"TH","wd:Q252":"ID","wd:Q881":"VN",
    # Oceania
    "wd:Q408":"AU","wd:Q664":"NZ",
    # Sud America
    "wd:Q155":"BR","wd:Q414":"AR",
    # Africa / Medio Oriente
    "wd:Q258":"ZA","wd:Q43":"TR","wd:Q801":"IL","wd:Q878":"AE",
}

INDUSTRIES = [
    "Q187939","Q13235160","Q190117","Q936518","Q5358497","Q1307914","Q1957908",
    "Q207652","Q540912","Q607081","Q507443","Q953045","Q7202108",
    "Q1945600","Q2283886","Q474200","Q26897133","Q13747706","Q13405640","Q2986369","Q1341478",
    # batch 2 (2026-07-02): validati via SPARQL count>=14 aziende con sito web ciascuna
    "Q56604576",  # packaging industry (25)
    "Q2151621",   # energy industry (297)
    "Q3477381",   # automotive supplier (62)
    "Q4899370",   # beverage industry (221)
    "Q63383285",  # medical technology industry (40)
    "Q3477363",   # aerospace industry (526)
    "Q2285982",   # iron and steel industry (250)
    "Q785222",    # glass production (14)
    "Q995609",    # wood industry (15)
    "Q474883",    # water collection, treatment and supply (42)
]
INDUSTRY_STR = ",".join(f"wd:{i}" for i in INDUSTRIES)

FX_TO_EUR = {
    "United States dollar": 0.92, "Euro": 1.0, "Pound sterling": 1.17,
    "Japanese yen": 0.0062, "Renminbi": 0.128, "Swiss franc": 1.05,
    "Canadian dollar": 0.68, "Australian dollar": 0.61, "Indian rupee": 0.011,
    "South Korean won": 0.00068, "Swedish krona": 0.087, "Norwegian krone": 0.085,
    "Danish krone": 0.134, "Polish zloty": 0.23, "Czech koruna": 0.040,
    "Hungarian forint": 0.0025, "Mexican peso": 0.047, "Brazilian real": 0.16,
    "Turkish lira": 0.026, "Israeli new shekel": 0.25, "United Arab Emirates dirham": 0.25,
    "New Taiwan dollar": 0.029, "Singapore dollar": 0.68, "Thai baht": 0.026,
    "Indonesian rupiah": 0.000057, "Vietnamese dong": 0.000037, "South African rand": 0.049,
    "Argentine peso": 0.00095, "Romanian leu": 0.20,
}
def revenue_to_eur_millions(amount, unit_label):
    try:
        amt = float(amount)
    except (TypeError, ValueError):
        return None
    rate = FX_TO_EUR.get(unit_label)
    if rate is None:
        return None
    return round(amt * rate / 1_000_000, 2)

# Mappa COMPLETA ed esplicita per tutti i 41 paesi coperti dal feeder (COUNTRIES dict).
# Il vecchio fallback name[:2].upper() causava collisioni gravi mai notate prima:
# China/Switzerland -> "CH", Singapore/Slovenia -> "SI", South Korea/South Africa -> "SO",
# Taiwan -> "TA" invece di "TW", ecc. Ora ogni paese ha un codice esplicito univoco.
COUNTRY_NAME_TO_CODE = {
    "italy":"IT","germany":"DE","france":"FR","switzerland":"CH","austria":"AT",
    "netherlands":"NL","belgium":"BE","sweden":"SE","denmark":"DK","norway":"NO",
    "finland":"FI","spain":"ES","poland":"PL","united kingdom":"GB","portugal":"PT",
    "ireland":"IE","czech republic":"CZ","czechia":"CZ","hungary":"HU","romania":"RO",
    "slovakia":"SK","slovenia":"SI",
    "united states":"US","united states of america":"US","canada":"CA","mexico":"MX",
    "japan":"JP","india":"IN","china":"CN","people's republic of china":"CN",
    "south korea":"KR","republic of korea":"KR","singapore":"SG","taiwan":"TW",
    "thailand":"TH","indonesia":"ID","vietnam":"VN",
    "australia":"AU","new zealand":"NZ",
    "brazil":"BR","argentina":"AR",
    "south africa":"ZA","turkey":"TR","israel":"IL",
    "united arab emirates":"AE",
}
def norm_country(name):
    key = (name or "").lower().strip()
    if key in COUNTRY_NAME_TO_CODE:
        return COUNTRY_NAME_TO_CODE[key]
    # fallback: nessun match esplicito, teniamo il nome esteso invece di un codice a 2 lettere
    # sbagliato/ambiguo (meglio un valore leggibile e corretto che un codice errato)
    return (name or "").strip()[:60] or "UNKNOWN"

NON_IND = re.compile(
    r'\b(law firm|legal services|avvocato|anwaltskanzlei|real estate agent|'
    r'immobilienmakler|insurance broker|restaurant|ristorante|hotel|albergo|'
    r'software development company|web agency|digital marketing agency|'
    r'university|hospital|school|charity|non.?profit|ngo|onlus|'
    r'yarn|garn|textile fashion|underwear|lingerie|furniture retailer|'
    r'hunting|gun shop|weapon|pistol|amusement ride|newspaper publisher|'
    r'winery|brewery|dairy|museum|bank\b|insurance\b|'
    r'information technology|software company|video game|game developer|'
    r'it services|it consulting|web design|app developer)\b', re.I)

def build_query(country_wd, offset, limit=300):
    return f"""
    SELECT DISTINCT ?company ?companyLabel ?website ?countryLabel ?employees ?industryLabel
                     ?revAmount ?revUnitLabel ?revDate WHERE {{
      ?company wdt:P31 wd:Q4830453 .
      ?company wdt:P856 ?website .
      ?company wdt:P17 {country_wd} .
      ?company wdt:P452 ?industry .
      FILTER(?industry IN ({INDUSTRY_STR}))
      OPTIONAL {{ ?company wdt:P1128 ?employees }}
      OPTIONAL {{
        ?company p:P2139 ?revStmt .
        ?revStmt psv:P2139 ?revValue .
        ?revValue wikibase:quantityAmount ?revAmount ;
                  wikibase:quantityUnit ?revUnit .
        OPTIONAL {{ ?revStmt pq:P585 ?revDate }}
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,it,de,fr" .
        ?company rdfs:label ?companyLabel .
        {country_wd} rdfs:label ?countryLabel .
        ?industry rdfs:label ?industryLabel .
        ?revUnit rdfs:label ?revUnitLabel .
      }}
    }}
    LIMIT {limit}
    OFFSET {offset}
    """

SPARQL_SESSION = requests.Session()
SPARQL_SESSION.headers.update(HEADERS_WD)
VALIDATE_SESSION = requests.Session()
VALIDATE_SESSION.headers.update(UA)

def fetch_batch(country_wd, offset, limit=300, retries=4):
    q = build_query(country_wd, offset, limit)
    for attempt in range(retries):
        try:
            r = SPARQL_SESSION.get(SPARQL_URL, params={"query": q, "format":"json"}, timeout=60)
            if r.status_code == 200:
                return r.json().get("results",{}).get("bindings",[])
            if r.status_code == 429:
                wait = 20*(attempt+1)
                log(f"  SPARQL 429 rate-limit {country_wd} — pausa {wait}s (tentativo {attempt+1})")
                time.sleep(wait)
                continue
            log(f"  SPARQL HTTP {r.status_code} {country_wd} tentativo {attempt+1}")
        except Exception as e:
            log(f"  SPARQL errore {country_wd} tentativo {attempt+1}: {str(e)[:100]}")
        time.sleep(5*(attempt+1))
    return []

def parse_rows(rows):
    by_domain = {}
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

        rev_date = row.get("revDate",{}).get("value","")
        rev_amount = row.get("revAmount",{}).get("value")
        rev_unit = row.get("revUnitLabel",{}).get("value")
        rev_eur_m = revenue_to_eur_millions(rev_amount, rev_unit) if rev_amount else None

        c = by_domain.get(domain)
        if c is None:
            c = {
                "name": name[:100], "domain": domain,
                "website_url": f"https://www.{domain}",
                "country": norm_country(row.get("countryLabel",{}).get("value","")),
                "industry": row.get("industryLabel",{}).get("value","Manufacturing")[:60],
                "employee_count": emp or None, "source": "wikidata_P856",
                "_rev_date": "", "revenue_eur_m": None,
            }
            by_domain[domain] = c
        if rev_eur_m is not None and rev_date > c["_rev_date"]:
            c["_rev_date"] = rev_date
            c["revenue_eur_m"] = rev_eur_m

    out = []
    for c in by_domain.values():
        c.pop("_rev_date", None)
        out.append(c)
    return out

def check_url(c):
    for u in [f"https://www.{c['domain']}", f"https://{c['domain']}"]:
        try:
            r = VALIDATE_SESSION.get(u, timeout=6, verify=False, allow_redirects=True)
            if r.status_code < 400:
                c["website_url"] = u
                return c
        except: continue
    return None

def get_existing_domains(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT domain FROM industrial_company")
        return set(r[0] for r in cur.fetchall())

def get_country_offset(conn, code):
    with conn.cursor() as cur:
        cur.execute("SELECT last_offset FROM feeder_state WHERE country_code=%s", (code,))
        row = cur.fetchone()
        return row[0] if row else 0

def set_country_offset(conn, code, offset):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO feeder_state (country_code, last_offset, last_run_at)
            VALUES (%s, %s, now())
            ON CONFLICT (country_code) DO UPDATE SET last_offset=%s, last_run_at=now()
        """, (code, offset, offset))
    conn.commit()

def insert_candidates(conn, live):
    if not live: return 0
    cols = ["domain","name","website_url","country","industry","employee_count",
            "revenue_eur_m","source","scan_status","scanned","created_at","updated_at"]
    rows = []
    for c in live:
        rows.append((
            c["domain"], c["name"], c["website_url"], c["country"], c["industry"],
            c.get("employee_count"), c.get("revenue_eur_m"), c["source"],
            "pending", False
        ))
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO industrial_company (domain,name,website_url,country,industry,
                employee_count,revenue_eur_m,source,scan_status,scanned,created_at,updated_at)
            VALUES %s
            ON CONFLICT (domain) DO NOTHING
        """, rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM industrial_company WHERE domain = ANY(%s)",
                    ([c["domain"] for c in live],))
    return len(live)

def run_cycle():
    log("=== CICLO FEEDER PG — inizio ===")
    stats["status"] = "harvesting"
    conn = get_conn()
    try:
        existing = get_existing_domains(conn)
        log(f"Domini esistenti in Postgres: {len(existing)}")

        all_new = {}
        for country_wd, code in COUNTRIES.items():
            offset = get_country_offset(conn, code)
            rows = fetch_batch(country_wd, offset, limit=300)
            parsed = parse_rows(rows)
            new_here = 0
            for c in parsed:
                if c["domain"] not in existing and c["domain"] not in all_new:
                    all_new[c["domain"]] = c
                    new_here += 1
            log(f"  {code} (offset {offset}): {len(rows)} righe, {new_here} nuovi (tot ciclo {len(all_new)})")
            # avanza l'offset per questo paese SOLO se abbiamo ricevuto risultati pieni
            # (altrimenti vuol dire che siamo al fondo del pool per questo paese, non avanzare a vuoto)
            new_offset = offset + 300 if len(rows) >= 300 else offset
            set_country_offset(conn, code, new_offset)
            time.sleep(1)

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
        n = insert_candidates(conn, live)
        log(f"=== CICLO COMPLETATO: {n} candidati processati su Postgres ===")
        stats["last_cycle_inserted"] = n
        stats["inserted_total"] += n
        return n
    finally:
        conn.close()

def main():
    log("Feeder PG avviato — loop continuo H24 (scrive solo su Postgres, zero chiamate Base44)")
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
        slept = 0
        while slept < pausa:
            time.sleep(min(30, pausa - slept))
            slept += 30

if __name__ == "__main__":
    main()

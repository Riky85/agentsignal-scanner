"""
feeder_v2_industrial.py — SOLO Wikidata P856 (siti ufficiali reali)
Ogni azienda inserita ha il dominio preso direttamente dal campo P856 di Wikidata.
ZERO costruzione slug dal nome. ZERO Companies House. ZERO fonti generiche.
"""
import requests, re, time, os, concurrent.futures
from urllib.parse import urlparse
import warnings
warnings.filterwarnings("ignore")

API_KEY  = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID   = os.environ.get("BASE44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE_URL = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS     = {"api-key": API_KEY, "Content-Type": "application/json"}
UA       = {"User-Agent": "Mozilla/5.0 (compatible; IndustrialBot/2.0)"}

PARKING  = re.compile(r'(domain.for.sale|buy.this.domain|godaddy|sedo\.com|afternic|'
                       r'hugedomains|dan\.com|domain.parking|parked.page|underconstruction)', re.I)

existing_domains = set()

def normalize(url):
    if not url: return ""
    u = url.strip().lower()
    if u.startswith("http"):
        u = urlparse(u).netloc
    u = re.sub(r'^www\.', '', u).split('/')[0].strip()
    return u if '.' in u else ""

def load_existing():
    skip = 0
    while True:
        try:
            b = requests.get(f"{BASE_URL}?limit=500&skip={skip}&fields=domain", headers=HDRS, timeout=20).json()
            if not isinstance(b, list) or not b: break
            for r in b:
                d = normalize(r.get("domain", ""))
                if d: existing_domains.add(d)
            if len(b) < 500: break
            skip += 500
        except: break
    print(f"[init] Domini esistenti: {len(existing_domains)}", flush=True)

def check_live(domain):
    for url in [f"https://www.{domain}", f"https://{domain}"]:
        try:
            r = requests.get(url, timeout=8, headers=UA, allow_redirects=True, verify=False)
            body = r.text[:1500].lower()
            if PARKING.search(body): return False
            if r.status_code < 500: return True
        except: pass
    return False

def push(name, domain, country, industry, employees, description, source):
    d = normalize(domain)
    if not d or len(d) < 5 or d in existing_domains: return False
    if not check_live(d): return False
    payload = {
        "name": name[:200], "domain": d,
        "website_url": f"https://www.{d}",
        "country": (country or "")[:5].upper(),
        "industry": (industry or "Industrial")[:100],
        "employee_count": employees or 0,
        "description": (description or "")[:500],
        "source": source, "scan_status": "pending", "scanned": False,
    }
    for attempt in range(3):
        try:
            r = requests.post(BASE_URL, json=payload, headers=HDRS, timeout=12)
            if r.status_code in (200, 201):
                existing_domains.add(d)
                return True
            if r.status_code == 429:
                time.sleep(15); continue
            return False
        except: time.sleep(2)
    return False

# ── Wikidata queries SOLO con P856 (sito ufficiale) ──────────────────────
QUERIES = [
    # Manifattura generica
    ("Manufacturing", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website; wdt:P452 wd:Q187939.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 3000"""),
    # Macchinari industriali
    ("Industrial Machinery", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q11032 wd:Q170595 wd:Q82728 wd:Q187939 wd:Q205398}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 3000"""),
    # Robotica & Automazione
    ("Robotics & Automation", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q11642 wd:Q171603 wd:Q11023 wd:Q2122214 wd:Q131723 wd:Q189566}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 2000"""),
    # Elettronica & Semiconduttori
    ("Electronics", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q79782 wd:Q22698 wd:Q11016 wd:Q9143}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 2000"""),
    # Aerospace & Defence
    ("Aerospace & Defence", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q210932 wd:Q63717 wd:Q1569869 wd:Q182531}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 1500"""),
    # Automotive
    ("Automotive", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q1420 wd:Q262166 wd:Q14864}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 2000"""),
    # Chimica & Materiali
    ("Chemicals & Materials", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q11032 wd:Q28573 wd:Q2095 wd:Q190527}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 1500"""),
    # Energia & Power
    ("Energy & Power", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website.
      VALUES ?ind {wd:Q12748 wd:Q11946 wd:Q80638 wd:Q219416}
      ?c wdt:P452 ?ind.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 1500"""),
    # Software B2B / Industrial Tech
    ("Industrial Software", """SELECT DISTINCT ?name ?website ?country ?emp WHERE {
      ?c wdt:P31 wd:Q4830453; wdt:P856 ?website; wdt:P452 wd:Q205398.
      ?c rdfs:label ?name FILTER(LANG(?name)="en")
      OPTIONAL{?c wdt:P17 ?ct. ?ct wdt:P298 ?country}
      OPTIONAL{?c wdt:P1082 ?emp}
    } LIMIT 2000"""),
]

def run_query(label, sparql):
    print(f"\n[wikidata] ── {label} ──", flush=True)
    inserted = 0
    try:
        r = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": sparql, "format": "json"},
            headers={"User-Agent": "AgentSignalBot/2.0"},
            timeout=45
        )
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}", flush=True); return 0
        bindings = r.json().get("results", {}).get("bindings", [])
        print(f"  {len(bindings)} risultati Wikidata", flush=True)
        for b in bindings:
            name    = b.get("name", {}).get("value", "")
            website = b.get("website", {}).get("value", "")
            country = b.get("country", {}).get("value", "")
            emp_raw = b.get("emp", {}).get("value", "")
            if not name or not website: continue
            try: emp = int(float(emp_raw))
            except: emp = 0
            ok = push(name, normalize(website), country[:3].upper() if country else "", label, emp, "", "wikidata")
            if ok:
                inserted += 1
                print(f"  ✅ [{len(existing_domains)}] {name[:45]} | {normalize(website)}", flush=True)
            time.sleep(0.15)
    except Exception as e:
        print(f"  err: {e}", flush=True)
    print(f"  → {label}: {inserted} inseriti", flush=True)
    return inserted

def main():
    print("██ FEEDER V2 INDUSTRIAL — SOLO WIKIDATA P856 ██", flush=True)
    load_existing()
    cycle = 0
    while True:
        cycle += 1
        print(f"\n═══ CICLO {cycle} ═══", flush=True)
        total = 0
        for label, sparql in QUERIES:
            total += run_query(label, sparql)
            time.sleep(4)  # pausa tra query Wikidata
        print(f"\n[ciclo {cycle}] Totale inseriti: {total} | DB size: {len(existing_domains)}", flush=True)
        print(f"Pausa 60 min prima del prossimo ciclo...", flush=True)
        time.sleep(3600)

if __name__ == "__main__":
    main()

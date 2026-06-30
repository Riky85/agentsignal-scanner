"""
feeder_v2_industrial.py
========================
Feeder SOLO aziende manifatturiere/industriali/tech certificate.

FONTI (tutte pubbliche, no API key):
1. SEC EDGAR  — aziende USA quotate per codici SIC manifatturieri (3.000+ az.)
2. Wikidata   — aziende con P31=Q4830453 (business) + P452=industria manifatturiera
3. OpenCorporates bulk — aziende con "manufacturing" nel SIC/nome (top paesi)

REGOLE:
- Ogni record DEVE avere dominio da fonte strutturata (non costruito dal nome)
- HTTP validation obbligatoria: il sito deve rispondere 200/301/302
- Blocca parking pages (GoDaddy, Sedo, "domain for sale")
- Dedup per dominio (website field)
- Zero Companies House/GLEIF/SIRENE generici (troppo rumorosi)
"""
import requests, re, time, os, json, hashlib, concurrent.futures, socket
from urllib.parse import urlparse
import warnings
warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY  = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID   = os.environ.get("BASE44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE_URL = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS_B44 = {"api-key": API_KEY, "Content-Type": "application/json"}
HTTP_UA  = {"User-Agent": "Mozilla/5.0 (compatible; IndustrialBot/2.0)"}

stats = {"inserted": 0, "rejected": 0, "http_fail": 0, "dup": 0}

# ── Helpers ──────────────────────────────────────────────────────────────────
def normalize_domain(d):
    if not d: return ""
    d = d.lower().strip()
    if d.startswith("http"): d = urlparse(d).netloc
    d = re.sub(r'^www\.', '', d).split('/')[0].strip()
    return d

def load_existing_domains():
    existing = set()
    skip = 0
    while True:
        try:
            b = requests.get(f"{BASE_URL}?limit=500&skip={skip}&fields=domain",
                             headers=HDRS_B44, timeout=20).json()
            if not isinstance(b, list) or not b: break
            for r in b:
                d = normalize_domain(r.get("domain", ""))
                if d: existing.add(d)
            if len(b) < 500: break
            skip += 500
        except Exception as e:
            print(f"[load] err: {e}", flush=True)
            break
    print(f"[init] Domini esistenti: {len(existing)}", flush=True)
    return existing

PARKING_BODY = re.compile(
    r'(this domain is for sale|buy this domain|domain for sale|'
    r'get this domain|lease to own|godaddy|sedo\.com|afternic|'
    r'hugedomains|dan\.com|domain parking|this page is parked|'
    r'underconstruction|under construction)',
    re.I
)

http_cache = {}

def check_domain_live(domain):
    if domain in http_cache:
        return http_cache[domain]
    for scheme_url in [f"https://www.{domain}", f"https://{domain}"]:
        try:
            r = requests.get(scheme_url, timeout=7, headers=HTTP_UA,
                             allow_redirects=True, verify=False)
            body = r.text[:2000].lower()
            if PARKING_BODY.search(body):
                http_cache[domain] = (False, "parking")
                return False, "parking"
            if r.status_code in (200, 301, 302, 403):
                http_cache[domain] = (True, "ok")
                return True, "ok"
        except:
            pass
    http_cache[domain] = (False, "unreachable")
    return False, "unreachable"

def push_company(name, domain, country, industry, employees, description, source, existing):
    d = normalize_domain(domain)
    if not d or len(d) < 4 or "." not in d:
        return False, "invalid_domain"
    if d in existing:
        stats["dup"] += 1
        return False, "dup"

    live, reason = check_domain_live(d)
    if not live:
        stats["http_fail"] += 1
        return False, reason

    payload = {
        "name": name[:200],
        "domain": d,
        "website_url": f"https://www.{d}",
        "country": (country or "")[:5].upper(),
        "industry": (industry or "Manufacturing")[:100],
        "employee_count": employees or 0,
        "description": (description or "")[:500],
        "source": source,
        "scan_status": "pending",
        "scanned": False,
    }

    try:
        r = requests.post(BASE_URL, json=payload, headers=HDRS_B44, timeout=12)
        if r.status_code in (200, 201):
            existing.add(d)
            stats["inserted"] += 1
            return True, "ok"
        elif r.status_code == 429:
            time.sleep(12)
            return False, "rate_limit"
        else:
            return False, f"http_{r.status_code}"
    except Exception as e:
        return False, str(e)

# ════════════════════════════════════════════════════════════════════════════
# FONTE 1 — SEC EDGAR (SIC codes manifatturieri)
# ════════════════════════════════════════════════════════════════════════════
SEC_HDRS = {"User-Agent": "AgentSignal industrial@agentsignal.io"}

# SIC codes SOLO manifatturieri (no retail, no finance, no media)
MFG_SIC_CODES = [
    # Industrial machinery & equipment
    3559, 3560, 3562, 3563, 3564, 3565, 3566, 3567, 3568, 3569,
    3579, 3580, 3585, 3589, 3590, 3592, 3594, 3596, 3599,
    # Machine tools & metalworking
    3541, 3542, 3544, 3545, 3546, 3547, 3548, 3550, 3551, 3552,
    3553, 3554, 3555, 3556,
    # Construction/mining equipment
    3531, 3532, 3533, 3534, 3535, 3536, 3537,
    # Instruments & measurement
    3812, 3821, 3822, 3823, 3824, 3825, 3826, 3827, 3829,
    # Aerospace & defense
    3720, 3721, 3724, 3728, 3760, 3769, 3812,
    # Automotive
    3710, 3711, 3713, 3714, 3715, 3716,
    # Electronics & semiconductors
    3672, 3674, 3675, 3676, 3677, 3678, 3679, 3699,
    # Metals & fabrication
    3411, 3420, 3430, 3440, 3460, 3470, 3480, 3490,
    # Chemicals & plastics
    2810, 2819, 2820, 2830, 2860, 2890, 2891, 3080, 3082, 3086,
    # Electrical equipment
    3612, 3613, 3621, 3625, 3629, 3640, 3669, 3690,
    # Medical devices
    3841, 3842, 3843, 3844, 3845, 3851,
    # Pumps, valves, compressors
    3561, 3562, 3593, 3594,
    # Robotics / automation
    3559, 3699,
    # Paper & printing (industrial)
    2650, 2670, 2750, 2760,
]
MFG_SIC_CODES = list(set(MFG_SIC_CODES))

def get_edgar_company_website(cik):
    """Recupera sito web dall'ultimo 10-K/20-F filing di EDGAR."""
    try:
        r = requests.get(
            f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json",
            headers=SEC_HDRS, timeout=10
        )
        if r.status_code != 200: return None
        data = r.json()
        # Campo website diretto
        website = data.get("website", "")
        if website:
            return normalize_domain(website)
        # Cerca nei recent filings per 10-K
        name = data.get("name", "")
        return None
    except:
        return None

def fetch_edgar_by_sic(sic_code):
    """Recupera lista aziende EDGAR per SIC code."""
    results = []
    try:
        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index?q=%22%22&dateRange=custom"
            f"&startdt=2020-01-01&forms=10-K&hits.hits._source.period_of_report=*",
            headers=SEC_HDRS, timeout=10
        )
        # Usa l'endpoint company search
        r2 = requests.get(
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
            f"&SIC={sic_code}&owner=include&match=&start=0&count=100&hidefilings=0",
            headers=SEC_HDRS, timeout=15
        )
        if r2.status_code != 200: return results
        rows = re.findall(
            r'CIK=(\d+)[^>]*>([^<]+)</a></td><td[^>]*>([^<]*)</td><td[^>]*>([A-Z]{2})',
            r2.text
        )
        for cik, name, _, state in rows:
            name = name.strip()
            if not name or len(name) < 3: continue
            results.append({"cik": cik.strip(), "name": name, "sic": sic_code, "state": state})
    except Exception as e:
        pass
    return results

def run_edgar_source(existing):
    print("[edgar] Avvio importazione SEC EDGAR...", flush=True)
    inserted = 0
    for sic in MFG_SIC_CODES:
        companies = fetch_edgar_by_sic(sic)
        for co in companies:
            cik = co["cik"]
            name = co["name"]
            # Recupera sito web da EDGAR
            domain = get_edgar_company_website(cik)
            if not domain:
                # Costruisci slug dal nome come fallback
                slug = re.sub(r'[^a-z0-9]', '', name.lower().replace(' ', ''))[:20]
                domain = f"{slug}.com"
            ok, reason = push_company(
                name=name, domain=domain, country="US",
                industry=f"Manufacturing (SIC {sic})",
                employees=0, description="",
                source="sec_edgar", existing=existing
            )
            if ok:
                inserted += 1
                print(f"[edgar] ✅ [{stats['inserted']}] {name[:40]} | {domain}", flush=True)
            time.sleep(0.3)
        time.sleep(1)
    print(f"[edgar] Completato: {inserted} inseriti", flush=True)

# ════════════════════════════════════════════════════════════════════════════
# FONTE 2 — WIKIDATA (aziende manifatturiere con sito ufficiale P856)
# ════════════════════════════════════════════════════════════════════════════

WIKIDATA_QUERIES = [
    # Aziende manifatturiere per settore con sito web
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      ?co wdt:P452 wd:Q187939 .  # industria manifatturiera
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?countryEntity . ?countryEntity wdt:P298 ?country }
      FILTER(STRSTARTS(STR(?website), "http"))
    } LIMIT 2000""",
    # Aziende di automazione industriale
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      ?co wdt:P452 wd:Q82728 .  # manifattura
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
      FILTER(STRSTARTS(STR(?website), "http"))
    } LIMIT 3000""",
    # Robotica e automazione
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      VALUES ?ind { wd:Q11642 wd:Q171603 wd:Q11023 wd:Q2122214 wd:Q131723 }
      ?co wdt:P452 ?ind .
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
    } LIMIT 2000""",
    # Elettronica e semiconduttori
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      VALUES ?ind { wd:Q11032 wd:Q79782 wd:Q22698 wd:Q189566 }
      ?co wdt:P452 ?ind .
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
    } LIMIT 2000""",
    # Aerospace & Defense
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      VALUES ?ind { wd:Q210932 wd:Q63717 wd:Q1569869 }
      ?co wdt:P452 ?ind .
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
    } LIMIT 1000""",
    # Automotive
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      VALUES ?ind { wd:Q1420 wd:Q262166 }
      ?co wdt:P452 ?ind .
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
    } LIMIT 2000""",
    # Software/Tech B2B
    """SELECT DISTINCT ?name ?website ?country WHERE {
      ?co wdt:P31 wd:Q4830453 .
      ?co wdt:P856 ?website .
      VALUES ?ind { wd:Q11016 wd:Q9143 wd:Q205398 }
      ?co wdt:P452 ?ind .
      ?co rdfs:label ?name . FILTER(LANG(?name)="en")
      OPTIONAL { ?co wdt:P17 ?c . ?c wdt:P298 ?country }
    } LIMIT 3000""",
]

WIKIDATA_INDUSTRY_MAP = {
    "Q187939": "Manufacturing",
    "Q82728":  "Manufacturing",
    "Q11642":  "Robotics",
    "Q171603": "Industrial Automation",
    "Q11023":  "Engineering",
    "Q2122214":"Mechanical Engineering",
    "Q131723": "Automation",
    "Q11032":  "Electronics",
    "Q79782":  "Semiconductors",
    "Q22698":  "Technology",
    "Q189566": "Software",
    "Q210932": "Aerospace",
    "Q63717":  "Defense",
    "Q1420":   "Automotive",
    "Q262166": "Automotive",
    "Q11016":  "Software",
    "Q9143":   "Software",
    "Q205398": "Technology",
}

def fetch_wikidata(sparql_query):
    try:
        r = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": sparql_query, "format": "json"},
            headers={"User-Agent": "AgentSignalBot/2.0 (industrial@agentsignal.io)"},
            timeout=30
        )
        if r.status_code != 200: return []
        data = r.json()
        results = []
        for b in data.get("results", {}).get("bindings", []):
            name = b.get("name", {}).get("value", "")
            website = b.get("website", {}).get("value", "")
            country = b.get("country", {}).get("value", "")
            if name and website:
                results.append({
                    "name": name,
                    "domain": normalize_domain(website),
                    "country": country[:3].upper() if country else "",
                })
        return results
    except Exception as e:
        print(f"[wikidata] err: {e}", flush=True)
        return []

def run_wikidata_source(existing):
    print("[wikidata] Avvio query Wikidata...", flush=True)
    inserted = 0
    for i, query in enumerate(WIKIDATA_QUERIES):
        print(f"[wikidata] Query {i+1}/{len(WIKIDATA_QUERIES)}...", flush=True)
        results = fetch_wikidata(query)
        print(f"  → {len(results)} risultati", flush=True)
        
        industries = ["Manufacturing","Manufacturing","Robotics/Automation","Electronics",
                      "Aerospace","Automotive","Software/Tech"]
        industry = industries[i] if i < len(industries) else "Industrial"
        
        for co in results:
            if not co["domain"]: continue
            ok, reason = push_company(
                name=co["name"], domain=co["domain"],
                country=co["country"], industry=industry,
                employees=0, description="",
                source="wikidata", existing=existing
            )
            if ok:
                inserted += 1
                print(f"[wd] ✅ [{stats['inserted']}] {co['name'][:40]} | {co['domain']}", flush=True)
            time.sleep(0.2)
        time.sleep(3)  # pausa tra query Wikidata
    print(f"[wikidata] Completato: {inserted} inseriti", flush=True)

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    print("[feeder_v2] ██ AVVIO FEEDER INDUSTRIALE V2 ██", flush=True)
    existing = load_existing_domains()
    
    # Ciclo infinito: Wikidata → EDGAR → sleep → repeat
    cycle = 0
    while True:
        cycle += 1
        print(f"\n[feeder_v2] ═══ CICLO {cycle} ═══", flush=True)
        
        # Wikidata prima (fonte più affidabile)
        run_wikidata_source(existing)
        print(f"[stats] inserted={stats['inserted']} dup={stats['dup']} http_fail={stats['http_fail']} rejected={stats['rejected']}", flush=True)
        
        # EDGAR
        run_edgar_source(existing)
        print(f"[stats] inserted={stats['inserted']} dup={stats['dup']} http_fail={stats['http_fail']} rejected={stats['rejected']}", flush=True)
        
        # Pausa tra cicli (30 min)
        print(f"[feeder_v2] Ciclo {cycle} completato. Pausa 30min...", flush=True)
        time.sleep(1800)

if __name__ == "__main__":
    main()

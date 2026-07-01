"""
feeder_v3_multisource.py — Feeder industriale multi-source
Fonti: Wikidata SPARQL (manifattura, robotica, aerospace, automotive, elettronica)
       SEC EDGAR (SIC codes manifatturieri)
Target: IndustrialCompany su app AgentSignal (6a3a284ab0b87dfa27558bb6)
Regole: validate-first (HTTP check), dedup su domain, PUT atomico, no testo visibile
"""

import os, re, time, requests, concurrent.futures, threading, json
from urllib.parse import urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("AGENTSIGNAL_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", "8080"))

UA_WEB  = {"User-Agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/124.0 Safari/537.36"}
UA_BOT  = {"User-Agent": "IndustrialFeeder/3.0 (contact@agentsignal.io)"}

import warnings; warnings.filterwarnings("ignore")

# ── Stats & Healthcheck ──────────────────────────────────────────────────────
stats = {"inserted": 0, "skipped_dup": 0, "skipped_http": 0, "errors": 0, "cycle": 0}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps(stats).encode())
    def log_message(self, *a): pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Handler).serve_forever(),
    daemon=True
).start()
print(f"[healthcheck] Listening on :{PORT}", flush=True)

# ── Existing domains cache ───────────────────────────────────────────────────
KNOWN_DOMAINS: set = set()

def load_existing():
    """Carica tutti i domain già nel DB per deduplicazione"""
    global KNOWN_DOMAINS
    print("[cache] Carico domini esistenti...", flush=True)
    skip = 0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=domain",
                             headers=HDRS, timeout=20).json()
            if not isinstance(b, list) or not b: break
            for r in b:
                d = (r.get("domain") or "").strip().lower()
                if d: KNOWN_DOMAINS.add(d)
            if len(b) < 500: break
            skip += 500
        except Exception as e:
            print(f"[cache] Err: {e}", flush=True); break
    print(f"[cache] {len(KNOWN_DOMAINS)} domini noti", flush=True)

# ── HTTP Validation ──────────────────────────────────────────────────────────
BLOCKED_PATTERNS = re.compile(
    r'(godaddy|sedo|parking|forsale|for-sale|namecheap|afternic|hugedomains|'
    r'buydomains|dan\.com|underconstruction|under.construction)', re.I
)
BLOCKED_SLUGS = re.compile(
    r'(holdco|bidco|newco|spvco|acquisition|merger|finco|topco|midco)', re.I
)

def validate_domain(domain: str) -> bool:
    """HTTP check obbligatorio prima di inserire"""
    if not domain or len(domain) < 5: return False
    if BLOCKED_SLUGS.search(domain): return False
    for prefix in [f"https://www.{domain}", f"https://{domain}"]:
        try:
            r = requests.get(prefix, timeout=7, headers=UA_WEB,
                             allow_redirects=True, verify=False)
            if r.status_code >= 400: continue
            if BLOCKED_PATTERNS.search(r.text[:2000]): return False
            return True
        except: pass
    return False

# ── Normalize domain ─────────────────────────────────────────────────────────
def norm_domain(url: str) -> str:
    url = url.strip()
    if not url.startswith("http"): url = "https://" + url
    try:
        p = urlparse(url)
        host = p.netloc or p.path.split("/")[0]
        host = host.lower().removeprefix("www.")
        if "." not in host or len(host) < 5: return ""
        return host
    except: return ""

# ── DB Write (PUT atomico) ───────────────────────────────────────────────────
def upsert(company: dict) -> bool:
    domain = (company.get("domain") or "").lower().strip()
    if not domain: return False

    # Dedup
    if domain in KNOWN_DOMAINS:
        stats["skipped_dup"] += 1
        return False

    # HTTP validate
    if not validate_domain(domain):
        stats["skipped_http"] += 1
        return False

    # Costruisci payload minimo valido
    name = (company.get("name") or "").strip()
    if not name: name = domain.split(".")[0].replace("-", " ").title()

    payload = {
        "name":        name[:200],
        "domain":      domain,
        "website_url": f"https://www.{domain}",
        "country":     (company.get("country") or "")[:3].upper(),
        "industry":    (company.get("industry") or "Manufacturing")[:200],
        "source":      company.get("source", "feeder_v3"),
        "scan_status": "pending",
        "scanned":     False,
    }
    for opt in ("city", "description", "employee_count", "revenue"):
        if company.get(opt): payload[opt] = company[opt]

    # Cerca record esistente per domain
    try:
        existing = requests.get(
            f"{BASE}?domain={domain}&limit=1&fields=id",
            headers=HDRS, timeout=10
        ).json()
        if isinstance(existing, list) and existing:
            # Aggiorna via PUT
            rid = existing[0]["id"]
            r = requests.put(f"{BASE}/{rid}", json=payload, headers=HDRS, timeout=10)
        else:
            # Crea nuovo
            r = requests.post(BASE, json=payload, headers=HDRS, timeout=10)

        if r.status_code == 429:
            time.sleep(30)
            r = requests.post(BASE, json=payload, headers=HDRS, timeout=10)

        if r.status_code in (200, 201):
            KNOWN_DOMAINS.add(domain)
            stats["inserted"] += 1
            return True
        else:
            stats["errors"] += 1
            return False
    except Exception as e:
        stats["errors"] += 1
        return False

# ── Source: Wikidata SPARQL ──────────────────────────────────────────────────
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_UA = {"Accept": "application/json", "User-Agent": "IndustrialFeeder/3.0 (+https://agentsignal.io)"}

INDUSTRY_SECTORS = [
    # (label, [wikidata QIDs])
    ("Manufacturing",      ["wd:Q187939", "wd:Q22685", "wd:Q15142889", "wd:Q17006914"]),
    ("Electronics",        ["wd:Q28823",  "wd:Q170584"]),
    ("Automotive",         ["wd:Q83364",  "wd:Q11042"]),
    ("Aerospace",          ["wd:Q62794",  "wd:Q376"]),
    ("Robotics",           ["wd:Q11399",  "wd:Q9135"]),
    ("Chemicals",          ["wd:Q184356", "wd:Q177462"]),
    ("Food_Industry",      ["wd:Q11401",  "wd:Q131596"]),
    ("Energy_Equipment",   ["wd:Q12791",  "wd:Q11403"]),
    ("Medical_Devices",    ["wd:Q1128340","wd:Q498967"]),
]

def fetch_wikidata(sector_label: str, qids: list) -> list:
    values = " ".join(qids)
    query = f"""
SELECT DISTINCT ?name ?website ?countryLabel WHERE {{
  ?company wdt:P856 ?website ;
           wdt:P31 wd:Q4830453 ;
           wdt:P452 ?industry .
  VALUES ?industry {{ {values} }}
  ?company rdfs:label ?name FILTER(LANG(?name)="en") .
  OPTIONAL {{
    ?company wdt:P17 ?country .
    ?country rdfs:label ?countryLabel FILTER(LANG(?countryLabel)="en")
  }}
  FILTER(STRSTARTS(STR(?website), "https://www.") || STRSTARTS(STR(?website), "http://www."))
}} LIMIT 1000
"""
    try:
        r = requests.get(WIKIDATA_SPARQL, params={"query": query, "format": "json"},
                         headers=WIKIDATA_UA, timeout=45)
        rows = r.json().get("results", {}).get("bindings", [])
        out = []
        for row in rows:
            web  = row.get("website", {}).get("value", "")
            name = row.get("name",    {}).get("value", "")
            ctry = row.get("countryLabel", {}).get("value", "")
            dom  = norm_domain(web)
            if dom:
                out.append({"name": name, "domain": dom, "country": ctry,
                            "industry": sector_label, "source": "wikidata"})
        return out
    except Exception as e:
        print(f"[wikidata] {sector_label} err: {e}", flush=True)
        return []

# ── Source: SEC EDGAR ────────────────────────────────────────────────────────
# SIC codes manifatturieri 2000-3999
MANUFACTURING_SIC = list(range(2000, 4000))

def fetch_edgar(offset: int = 0, limit: int = 200) -> list:
    # EDGAR company search per SIC manifatturieri
    out = []
    for sic in MANUFACTURING_SIC[offset:offset+10]:
        try:
            url = f"https://efts.sec.gov/LATEST/search-index?q=%22{sic}%22&dateRange=custom&startdt=2020-01-01&forms=10-K"
            r = requests.get(
                f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&SIC={sic}&type=10-K&dateb=&owner=include&count=40&output=atom",
                headers=UA_BOT, timeout=15
            )
            names   = re.findall(r'<company-name>([^<]+)</company-name>', r.text)
            ciks    = re.findall(r'<CIK>(\d+)</CIK>',                    r.text)
            for name, cik in zip(names, ciks):
                # Cerca il sito ufficiale tramite EDGAR company facts
                try:
                    cf = requests.get(
                        f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json",
                        headers=UA_BOT, timeout=10
                    ).json()
                    web = cf.get("website", "") or ""
                    if not web:
                        # Costruisci dominio dal nome
                        slug = re.sub(r'[^a-z0-9]', '', name.lower()[:20])
                        web = f"https://www.{slug}.com"
                    dom = norm_domain(web)
                    if dom:
                        out.append({
                            "name": name.strip(), "domain": dom,
                            "country": "US", "industry": f"Manufacturing (SIC {sic})",
                            "source": "sec_edgar"
                        })
                except: pass
                time.sleep(0.05)
        except Exception as e:
            pass
        time.sleep(0.2)
    return out

# ── Auto-QC: rimuove non-industriali ────────────────────────────────────────
NON_INDUSTRIAL = re.compile(
    r'(church|parish|pizza|restaurant|real.estate|school|hospital|parish|'
    r'diocese|mosque|temple|synagogue|hotel|resort|boutique|salon|spa|'
    r'kindergarten|nursery|funeral|obituary|charity|foundation|ngo)', re.I
)

def auto_qc():
    """Ogni 50 inserimenti, rimuove record non-industriali"""
    try:
        batch = requests.get(f"{BASE}?limit=100&fields=id,name,industry", headers=HDRS, timeout=15).json()
        if not isinstance(batch, list): return
        removed = 0
        for rec in batch:
            name = rec.get("name", "")
            ind  = rec.get("industry", "")
            if NON_INDUSTRIAL.search(name) or NON_INDUSTRIAL.search(ind):
                r = requests.delete(f"{BASE}/{rec['id']}", headers=HDRS, timeout=8)
                if r.status_code in (200, 204):
                    KNOWN_DOMAINS.discard(rec.get("domain",""))
                    removed += 1
        if removed: print(f"[QC] Rimossi {removed} non-industriali", flush=True)
    except: pass

# ── Main loop ────────────────────────────────────────────────────────────────
def main():
    load_existing()

    while True:
        stats["cycle"] += 1
        print(f"\n{'═'*50}", flush=True)
        print(f"█ CICLO {stats['cycle']} | inserted={stats['inserted']} dup={stats['skipped_dup']} http_fail={stats['skipped_http']}", flush=True)

        # 1. Wikidata — tutti i settori
        for sector_label, qids in INDUSTRY_SECTORS:
            print(f"\n[wikidata] ── {sector_label} ──", flush=True)
            companies = fetch_wikidata(sector_label, qids)
            print(f"  {len(companies)} risultati da Wikidata", flush=True)

            for comp in companies:
                ok = upsert(comp)
                if ok:
                    print(f"  ✅ [{stats['inserted']}] {comp['name'][:40]} | {comp['domain']}", flush=True)
                elif stats["inserted"] > 0 and stats["inserted"] % 50 == 0:
                    auto_qc()

            time.sleep(3)

        # 2. SEC EDGAR — ruota SIC in offset ciclico
        print(f"\n[edgar] ── SEC EDGAR SIC offset={stats['cycle'] * 10} ──", flush=True)
        edgar_companies = fetch_edgar(offset=(stats['cycle'] * 10) % len(MANUFACTURING_SIC))
        for comp in edgar_companies:
            ok = upsert(comp)
            if ok:
                print(f"  ✅ [EDGAR] {comp['name'][:40]} | {comp['domain']}", flush=True)

        # QC finale del ciclo
        auto_qc()

        print(f"\n[ciclo {stats['cycle']}] Fine. inserted={stats['inserted']} | Pausa 30min...", flush=True)
        time.sleep(1800)

if __name__ == "__main__":
    main()

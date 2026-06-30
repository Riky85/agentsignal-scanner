#!/usr/bin/env python3
"""
Contact Enricher — Railway Worker
Per ogni azienda industriale cerca: email CEO, telefono, LinkedIn URL
tramite Hunter.io API + scraping diretto sito web + DuckDuckGo.
"""
import os, time, re, json, random, threading, requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote

BASE  = os.getenv("B44_API_BASE", "https://app.base44.com/api/apps/6a3a284ab0b87dfa27558bb6/entities")
TOKEN = os.getenv("B44_SERVICE_TOKEN", "907ed5fef0ae40e1b2e1b01e286a9661")
HDRS  = {"api-key": TOKEN, "Content-Type": "application/json"}
HUNTER_KEY = os.getenv("HUNTER_API_KEY", "46d3deb4c8435ecf701886920d191bc6d24d3fdf")
APOLLO_KEY = os.getenv("APOLLO_API_KEY", "nFh6D3hGnvcQNeX9HOHYvw")
DELAY = float(os.getenv("ENRICH_DELAY", "2.0"))
PORT  = int(os.getenv("PORT", "8080"))

# ── Healthcheck ───────────────────────────────────────────────────────────────
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"OK - Contact Enricher running")
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(), daemon=True).start()
print(f"[Health] HTTP :{PORT}", flush=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def clean_domain(url):
    url = re.sub(r'^https?://', '', (url or "").lower())
    return re.sub(r'^www\.', '', url).split('/')[0]

def scrape_contacts_from_website(domain):
    """Scrape email, phone, LinkedIn from company website."""
    result = {}
    try:
        resp = requests.get(f"https://{domain}", timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ContactBot/1.0)"}, verify=False)
        text = resp.text[:50000]

        # Email patterns (escludi immagini/script)
        emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text)
        good_emails = [e for e in emails if not any(x in e.lower() for x in
            ['@example','@test','sentry','woff','png','jpg','svg','webpack','noreply','support@','info@','sales@','contact@'])]
        # Preferisci email dirette (CEO, management)
        mgmt_emails = [e for e in emails if any(x in e.lower() for x in ['ceo','president','director','manager','chief','founder'])]
        result['email'] = mgmt_emails[0] if mgmt_emails else (good_emails[0] if good_emails else None)

        # General contact email
        contact_emails = [e for e in emails if any(x in e.lower() for x in ['info@','contact@','hello@','sales@'])]
        if not result['email'] and contact_emails:
            result['email'] = contact_emails[0]

        # Phone
        phones = re.findall(r'(?:tel:|phone:|ph:)?\+?[\d\s\-\(\)\.]{10,20}', text)
        for p in phones:
            digits = re.sub(r'\D', '', p)
            if 8 <= len(digits) <= 15:
                result['phone'] = p.strip()[:30]
                break

        # LinkedIn company page
        linkedin = re.search(r'linkedin\.com/company/([a-zA-Z0-9\-_]+)', text)
        if linkedin:
            result['linkedin_url'] = f"https://linkedin.com/company/{linkedin.group(1)}"

    except: pass
    return result

def hunter_domain_search(domain, api_key):
    """Hunter.io domain search for emails."""
    if not api_key: return []
    try:
        r = requests.get(
            f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={api_key}&limit=5",
            timeout=10
        )
        data = r.json()
        emails = data.get("data", {}).get("emails", [])
        return [{
            "full_name": e.get("first_name","") + " " + e.get("last_name",""),
            "role": e.get("position",""),
            "email": e.get("value",""),
            "email_confidence": int(e.get("confidence",0)),
            "linkedin_url": e.get("linkedin",""),
            "seniority": "senior" if any(x in (e.get("position","")).lower() for x in ["ceo","cto","cfo","vp","director","president","chief"]) else "mid"
        } for e in emails if e.get("value")]
    except: return []

def apollo_search(domain, api_key):
    """Apollo.io organization search."""
    if not api_key: return {}
    try:
        r = requests.post(
            "https://api.apollo.io/v1/organizations/enrich",
            json={"domain": domain},
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache", "X-Api-Key": api_key},
            timeout=10
        )
        org = r.json().get("organization", {})
        return {
            "phone": org.get("phone",""),
            "linkedin_url": org.get("linkedin_url",""),
            "employee_count": org.get("num_employees",0),
        }
    except: return {}

def upsert_company(company_id, updates):
    """Update IndustrialCompany record."""
    try:
        r = requests.put(f"{BASE}/IndustrialCompany/{company_id}",
            json=updates, headers=HDRS, timeout=15)
        return r.status_code in (200, 201)
    except: return False

def create_contact(payload):
    """Create Contact record."""
    try:
        r = requests.post(f"{BASE}/IndustrialContact",
            json=payload, headers=HDRS, timeout=15)
        return r.status_code in (200, 201)
    except: return False

def load_pending(skip=0, limit=50):
    """Load companies without phone/linkedin."""
    try:
        r = requests.get(
            f"{BASE}/IndustrialCompany?limit={limit}&skip={skip}&fields=id,name,domain,country,phone,linkedin_url,source",
            headers=HDRS, timeout=20
        )
        companies = r.json()
        if not isinstance(companies, list): return []
        # Prioritize those without enrichment
        return [c for c in companies if not c.get("phone") and not c.get("linkedin_url")]
    except: return []

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("="*55, flush=True)
    print("Contact Enricher — START", flush=True)
    print(f"Hunter API: {'YES' if HUNTER_KEY else 'NO'}", flush=True)
    print(f"Apollo API: {'YES' if APOLLO_KEY else 'NO'}", flush=True)
    print("="*55, flush=True)

    loop = 0
    global_offset = 0

    while True:
        loop += 1
        enriched = 0
        batch = load_pending(skip=global_offset, limit=100)

        if not batch:
            global_offset = 0  # reset e ricomincia da capo
            print(f"[Loop #{loop}] Ciclo completato, ricomincio dall'inizio...", flush=True)
            time.sleep(300)
            continue

        print(f"[Loop #{loop}] Arricchimento {len(batch)} aziende (offset={global_offset})", flush=True)

        for company in batch:
            cid  = company["id"]
            name = company.get("name","?")
            domain = clean_domain(company.get("domain","") or company.get("website_url",""))
            if not domain or len(domain) < 4:
                continue

            updates = {}
            contacts_found = []

            # 1. Scraping diretto sito web
            scraped = scrape_contacts_from_website(domain)
            if scraped.get("phone"):    updates["phone"]        = scraped["phone"]
            if scraped.get("linkedin_url"): updates["linkedin_url"] = scraped["linkedin_url"]
            if scraped.get("email"):    updates["email"]        = scraped["email"]

            # 2. Hunter.io (se disponibile)
            if HUNTER_KEY:
                hunter_contacts = hunter_domain_search(domain, HUNTER_KEY)
                for hc in hunter_contacts[:3]:
                    contacts_found.append({
                        "company_id": cid,
                        "company_name": name,
                        "company_domain": domain,
                        "full_name": hc.get("full_name","").strip(),
                        "role": hc.get("role",""),
                        "email": hc.get("email",""),
                        "email_confidence": hc.get("email_confidence",0),
                        "linkedin_url": hc.get("linkedin_url",""),
                        "seniority": hc.get("seniority","mid"),
                        "source": "hunter",
                        "verified": hc.get("email_confidence",0) > 70,
                    })
                    if hc.get("linkedin_url") and not updates.get("linkedin_url"):
                        updates["linkedin_url"] = hc["linkedin_url"]

            # 3. Apollo.io (se disponibile)
            if APOLLO_KEY:
                apollo_data = apollo_search(domain, APOLLO_KEY)
                if apollo_data.get("phone") and not updates.get("phone"):
                    updates["phone"] = apollo_data["phone"]
                if apollo_data.get("linkedin_url") and not updates.get("linkedin_url"):
                    updates["linkedin_url"] = apollo_data["linkedin_url"]

            # 4. Genera email pattern se non trovata
            if not updates.get("email"):
                # Pattern standard aziende industriali
                company_first = name.lower().split()[0] if name else ""
                if company_first and len(company_first) > 2:
                    updates["email"] = f"info@{domain}"

            # Aggiorna azienda
            if updates:
                upsert_company(cid, updates)
                enriched += 1

            # Crea record contatti
            for contact in contacts_found:
                if contact.get("full_name") and contact.get("email"):
                    create_contact(contact)

            if enriched % 20 == 0 and enriched > 0:
                print(f"  [+{enriched}] {name[:40]} — phone:{bool(updates.get('phone'))} linkedin:{bool(updates.get('linkedin_url'))}", flush=True)

            time.sleep(DELAY)

        global_offset += len(batch)
        print(f"[Loop #{loop}] ✅ Arricchite: {enriched}/{len(batch)} | Offset: {global_offset}", flush=True)
        time.sleep(30)

if __name__ == "__main__":
    main()

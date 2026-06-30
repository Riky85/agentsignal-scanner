"""
fix_www_urls.py
Aggiorna website_url di tutti i record IndustrialCompany a https://www.{domain}
Gira su Railway, usa la REST API di Base44 con retry + backoff
"""
import requests, re, time, os

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("BASE44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY}

print("[fix_www] Avvio...", flush=True)

# Carica tutti i record
all_recs = []
skip = 0
while True:
    b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=id,domain,website_url",
                     headers=HDRS, timeout=30).json()
    if not isinstance(b, list) or not b: break
    all_recs.extend(b)
    print(f"  Caricati: {len(all_recs)}", flush=True)
    if len(b) < 500: break
    skip += 500

print(f"[fix_www] Totale: {len(all_recs)}", flush=True)

to_fix = []
for rec in all_recs:
    d = re.sub(r'^www\.', '', (rec.get("domain") or "").lower()).split('/')[0].strip()
    if not d or "." not in d: continue
    correct = f"https://www.{d}"
    if rec.get("website_url") != correct:
        to_fix.append((rec["id"], correct))

print(f"[fix_www] Da aggiornare: {len(to_fix)}", flush=True)

updated = 0
for i, (rid, url) in enumerate(to_fix):
    for attempt in range(3):
        try:
            r = requests.put(f"{BASE}/{rid}", json={"website_url": url},
                             headers=HDRS, timeout=10)
            if r.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  [429] rate limit, attendo {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if r.status_code in (200, 201):
                updated += 1
                break
            break
        except Exception as e:
            time.sleep(3)
    
    time.sleep(0.5)  # 2 req/sec max
    if (i+1) % 50 == 0:
        print(f"  [{i+1}/{len(to_fix)}] aggiornati={updated}", flush=True)

print(f"[fix_www] ✅ Completato: {updated}/{len(to_fix)}", flush=True)

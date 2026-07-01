"""
purge_all.py — svuota completamente IndustrialCompany poi esce.
Railway esegue: purge_all.py && feeder_v2_industrial.py
"""
import requests, time, os, concurrent.futures

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("BASE44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY}

print("[purge] Avvio svuotamento totale DB...", flush=True)
total = 0

for attempt in range(200):
    try:
        b = requests.get(f"{BASE}?limit=200&fields=id", headers=HDRS, timeout=30).json()
    except Exception as e:
        print(f"[purge] load err: {e}"); time.sleep(10); continue

    if not isinstance(b, list) or not b:
        print(f"[purge] ✅ DB vuoto. Eliminati totali: {total}"); break

    ids = [r['id'] for r in b]

    def del_one(rid):
        for _ in range(3):
            try:
                r = requests.delete(f"{BASE}/{rid}", headers=HDRS, timeout=10)
                if r.status_code == 429: time.sleep(15); continue
                return r.status_code in (200, 204, 404)
            except: time.sleep(3)
        return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        deleted = sum(ex.map(del_one, ids))
    total += deleted
    print(f"[purge] -{deleted} | tot={total}", flush=True)
    time.sleep(1)

print(f"[purge] Completato. Avvio feeder...", flush=True)

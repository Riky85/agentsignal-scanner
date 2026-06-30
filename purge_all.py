"""
purge_all.py — svuota completamente IndustrialCompany
poi termina, Railway farà ripartire feeder_runner.py
"""
import requests, time, os, concurrent.futures

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("BASE44_APP_ID",  "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY}

print("[purge] Avvio svuotamento completo...", flush=True)

total_deleted = 0
rounds = 0

while True:
    rounds += 1
    # Carica batch di ID
    ids = []
    skip = 0
    while len(ids) < 2000:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}&fields=id", headers=HDRS, timeout=30).json()
            if not isinstance(b, list) or not b: break
            ids.extend([r['id'] for r in b])
            if len(b) < 500: break
            skip += 500
        except Exception as e:
            print(f"  [load err] {e}", flush=True)
            time.sleep(5)
            break

    if not ids:
        print(f"[purge] ✅ DB vuoto dopo {rounds} tornate. Totale eliminati: {total_deleted}", flush=True)
        break

    print(f"[purge] Tornata {rounds}: {len(ids)} record da eliminare...", flush=True)

    def del_one(rid):
        for attempt in range(3):
            try:
                r = requests.delete(f"{BASE}/{rid}", headers=HDRS, timeout=10)
                if r.status_code == 429:
                    time.sleep(15)
                    continue
                return r.status_code in (200, 204, 404)
            except:
                time.sleep(2)
        return False

    deleted_this_round = 0
    BATCH = 20
    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i+BATCH]
        with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH) as ex:
            deleted_this_round += sum(ex.map(del_one, chunk))
        time.sleep(0.8)  # rispetta rate limit

    total_deleted += deleted_this_round
    print(f"  Eliminati questa tornata: {deleted_this_round} | Totale: {total_deleted}", flush=True)
    time.sleep(3)

print("[purge] Completato. Avvio feeder pulito...", flush=True)

#!/usr/bin/env python3
"""
quality_monitor.py — Controlla ogni 50 record:
1. Duplicati (domain)
2. Record spazzatura (non industriali)
3. Descrizioni fasulle generate
4. Industry=default
5. Segnala anomalie via log
"""
import requests, re, time, json, threading, os
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import Counter

API_KEY = os.environ.get("BASE44_API_KEY", "907ed5fef0ae40e1b2e1b01e286a9661")
APP_ID  = os.environ.get("B44_APP_ID", "6a3a284ab0b87dfa27558bb6")
BASE    = f"https://app.base44.com/api/apps/{APP_ID}/entities/IndustrialCompany"
HDRS    = {"api-key": API_KEY, "Content-Type": "application/json"}
PORT    = int(os.environ.get("PORT", "8080"))
CHECK_INTERVAL = 50   # ogni N nuovi record

NON_INDUSTRIAL = re.compile(
    r'(myspace|paginegialle|ubisoft|\.gov$|church|parish|pizzeria|ristorante|'
    r'fondazione(?! research| tech)|onlus|cooperativa sociale|'
    r'assicurazion|banca(?! tech)|editrice(?! tech)|'
    r'luxury brand|fashion group|food(?! processing|manufacturing|tech)|'
    r'hotel|resort|ospedale|farmacia|scuola|università|church)',
    re.I
)
FAKE_DESC = re.compile(r'is an industrial company operating in (default|the \w+\.)', re.I)

report = {"last_check": 0, "total_checked": 0, "deleted": 0, "fixed": 0,
          "dupes": 0, "dirty": 0, "cycle": 0, "db_size": 0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(report).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(), daemon=True).start()
print(f"[monitor] Healthcheck su :{PORT}", flush=True)

def load_all():
    recs, skip = [], 0
    while True:
        try:
            b = requests.get(f"{BASE}?limit=500&skip={skip}", headers=HDRS, timeout=25).json()
            if not isinstance(b, list) or not b: break
            recs.extend(b); skip += 500
            if len(b) < 500: break
        except Exception as e:
            print(f"[load] err: {e}", flush=True); break
    return recs

def check_and_clean(recs):
    to_delete, to_fix = [], []
    seen_domains = {}
    
    for r in recs:
        rid    = r["id"]
        name   = str(r.get("name","") or "")
        domain = str(r.get("domain","") or "")
        ind    = str(r.get("industry","") or "")
        desc   = str(r.get("description","") or "")
        
        # 1. Duplicati
        key = re.sub(r'^(https?://)?(www\.)?',"",domain).rstrip("/").lower()
        if key and key in seen_domains:
            to_delete.append(rid)
            print(f"[dup] {domain}", flush=True)
            continue
        if key: seen_domains[key] = rid
        
        # 2. Non industriali
        if NON_INDUSTRIAL.search(f"{name} {domain} {ind}"):
            to_delete.append(rid)
            print(f"[dirty] {name} | {domain}", flush=True)
            continue
        
        # 3. Descrizione fasulla
        if FAKE_DESC.search(desc):
            to_fix.append(r)
        
        # 4. Industry=default
        if ind == "default":
            to_fix.append(r)
    
    # Esegui eliminazioni
    del_count = 0
    for rid in to_delete:
        try:
            resp = requests.delete(f"{BASE}/{rid}", headers=HDRS, timeout=10)
            if resp.status_code in [200, 204]: del_count += 1
            time.sleep(0.3)
        except: pass
    
    # Esegui fix
    fix_count = 0
    for r in to_fix:
        patch = {}
        desc = str(r.get("description","") or "")
        ind  = str(r.get("industry","") or "")
        if FAKE_DESC.search(desc): patch["description"] = ""
        if ind == "default": patch["industry"] = "Manufacturing"
        if patch:
            try:
                resp = requests.put(f"{BASE}/{r['id']}", json=patch, headers=HDRS, timeout=10)
                if resp.status_code in [200, 204]: fix_count += 1
                time.sleep(0.2)
            except: pass
    
    return del_count, fix_count, len(to_delete), len(to_fix)

# Main loop
prev_size = 0
while True:
    recs = load_all()
    curr_size = len(recs)
    report["db_size"] = curr_size
    
    # Check ogni 50 nuovi record O ogni 10 minuti (whichever comes first)
    if curr_size - prev_size >= CHECK_INTERVAL or (time.time() - report["last_check"]) > 600:
        report["cycle"] += 1
        print(f"[C{report['cycle']}] Check qualità su {curr_size} record (prev={prev_size})", flush=True)
        
        deleted, fixed, dirty, fake = check_and_clean(recs)
        
        report["deleted"] += deleted
        report["fixed"]   += fixed
        report["dirty"]   += dirty
        report["last_check"] = int(time.time())
        report["total_checked"] = curr_size
        
        print(f"[C{report['cycle']}] Eliminati={deleted} | Fixati={fixed} | DB ora={curr_size-deleted}", flush=True)
        prev_size = curr_size - deleted
    
    time.sleep(120)  # check ogni 2 minuti

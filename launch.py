#!/usr/bin/env python3
"""
launch.py — Avvia feeder_v3 + scanner_with_scoring in parallelo
Il feeder gestisce il healthcheck HTTP sulla PORT
Lo scanner gira in thread separato senza conflitti di porta
"""
import subprocess, sys, os, time, threading, json
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = int(os.environ.get("PORT", "8080"))

# Stato condiviso
status = {"feeder": "starting", "scanner": "starting", "ts": 0}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.end_headers()
        self.wfile.write(json.dumps(status).encode())
    def log_message(self, *a): pass

# Avvia healthcheck server
threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), H).serve_forever(),
    daemon=True
).start()
print(f"[launch] Healthcheck su :{PORT}", flush=True)

def run_script(name, script):
    env = {**os.environ, "PYTHONUNBUFFERED": "1",
           "PORT": "0"}  # porta 0 = disabilitato per i subprocess
    while True:
        status[name] = "running"
        print(f"[{name}] START {script}", flush=True)
        p = subprocess.Popen(
            [sys.executable, "-u", script],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=1, text=True, env=env
        )
        for line in p.stdout:
            print(f"[{name}] {line}", end="", flush=True)
        code = p.wait()
        status[name] = f"restarting (exit={code})"
        print(f"[{name}] EXIT {code} — restart in 10s", flush=True)
        time.sleep(10)

# Avvia i due script in thread separati con restart automatico
for name, script in [("feeder","feeder_v3_multisource.py"), ("scanner","scanner_with_scoring.py")]:
    threading.Thread(target=run_script, args=(name, script), daemon=True).start()

# Mantieni il processo principale vivo
while True:
    status["ts"] = int(time.time())
    time.sleep(30)

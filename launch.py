#!/usr/bin/env python3
"""
launch.py — Avvia feeder_v3 + scanner_with_scoring in parallelo
"""
import subprocess, sys, os, time

scripts = [
    ("feeder",  "feeder_v3_multisource.py"),
    ("scanner", "scanner_with_scoring.py"),
]

procs = []
for name, script in scripts:
    print(f"[launch] Avvio {name}: {script}", flush=True)
    p = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"}
    )
    procs.append((name, p))

# Stream output di entrambi
import threading

def stream(name, proc):
    for line in proc.stdout:
        print(f"[{name}] {line}", end="", flush=True)

threads = [threading.Thread(target=stream, args=(n, p), daemon=True) for n, p in procs]
for t in threads: t.start()

# Healthcheck sul main process
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        alive = {n: p.poll() is None for n, p in procs}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(alive).encode())
    def log_message(self, *a): pass

port = int(os.environ.get("PORT", "8080"))
print(f"[launch] Healthcheck su :{port}", flush=True)
HTTPServer(("0.0.0.0", port), H).serve_forever()

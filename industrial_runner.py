#!/usr/bin/env python3
"""Industrial Scanner Runner — avvia HTTP healthcheck + scanner in parallelo"""
import asyncio, threading, os
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

PORT = int(os.environ.get("PORT", 8080))
stats = {"status": "starting", "scanned": 0, "signals": 0, "opportunities": 0}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

# Avvia healthcheck SUBITO
threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), Health).serve_forever(),
    daemon=True
).start()
print(f"Healthcheck HTTP su :{PORT}")
stats["status"] = "running"

# Avvia scanner industriale
from industrial_scanner import main, INDUSTRIAL_SEED
print(f"Industrial Scanner — {len(INDUSTRIAL_SEED)} aziende seed")
asyncio.run(main())
stats["status"] = "done"
print("Industrial scan completato")

# Mantieni il processo vivo (il service si riavvierà il giorno dopo)
import time
while True:
    time.sleep(86400)

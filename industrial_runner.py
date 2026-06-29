#!/usr/bin/env python3
"""Industrial Scanner Runner — healthcheck immediato + scan asincrono"""
import asyncio, os
from industrial_scanner import main, PORT, stats
import json, threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps(stats).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0",PORT),H).serve_forever(), daemon=True).start()
print(f"Healthcheck su :{PORT} — avvio scanner...")
asyncio.run(main())

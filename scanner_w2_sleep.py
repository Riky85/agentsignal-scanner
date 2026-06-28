#!/usr/bin/env python3
"""
scanner-w2 SLEEP MODE
Risponde agli healthcheck Railway senza fare scanning.
Attivato quando WORKER_MODE=sleep
"""
import os, asyncio, aiohttp
from aiohttp import web

PORT = int(os.environ.get("PORT", "8080"))

async def health(request):
    return web.Response(text="OK sleep mode")

async def main():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[W2-SLEEP] Healthcheck attivo su :{PORT} — nessun scan in corso")
    # Loop infinito — non fa nulla
    while True:
        await asyncio.sleep(60)
        print("[W2-SLEEP] alive", flush=True)

if __name__ == "__main__":
    asyncio.run(main())

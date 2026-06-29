#!/usr/bin/env python3
"""Industrial Scanner Runner — il healthcheck e' gia' aperto da industrial_scanner.py al boot"""
import asyncio
from industrial_scanner import main

print("Starting Industrial Scanner v3.0 (multi-page, LLM-ready)...")
asyncio.run(main())

#!/bin/bash
pip install aiohttp beautifulsoup4 lxml -q

ROLE=${WORKER_ROLE:-scanner}
echo "=== Starting worker role: $ROLE ==="

if [ "$ROLE" = "feeder" ]; then
    exec python feeder_runner.py
else
    exec python mass_scanner_runner.py
fi

#!/bin/bash
pip install aiohttp -q

# Determina il ruolo dal nome del servizio Railway o dalla variabile
ROLE=${WORKER_ROLE:-}
SERVICE=${RAILWAY_SERVICE_NAME:-}

if [ "$ROLE" = "feeder" ] || [ "$SERVICE" = "feeder" ]; then
    echo "=== Starting FEEDER (Majestic+GLEIF) ==="
    exec python feeder_runner.py
else
    echo "=== Starting SCANNER (mass_scanner_runner) ==="
    pip install beautifulsoup4 lxml -q
    exec python mass_scanner_runner.py
fi

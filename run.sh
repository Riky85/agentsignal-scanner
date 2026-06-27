#!/bin/bash
SCRIPT="${SCRIPT:-scanner.py}"
echo "Starting: python3 -u $SCRIPT"
exec python3 -u "$SCRIPT"

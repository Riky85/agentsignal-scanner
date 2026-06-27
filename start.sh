#!/bin/bash
set -e
echo "=== AgentSignal Scanner v4.0 ==="
echo "Worker: $WORKER_ID / $TOTAL_WORKERS"
python3 -c "
import os, base64, gzip
code = gzip.decompress(base64.b64decode(os.environ['SCANNER_GZ']))
open('/tmp/s.py', 'wb').write(code)
print(f'Code decompressed: {len(code)} bytes')
"
exec python3 -u /tmp/s.py

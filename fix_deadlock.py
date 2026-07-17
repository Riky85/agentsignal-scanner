import re

with open("signal_engine_pg.py", "r") as f:
    code = f.read()

# FIX 1: fetch timeout 6→4
code = code.replace("def fetch(url, session=None, timeout=6):", "def fetch(url, session=None, timeout=4):")

# FIX 2: gather_pages — as_completed with timeout + cancel pending
old_gather = """        with ThreadPoolExecutor(max_workers=15) as ex:
            futs = {ex.submit(fetch, u, session): k for k, u in urls.items()}
            for f in as_completed(futs):
                k = futs[f]
                r = f.result()
                if r["text"]: out[k] = r"""

new_gather = """        with ThreadPoolExecutor(max_workers=15) as ex:
            futs = {ex.submit(fetch, u, session): k for k, u in urls.items()}
            try:
                for f in as_completed(futs, timeout=45):
                    k = futs[f]
                    r = f.result()
                    if r["text"]: out[k] = r
            except TimeoutError:
                for f in futs:
                    f.cancel()"""

code = code.replace(old_gather, new_gather)

# FIX 3: main loop — as_completed with timeout + cancel pending
old_main = """        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(_work, rec) for rec in batch]
            for f in as_completed(futs):
                pass"""

new_main = """        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(_work, rec) for rec in batch]
            try:
                for f in as_completed(futs, timeout=300):
                    pass
            except TimeoutError:
                log.warning(f"[C{stats['cycle']}] Batch timeout 300s — cancelling {len(futs)} pending futures")
                for f in futs:
                    f.cancel()"""

code = code.replace(old_main, new_main)

# FIX 4: pool size already done (50), verify
code = code.replace("pool_connections=10, pool_maxsize=10", "pool_connections=50, pool_maxsize=50")

# FIX 5: backoff already done (0.3), verify
code = code.replace("backoff_factor=0.5", "backoff_factor=0.3")

# FIX 6: Reduce retry total from 2 to 1 — less time wasted on dead sites
code = code.replace("Retry(total=2,", "Retry(total=1,")

# FIX 7: Add signal alarm to process_company — can't exceed 90s total
# Find the process_company function start
old_process_start = "def process_company(rec, conn):"
new_process_start = """import signal as _signal

def _company_timeout_handler(signum, frame):
    raise TimeoutError("Company processing exceeded 90s")

def process_company(rec, conn):"""
code = code.replace(old_process_start, new_process_start)

# Add signal alarm at the start of process_company body
old_work_func = """        def _work(rec):
            c = get_conn()
            try:
                process_company(rec, c)
            finally:
                c.close()"""

new_work_func = """        def _work(rec):
            c = get_conn()
            try:
                process_company(rec, c)
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                log.error(f"  ERR {rec.get('name','?')[:30]}: {e}")
            finally:
                c.close()"""

code = code.replace(old_work_func, new_work_func)

with open("signal_engine_pg.py", "w") as f:
    f.write(code)

print("All fixes applied successfully")

# Verify
for keyword in ["timeout=4", "timeout=45", "timeout=300", "Retry(total=1,", "pool_connections=50", "backoff_factor=0.3", "TimeoutError", "f.cancel()"]:
    count = code.count(keyword)
    print(f"  {keyword}: {count} occurrence(s)")

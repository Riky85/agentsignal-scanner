import asyncio, aiohttp, psycopg2, os, time, traceback

PG_HOST = os.environ.get("PG_HOST", "postgres-db.railway.internal")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "agentsignal")
PG_USER = os.environ.get("PG_USER", "agent")
PG_PASS = os.environ.get("PG_PASS", "AgentSignal2026!")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "150"))
TIMEOUT = aiohttp.ClientTimeout(total=8, connect=5)

# Piattaforme/social generici: se un'azienda ha come "website" solo un link social,
# la normalizzazione perde il path e risulterebbe erroneamente valido (es. facebook.com sempre raggiungibile).
PLATFORM_BLOCKLIST = {
    "facebook.com","google.com","linkedin.com","instagram.com","twitter.com","x.com","youtube.com",
    "wix.com","godaddy.com","wordpress.com","blogspot.com","weebly.com","squarespace.com","github.com",
    "business.site","pinterest.com","yelp.com","tiktok.com","medium.com","about.me","sites.google.com",
    "gmail.com","yahoo.com","hotmail.com","outlook.com","apple.com","amazon.com","amazonaws.com",
    "shopify.com","myshopify.com","whatsapp.com","telegram.org","t.me","bit.ly","linktr.ee"
}

from psycopg2.extras import execute_values

def connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS, connect_timeout=15)

def parse_size(size_str):
    if not size_str:
        return None
    s = size_str.strip().upper()
    try:
        if 'K+' in s:
            return int(float(s.replace('K+', '')) * 1000)
        if '-' in s:
            lo, hi = s.split('-')
            lo = int(''.join(c for c in lo if c.isdigit()) or 0)
            hi_digits = ''.join(c for c in hi if c.isdigit())
            hi_val = int(hi_digits) if hi_digits else lo
            return (lo + hi_val) // 2 if hi_val else lo
    except Exception:
        return None
    return None

async def check_domain(session, domain):
    for scheme in ("https://", "http://"):
        url = f"{scheme}{domain}"
        try:
            async with session.get(url, timeout=TIMEOUT, allow_redirects=True, ssl=False) as resp:
                if resp.status < 500:
                    final_host = str(resp.url).split("//")[-1].split("/")[0].replace("www.", "")
                    if final_host in PLATFORM_BLOCKLIST:
                        return False, None
                    return True, str(resp.url)
        except Exception:
            continue
    return False, None

async def process_batch(batch):
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; AgentSignalBot/1.0)"}
    results = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        async def worker(row):
            domain = row[0]
            async with sem:
                if domain in PLATFORM_BLOCKLIST:
                    results.append((row, False, None))
                    return
                ok, final_url = await check_domain(session, domain)
                results.append((row, ok, final_url))
        await asyncio.gather(*(worker(r) for r in batch))
    return results

def recover_stuck():
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE opendata_staging SET status='pending' WHERE status='processing'")
    n = cur.rowcount
    cur.execute("UPDATE opendata_staging SET status='dead' WHERE status='pending' AND domain = ANY(%s)", (list(PLATFORM_BLOCKLIST),))
    n2 = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Recovery: {n} righe 'processing' rimesse in pending, {n2} domini piattaforma marcati come morti.", flush=True)

def run_cycle():
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT domain, name, industry, size, founded, city, state, country_code
        FROM opendata_staging WHERE status='pending' LIMIT %s
    """, (BATCH_SIZE,))
    batch = cur.fetchall()
    if not batch:
        conn.close()
        return None

    domains = [r[0] for r in batch]
    cur.execute("UPDATE opendata_staging SET status='processing' WHERE domain = ANY(%s)", (domains,))
    conn.commit()
    conn.close()

    results = asyncio.run(process_batch(batch))

    to_insert = []
    valid_domains = []
    dead_domains = []
    for row, ok, final_url in results:
        domain, name, industry, size, founded, city, state, country_code = row
        if ok:
            valid_domains.append(domain)
            emp = parse_size(size)
            safe_name = (name or domain.split('.')[0].replace('-', ' ').title())[:250]
            to_insert.append((
                domain, safe_name, final_url or f"https://{domain}", country_code, city, industry, emp,
                'bigpicture_opendata', 'pending', False, False
            ))
        else:
            dead_domains.append(domain)

    conn = connect()
    cur = conn.cursor()
    try:
        if to_insert:
            execute_values(cur, """
                INSERT INTO industrial_company
                (domain, name, website_url, country, city, industry, employee_count, source, scan_status, scanned, dirty)
                VALUES %s ON CONFLICT (domain) DO NOTHING
            """, to_insert, page_size=200)
        if valid_domains:
            cur.execute("UPDATE opendata_staging SET status='imported' WHERE domain = ANY(%s)", (valid_domains,))
        if dead_domains:
            cur.execute("UPDATE opendata_staging SET status='dead' WHERE domain = ANY(%s)", (dead_domains,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"ERRORE nel batch (rollback, verra' ritentato riga per riga): {e}", flush=True)
        # Fallback: prova riga per riga cosi' una sola riga sporca non blocca l'intero batch
        for row in to_insert:
            try:
                cur.execute("""
                    INSERT INTO industrial_company
                    (domain, name, website_url, country, city, industry, employee_count, source, scan_status, scanned, dirty)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (domain) DO NOTHING
                """, row)
                conn.commit()
            except Exception as e2:
                conn.rollback()
                print(f"Riga scartata ({row[0]}): {e2}", flush=True)
        if valid_domains:
            cur.execute("UPDATE opendata_staging SET status='imported' WHERE domain = ANY(%s)", (valid_domains,))
            conn.commit()
        if dead_domains:
            cur.execute("UPDATE opendata_staging SET status='dead' WHERE domain = ANY(%s)", (dead_domains,))
            conn.commit()
    conn.close()
    return len(valid_domains), len(dead_domains)

def main_loop():
    print("Opendata importer avviato.", flush=True)
    recover_stuck()
    total_imported = 0
    total_dead = 0
    cycles = 0
    while True:
        try:
            r = run_cycle()
        except Exception as e:
            print(f"Errore imprevisto nel ciclo, continuo dopo 10s: {e}", flush=True)
            traceback.print_exc()
            time.sleep(10)
            continue
        if r is None:
            print("Coda staging esaurita. Sleep 5 min e ricontrollo.", flush=True)
            time.sleep(300)
            continue
        imp, dead = r
        total_imported += imp
        total_dead += dead
        cycles += 1
        if cycles % 5 == 0:
            print(f"[ciclo {cycles}] Importate finora: {total_imported} | morte: {total_dead}", flush=True)

if __name__ == "__main__":
    main_loop()

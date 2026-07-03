import asyncio, aiohttp, psycopg2, os, time, traceback, re

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

# Safety-net: alcune righe del dataset BigPicture (o future fonti) possono avere codici
# ISO-3 (es. "USA","TUV") invece di ISO-2 standard usato ovunque nel resto della pipeline.
# Senza questa normalizzazione, questi codici sporchi finirebbero nel filtro Country della UI
# creando voci duplicate/incoerenti (bug gia' riscontrato e corretto sui dati esistenti il 2026-07-03).
ISO3_TO_ISO2 = {'AFG': 'AF', 'ALA': 'AX', 'ALB': 'AL', 'DZA': 'DZ', 'ASM': 'AS', 'AND': 'AD', 'AGO': 'AO', 'AIA': 'AI', 'ATA': 'AQ', 'ATG': 'AG', 'ARG': 'AR', 'ARM': 'AM', 'ABW': 'AW', 'AUS': 'AU', 'AUT': 'AT', 'AZE': 'AZ', 'BHS': 'BS', 'BHR': 'BH', 'BGD': 'BD', 'BRB': 'BB', 'BLR': 'BY', 'BEL': 'BE', 'BLZ': 'BZ', 'BEN': 'BJ', 'BMU': 'BM', 'BTN': 'BT', 'BOL': 'BO', 'BES': 'BQ', 'BIH': 'BA', 'BWA': 'BW', 'BVT': 'BV', 'BRA': 'BR', 'IOT': 'IO', 'BRN': 'BN', 'BGR': 'BG', 'BFA': 'BF', 'BDI': 'BI', 'CPV': 'CV', 'KHM': 'KH', 'CMR': 'CM', 'CAN': 'CA', 'CYM': 'KY', 'CAF': 'CF', 'TCD': 'TD', 'CHL': 'CL', 'CHN': 'CN', 'CXR': 'CX', 'CCK': 'CC', 'COL': 'CO', 'COM': 'KM', 'COG': 'CG', 'COD': 'CD', 'COK': 'CK', 'CRI': 'CR', 'CIV': 'CI', 'HRV': 'HR', 'CUB': 'CU', 'CUW': 'CW', 'CYP': 'CY', 'CZE': 'CZ', 'DNK': 'DK', 'DJI': 'DJ', 'DMA': 'DM', 'DOM': 'DO', 'ECU': 'EC', 'EGY': 'EG', 'SLV': 'SV', 'GNQ': 'GQ', 'ERI': 'ER', 'EST': 'EE', 'SWZ': 'SZ', 'ETH': 'ET', 'FLK': 'FK', 'FRO': 'FO', 'FJI': 'FJ', 'FIN': 'FI', 'FRA': 'FR', 'GUF': 'GF', 'PYF': 'PF', 'ATF': 'TF', 'GAB': 'GA', 'GMB': 'GM', 'GEO': 'GE', 'DEU': 'DE', 'GHA': 'GH', 'GIB': 'GI', 'GRC': 'GR', 'GRL': 'GL', 'GRD': 'GD', 'GLP': 'GP', 'GUM': 'GU', 'GTM': 'GT', 'GGY': 'GG', 'GIN': 'GN', 'GNB': 'GW', 'GUY': 'GY', 'HTI': 'HT', 'HMD': 'HM', 'VAT': 'VA', 'HND': 'HN', 'HKG': 'HK', 'HUN': 'HU', 'ISL': 'IS', 'IND': 'IN', 'IDN': 'ID', 'IRN': 'IR', 'IRQ': 'IQ', 'IRL': 'IE', 'IMN': 'IM', 'ISR': 'IL', 'ITA': 'IT', 'JAM': 'JM', 'JPN': 'JP', 'JEY': 'JE', 'JOR': 'JO', 'KAZ': 'KZ', 'KEN': 'KE', 'KIR': 'KI', 'PRK': 'KP', 'KOR': 'KR', 'KWT': 'KW', 'KGZ': 'KG', 'LAO': 'LA', 'LVA': 'LV', 'LBN': 'LB', 'LSO': 'LS', 'LBR': 'LR', 'LBY': 'LY', 'LIE': 'LI', 'LTU': 'LT', 'LUX': 'LU', 'MAC': 'MO', 'MKD': 'MK', 'MDG': 'MG', 'MWI': 'MW', 'MYS': 'MY', 'MDV': 'MV', 'MLI': 'ML', 'MLT': 'MT', 'MHL': 'MH', 'MTQ': 'MQ', 'MRT': 'MR', 'MUS': 'MU', 'MYT': 'YT', 'MEX': 'MX', 'FSM': 'FM', 'MDA': 'MD', 'MCO': 'MC', 'MNG': 'MN', 'MNE': 'ME', 'MSR': 'MS', 'MAR': 'MA', 'MOZ': 'MZ', 'MMR': 'MM', 'NAM': 'NA', 'NRU': 'NR', 'NPL': 'NP', 'NLD': 'NL', 'NCL': 'NC', 'NZL': 'NZ', 'NIC': 'NI', 'NER': 'NE', 'NGA': 'NG', 'NIU': 'NU', 'NFK': 'NF', 'MNP': 'MP', 'NOR': 'NO', 'OMN': 'OM', 'PAK': 'PK', 'PLW': 'PW', 'PSE': 'PS', 'PAN': 'PA', 'PNG': 'PG', 'PRY': 'PY', 'PER': 'PE', 'PHL': 'PH', 'PCN': 'PN', 'POL': 'PL', 'PRT': 'PT', 'PRI': 'PR', 'QAT': 'QA', 'REU': 'RE', 'ROU': 'RO', 'RUS': 'RU', 'RWA': 'RW', 'BLM': 'BL', 'SHN': 'SH', 'KNA': 'KN', 'LCA': 'LC', 'MAF': 'MF', 'SPM': 'PM', 'VCT': 'VC', 'WSM': 'WS', 'SMR': 'SM', 'STP': 'ST', 'SAU': 'SA', 'SEN': 'SN', 'SRB': 'RS', 'SYC': 'SC', 'SLE': 'SL', 'SGP': 'SG', 'SXM': 'SX', 'SVK': 'SK', 'SVN': 'SI', 'SLB': 'SB', 'SOM': 'SO', 'ZAF': 'ZA', 'SGS': 'GS', 'SSD': 'SS', 'ESP': 'ES', 'LKA': 'LK', 'SDN': 'SD', 'SUR': 'SR', 'SJM': 'SJ', 'SWE': 'SE', 'CHE': 'CH', 'SYR': 'SY', 'TWN': 'TW', 'TJK': 'TJ', 'TZA': 'TZ', 'THA': 'TH', 'TLS': 'TL', 'TGO': 'TG', 'TKL': 'TK', 'TON': 'TO', 'TTO': 'TT', 'TUN': 'TN', 'TUR': 'TR', 'TKM': 'TM', 'TCA': 'TC', 'TUV': 'TV', 'UGA': 'UG', 'UKR': 'UA', 'ARE': 'AE', 'GBR': 'GB', 'USA': 'US', 'UMI': 'UM', 'URY': 'UY', 'UZB': 'UZ', 'VUT': 'VU', 'VEN': 'VE', 'VNM': 'VN', 'VGB': 'VG', 'VIR': 'VI', 'WLF': 'WF', 'ESH': 'EH', 'YEM': 'YE', 'ZMB': 'ZM', 'ZWE': 'ZW'}

def norm_country_code(code):
    if not code:
        return None
    c = code.strip().upper()
    if len(c) == 2:
        return c
    if len(c) == 3:
        return ISO3_TO_ISO2.get(c)  # None se non riconosciuto: meglio NULL che un codice sporco
    return None  # nomi completi o altri formati anomali: da non inserire cosi' come sono

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
                # Solo 2xx/3xx sono "vivo". <500 accettava anche 404/410/403 (siti morti o bloccati)
                # e URL finale che termina in una pagina di errore comune (404.html, not-found, ecc.)
                final_url_str = str(resp.url)
                final_path = final_url_str.split("//")[-1].split("/", 1)[1].lower() if "/" in final_url_str.split("//")[-1] else ""
                looks_like_error_page = any(p in final_path for p in ("404", "not-found", "notfound", "error", "page-not-found"))
                if 200 <= resp.status < 400 and not looks_like_error_page:
                    final_host = final_url_str.split("//")[-1].split("/")[0].replace("www.", "")
                    if final_host in PLATFORM_BLOCKLIST:
                        return False, None
                    return True, final_url_str
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
            fallback_name = domain.split('.')[0].replace('-', ' ').title()
            candidate_name = name or fallback_name
            clean_len = len(re.sub(r'[^a-zA-Z0-9]', '', candidate_name))
            JUNK_NAMES = {"xx", "x", "na", "n a", "nd", "tbd", "unknown", "_", "-", "none", "null", "test"}
            is_junk = clean_len <= 2 or candidate_name.strip().lower() in JUNK_NAMES
            if is_junk:
                # Nome troppo corto/placeholder (es. "xx", "_", "A"): scarta, non e' un lead utile.
                dead_domains.append(domain)
                continue
            valid_domains.append(domain)
            emp = parse_size(size)
            safe_name = (name or fallback_name)[:250]
            to_insert.append((
                domain, safe_name, final_url or f"https://{domain}", norm_country_code(country_code), city, industry, emp,
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
    start_time = time.time()
    while True:
        # Railway killa il container periodicamente (probabile limite di memoria) dopo ~8-9 min.
        # Meglio uscire noi puliti PRIMA di quel punto: il supervisore bash ci fa ripartire in 3s
        # con memoria fresca, invece di subire un kill non controllato a metà batch.
        if time.time() - start_time > 300 or cycles >= 100:
            print(f"[ciclo {cycles}] Restart preventivo pulito dopo {int(time.time()-start_time)}s per liberare memoria.", flush=True)
            return
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

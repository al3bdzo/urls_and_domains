import pandas as pd
import requests
import random
import time
import socket
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
INPUT_FILE = "cleaned_data.csv"
TIMEOUT = 6
RETRIES = 2
MAX_WORKERS = 5   # 🔥 tweak: 5–8 is safe

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

SALLA = ["cdn.assets.salla.network", "cdn.salla.sa", "cdn.files.salla.network"]
ZID = ["media.zid.store", "assets.zid.store"]

ADFAZ = "تم التطوير بواسطة ادفاز | ADFAZ"
MUTHRI = "تم التطوير بواسطة مثري | Muthri"
LAK = ["تم التطوير بواسطة لك | LAK", "theme.lak.sa", "lak/tenants"]

# ================= HELPERS =================

def resolve_domain(domain):
    try:
        socket.gethostbyname(domain)
        return True
    except:
        return False

def get_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS)
    })
    return session

def safe_request(session, url):
    for _ in range(RETRIES):
        try:
            return session.get(url, timeout=TIMEOUT, allow_redirects=True)
        except:
            time.sleep(random.uniform(0.3, 1.0))
    return None

def detect_platform(html):
    html_lower = html.lower()

    if ADFAZ in html:
        return "adfaz"

    if MUTHRI in html:
        return "muthri"

    if any(fp in html_lower for fp in LAK):
        return "lak"

    if any(fp in html_lower for fp in SALLA):
        return "salla"

    if any(fp in html_lower for fp in ZID):
        return "zid"

    return None

def classify_status(response, html):
    if response is None:
        return "dead_domain"

    html_lower = html.lower()

    if "cloudflare" in html_lower or response.status_code == 403:
        return "blocked"

    if response.status_code >= 400:
        return "error"

    if "domain for sale" in html_lower or "parked" in html_lower:
        return "parked"

    if len(html.strip()) < 200:
        return "error"

    return "alive"

# ================= PROCESS FUNCTION =================

def process_row(i, row):
    domain = str(row["domain"]).strip()
    print(f"🔍 Checking: {domain}")

    if not resolve_domain(domain):
        return i, "", "dead_domain"

    session = get_session()
    url = f"http://{domain}"
    response = safe_request(session, url)

    if response is None:
        return i, "", "dead_domain"

    html = response.text

    platform = detect_platform(html)

    if platform:
        print(f"✅ {domain} → PLATFORM: {platform}")
        return i, platform, "alive"

    status = classify_status(response, html)
    print(f"⚠️ {domain} → STATUS: {status}")

    return i, "", status

# ================= LOAD =================

df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")

if "platform" not in df.columns:
    df["platform"] = ""

if "status" not in df.columns:
    df["status"] = ""

if "submitted" not in df.columns:
    df["submitted"] = ""

df["platform"] = df["platform"].astype("object").fillna("")
df["status"] = df["status"].astype("object").fillna("")
df["submitted"] = df["submitted"].astype("object").fillna("")

today_date = datetime.now().date()

# ================= FILTER =================

tasks = []

for i, row in df.iterrows():
    try:
        row_date = pd.to_datetime(row["added_at"], errors="coerce")
        if pd.isna(row_date) or row_date.date() != today_date:
            continue
    except:
        continue

    if str(row["platform"]).strip() != "":
        continue

    tasks.append((i, row))

print(f"\n🚀 Total tasks: {len(tasks)}")

# ================= PARALLEL EXECUTION =================

results = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_row, i, row) for i, row in tasks]

    for future in as_completed(futures):
        results.append(future.result())

# ================= APPLY RESULTS =================

for i, platform, status in results:
    if platform:
        df.at[i, "platform"] = platform
    df.at[i, "status"] = status

# ================= SORT & SAVE =================

df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
df["added_at_dt"] = pd.to_datetime(df["added_at"], errors="coerce")

if "submitted" not in df.columns:
    df["submitted"] = ""

df["submitted"] = df["submitted"].astype("object").fillna("")

df = df.sort_values(by=["added_at_dt", "submitted", "creation_date"], ascending=[False, True, False])
df.drop(columns=["added_at_dt"], inplace=True)

df.to_csv(INPUT_FILE, encoding="utf-8-sig", index=False)

print("\n✅ DONE")
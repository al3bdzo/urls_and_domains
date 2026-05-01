import csv
import os
import random
import socket
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import tldextract
import whois

from .config import DEFAULT_INPUT_FILE, DEFAULT_CLEANED_FILE, DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILE
from .csv_utils import (
    DEFAULT_COLUMNS,
    read_csv,
    ensure_columns,
    normalize_dates,
    normalize_text,
    sort_dataframe,
    save_csv,
)


def import_domains(input_path=None, output_path=None):
    input_path = input_path or DEFAULT_INPUT_FILE
    output_path = output_path or DEFAULT_CLEANED_FILE
    domains = []

    with open(input_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            url = str(row[0]).strip()
            source = str(row[1]).strip() if len(row) > 1 else ""
            if not url:
                continue
            ext = tldextract.extract(url)
            if ext.domain and ext.suffix:
                domains.append({"domain": f"{ext.domain}.{ext.suffix}", "source": source})

    df_input = pd.DataFrame(domains, columns=["domain", "source"]).drop_duplicates(subset=["domain"])
    df_existing = read_csv(output_path, DEFAULT_COLUMNS)
    df_existing = ensure_columns(df_existing, DEFAULT_COLUMNS)
    df_existing["domain"] = df_existing["domain"].astype(str).str.strip()

    existing_domains = set(df_existing["domain"].dropna())
    df_new = df_input[~df_input["domain"].isin(existing_domains)].copy()
    df_new = df_new.drop_duplicates(subset=["domain"])
    df_new["creation_date"] = ""
    df_new["age_years"] = ""
    df_new["added_at"] = date.today()
    df_new["platform"] = ""
    df_new["status"] = ""
    df_new["submitted"] = ""
    df_new["phone_number"] = ""

    df_all = pd.concat([df_existing, df_new], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["domain"], keep="first")
    df_all = normalize_dates(df_all)
    df_all = normalize_text(df_all)
    df_all = sort_dataframe(df_all)
    save_csv(df_all, output_path)

    return len(df_new), output_path


def submit_today_rows(input_path=None, output_path=None, today=None, dry_run=False):
    input_path = input_path or DEFAULT_CLEANED_FILE
    output_path = output_path or DEFAULT_OUTPUT_FILE
    today = today or date.today()
    df_source = read_csv(input_path, DEFAULT_COLUMNS)
    df_target = read_csv(output_path, DEFAULT_COLUMNS)

    df_source = ensure_columns(df_source, DEFAULT_COLUMNS)
    df_target = ensure_columns(df_target, DEFAULT_COLUMNS)
    df_source = normalize_dates(df_source)
    df_source = normalize_text(df_source)
    df_target = normalize_dates(df_target)
    df_target = normalize_text(df_target)

    mask = (
        (df_source["added_at"] == today)
        & df_source["creation_date"].notna()
        & df_source["platform"].astype(str).str.strip().ne("")
        & df_source["submitted"].astype(str).str.strip().eq("")
    )

    df_to_submit = df_source[mask].copy()
    submitted_count = len(df_to_submit)

    if submitted_count > 0 and not dry_run:
        df_source.loc[mask, "submitted"] = "yes"
        df_to_submit["submitted"] = "yes"
        df_target = pd.concat([df_target, df_to_submit], ignore_index=True)
        df_target = df_target.drop_duplicates(subset=["domain"], keep="last")
        df_target = sort_dataframe(df_target)
        save_csv(df_target, output_path)

    df_source = sort_dataframe(df_source)
    if not dry_run:
        save_csv(df_source, input_path)

    return submitted_count, output_path


SALLA_BLACKLIST = [
    "docs.salla.sa",
    "help.salla.sa",
    "blog.salla.sa",
    "developers.salla.sa",
    "salla.sa/page",
    "salla.sa/blog",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

SALLA_PLATFORMS = ["cdn.assets.salla.network", "cdn.salla.sa", "cdn.files.salla.network"]
ZID_PLATFORMS = ["media.zid.store", "assets.zid.store"]
ADFAZ = "تم التطوير بواسطة ادفاز"
MUTHRI = "تم التطوير بواسطة مثري"
LAK_PLATFORMS = ["تم التطوير بواسطة لك | LAK", "theme.lak.sa", "lak/tenants"]

RETRIES = 2
TIMEOUT = 6


def fill_creation_dates(input_path=None, output_path=None, today=None, max_workers=20):
    input_path = input_path or DEFAULT_CLEANED_FILE
    output_path = output_path or input_path
    today = today or date.today()
    df = read_csv(input_path, DEFAULT_COLUMNS)
    df = ensure_columns(df, DEFAULT_COLUMNS)
    df = normalize_text(df)

    df["added_at"] = pd.to_datetime(df["added_at"], errors="coerce").dt.date
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    mask = (df["added_at"] == today) & df["creation_date"].isna()
    df_to_process = df[mask]
    print(f"🔎 Checking creation date for {len(df_to_process)} domain(s)...")

    def normalize_timestamp(value):
        if value is None or pd.isna(value):
            return None
        try:
            ts = pd.to_datetime(value, errors="coerce", utc=True)
            if pd.isna(ts):
                return None
            return ts.tz_localize(None)
        except Exception:
            return None

    def check_domain(domain):
        print(f"Checking WHOIS for {domain}...")
        try:
            w = whois.whois(domain)
            creation_date = w.creation_date
            if isinstance(creation_date, list):
                creation_date = creation_date[0]
            if creation_date:
                processed_date = normalize_timestamp(creation_date)
                if processed_date:
                    age = datetime.now().year - processed_date.year
                    print(f"✅ Found creation date for {domain}: {processed_date}")
                    return domain, processed_date, age
            print(f"❌ No valid creation date found for {domain}")
        except Exception:
            print(f"❌ WHOIS failed for {domain}")
        return domain, None, None

    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_domain, row["domain"]): row["domain"] for _, row in df_to_process.iterrows()}
        for future in as_completed(futures):
            domain, creation_date, age = future.result()
            if creation_date:
                results[domain] = (creation_date, age)

    df["creation_date"] = df["creation_date"].astype(object)
    for idx, row in df.iterrows():
        domain = str(row["domain"]).strip()
        if domain in results:
            creation_date, age = results[domain]
            df.at[idx, "creation_date"] = creation_date
            df.at[idx, "age_years"] = age

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce").dt.date
    df = normalize_text(df)
    df = sort_dataframe(df)
    save_csv(df, output_path)

    print(f"✅ Created dates updated for {len(results)} row(s) and saved to {output_path}")
    return len(results), output_path


def detect_platforms(input_path=None, output_path=None, today=None, max_workers=5):
    input_path = input_path or DEFAULT_CLEANED_FILE
    output_path = output_path or input_path
    today = today or date.today()
    df = read_csv(input_path, DEFAULT_COLUMNS)
    df = ensure_columns(df, DEFAULT_COLUMNS)
    df = normalize_text(df)

    df["added_at"] = pd.to_datetime(df["added_at"], errors="coerce").dt.date

    def resolve_domain(domain):
        try:
            socket.gethostbyname(domain)
            return True
        except Exception:
            return False

    def get_session():
        session = requests.Session()
        session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
        return session

    def safe_request(session, url):
        for _ in range(RETRIES):
            try:
                return session.get(url, timeout=TIMEOUT, allow_redirects=True)
            except Exception:
                time.sleep(random.uniform(0.3, 1.0))
        return None

    def detect_platform(html):
        html_lower = html.lower()
        if ADFAZ in html:
            return "adfaz"
        if MUTHRI in html:
            return "muthri"
        if any(fp in html_lower for fp in LAK_PLATFORMS):
            return "lak"
        if any(fp in html_lower for fp in SALLA_PLATFORMS):
            return "salla"
        if any(fp in html_lower for fp in ZID_PLATFORMS):
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

    def process_row(index, row):
        domain = str(row["domain"]).strip()
        print(f"Checking platform for {domain}...")
        if not resolve_domain(domain):
            print(f"❌ {domain} is not resolvable")
            return index, "", "dead_domain"
        session = get_session()
        response = safe_request(session, f"http://{domain}")
        if response is None:
            print(f"❌ HTTP check failed for {domain}")
            return index, "", "dead_domain"
        html = response.text
        platform = detect_platform(html)
        if platform:
            print(f"✅ {domain} → PLATFORM: {platform}")
            return index, platform, "alive"
        status = classify_status(response, html)
        print(f"⚠️ {domain} → STATUS: {status}")
        return index, "", status

    mask = (df["added_at"] == today) & df["platform"].astype(str).str.strip().eq("")
    df_to_process = df[mask]
    print(f"🔎 Checking platform for {len(df_to_process)} domain(s)...")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, idx, row) for idx, row in df_to_process.iterrows()]
        for future in as_completed(futures):
            results.append(future.result())

    for idx, platform, status in results:
        if platform:
            df.at[idx, "platform"] = platform
        df.at[idx, "status"] = status

    df = normalize_text(df)
    df = sort_dataframe(df)
    save_csv(df, output_path)

    print(f"✅ Platform check complete for {len(results)} row(s) and saved to {output_path}")
    return len(results), output_path


def process_salla_stores(input_path, output_path=None, max_workers=15):
    output_path = output_path or DEFAULT_CLEANED_FILE
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    DUMMY_CREATION_DATE = date(2026, 1, 1)

    rows = []
    with open(input_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            raw_url = str(row[0]).strip()
            source = str(row[1]).strip() if len(row) > 1 else "salla"
            if not raw_url:
                continue
            normalized_url = raw_url if raw_url.startswith("http") else f"https://{raw_url}"
            parsed = urlparse(normalized_url)
            netloc = parsed.netloc.lower()
            if netloc != "salla.sa":
                continue
            clean_url = normalized_url
            if any(block in clean_url for block in SALLA_BLACKLIST):
                continue
            handle = get_salla_store_handle(clean_url)
            if handle:
                rows.append({"store_handle": handle, "full_url": clean_url, "source": source})

    if not rows:
        print("⚠️ No Salla store URLs found in input file.")
        return 0, output_path

    print(f"🔎 Found {len(rows)} Salla store URL(s) to resolve...")
    df = pd.DataFrame(rows).drop_duplicates(subset=["store_handle"]).copy()
    df["base_url"] = df["store_handle"].apply(lambda handle: f"https://salla.sa/{handle}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        df["resolved_domain"] = list(executor.map(resolve_salla_store_domain, df["base_url"]))

    def format_row(row):
        resolved = row["resolved_domain"]
        if isinstance(resolved, str) and resolved:
            is_paid = "salla.sa" not in resolved
            domain_str = resolved if is_paid else f"salla.sa/{row['store_handle']}"
            return domain_str, is_paid
        return f"salla.sa/{row['store_handle']}", False

    formatted = df.apply(lambda r: pd.Series(format_row(r), index=["domain", "is_paid"]), axis=1)
    df = pd.concat([df, formatted], axis=1)
    df["date_run"] = datetime.now().strftime("%Y-%m-%d")

    final_df = df[["domain", "is_paid", "date_run", "source"]].copy()
    final_df["creation_date"] = ""
    final_df["age_years"] = ""
    final_df["added_at"] = date.today()
    final_df["platform"] = "salla"
    final_df["status"] = final_df["is_paid"].apply(lambda paid: "paid" if paid else "unpaid")
    final_df["submitted"] = ""
    final_df = final_df.drop(columns=["is_paid", "date_run"])

    existing_df = read_csv(output_path, DEFAULT_COLUMNS)
    existing_df = ensure_columns(existing_df, DEFAULT_COLUMNS)
    existing_df["domain"] = existing_df["domain"].astype(str).str.strip()

    existing_domains = set(existing_df["domain"].dropna())
    new_domains = final_df[~final_df["domain"].isin(existing_domains)].copy()

    new_domains.loc[new_domains["status"] == "unpaid", "creation_date"] = DUMMY_CREATION_DATE.strftime("%Y-%m-%d")

    final_df = pd.concat([existing_df, new_domains], ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["domain"], keep="first")
    final_df = normalize_dates(final_df)
    final_df = normalize_text(final_df)
    final_df = sort_dataframe(final_df)
    save_csv(final_df, output_path)

    print(f"✅ Salla import complete: saved {len(new_domains)} new rows to {output_path}")
    return len(new_domains), output_path


def input_contains_salla_urls(input_path):
    try:
        with open(input_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for row in csv.reader(f):
                if not row:
                    continue
                url = str(row[0]).strip().lower()
                if "salla.sa" in url:
                    return True
    except FileNotFoundError:
        raise
    return False


def _split_input_rows(input_path):
    salla_rows = []
    other_rows = []
    with open(input_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            raw_url = str(row[0]).strip()
            if not raw_url:
                continue
            if "salla.sa" in raw_url.lower():
                salla_rows.append(row)
            else:
                other_rows.append(row)
    return salla_rows, other_rows


def _write_rows_to_temp_csv(rows):
    temp_file = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8-sig", newline="", suffix=".csv", delete=False)
    try:
        writer = csv.writer(temp_file)
        writer.writerows(rows)
        temp_file.close()
        return temp_file.name
    except Exception:
        temp_file.close()
        os.unlink(temp_file.name)
        raise


def process_input_file(input_path=None, output_path=None, max_workers=15):
    input_path = input_path or DEFAULT_INPUT_FILE
    output_path = output_path or DEFAULT_CLEANED_FILE

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    salla_rows, other_rows = _split_input_rows(input_path)
    total_processed = 0

    if other_rows:
        temp_input = _write_rows_to_temp_csv(other_rows)
        try:
            count, _ = import_domains(temp_input, output_path)
            total_processed += count
        finally:
            os.unlink(temp_input)

    if salla_rows:
        temp_input = _write_rows_to_temp_csv(salla_rows)
        try:
            _, _ = process_salla_stores(temp_input, output_path, max_workers=max_workers)
            total_processed += len(salla_rows)
        finally:
            os.unlink(temp_input)

    return total_processed, output_path


def get_salla_store_handle(url):
    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        if not path:
            return None
        parts = path.split("/")
        handle = parts[0]
        if handle in ["", "auth", "search", "p", "blog", "category", "tags"]:
            return None
        return handle
    except Exception:
        return None


def resolve_salla_store_domain(url):
    print(f"Checking Salla store: {url}")
    try:
        response = requests.get(url, timeout=5, allow_redirects=True)
        resolved = urlparse(response.url).netloc.lower()
        if resolved:
            print(f"✅ Resolved {url} → {resolved}")
        else:
            print(f"❌ Failed to resolve {url}")
        return resolved
    except Exception:
        print(f"❌ Request failed for {url}")
        return ""


def prepare_output_dir(path=None):
    directory = Path(path or DEFAULT_OUTPUT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return directory

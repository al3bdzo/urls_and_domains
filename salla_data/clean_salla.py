import pandas as pd
import requests
import os
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- Configuration ---
INPUT_FILE = "input.csv"
OUTPUT_FILE = "unique_salla_stores.csv"
MAX_WORKERS = 15 
BLACKLIST = ["docs.salla.sa", "help.salla.sa", "blog.salla.sa", "developers.salla.sa", "salla.sa/page", "salla.sa/blog"]

def normalize(url):
    try:
        url = str(url).strip()
        if not url: 
            return None, None
        if not url.startswith("http"): 
            url = "https://" + url
        parsed = urlparse(url)
        return parsed.netloc.lower(), url
    except:
        return None, None

def get_store_handle(url):
    try:
        path = urlparse(url).path.strip('/')
        parts = path.split('/')
        if parts:
            handle = parts[0]
            if handle in ['', 'auth', 'search', 'p', 'blog', 'category', 'tags']:
                return None
            return handle
    except:
        return None

def resolve_domain(url):
    try:
        print(f"Checking: {url}")
        r = requests.get(url, timeout=5, allow_redirects=True, stream=True)
        return urlparse(r.url).netloc.lower()
    except:
        print(f"Error connecting to: {url}")
        return None

def main():
    start_time = datetime.now()
    current_date = start_time.strftime("%Y-%m-%d")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found!")
        return

    print(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    url_col = df.columns[0]
    
    df[['netloc', 'full_url']] = df[url_col].apply(lambda x: pd.Series(normalize(x)))
    df = df.dropna(subset=['full_url'])
    df = df[df['netloc'] == 'salla.sa']
    df = df[~df['full_url'].apply(lambda x: any(b in x for b in BLACKLIST))]
    
    df['store_handle'] = df['full_url'].apply(get_store_handle)
    df = df.dropna(subset=['store_handle'])
    
    unique_stores = df.drop_duplicates(subset=['store_handle']).copy()
    unique_stores['base_url'] = unique_stores['store_handle'].apply(lambda x: f"https://salla.sa/{x}")
    
    print(f"Found {len(unique_stores)} unique stores to process.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        unique_stores['resolved_domain'] = list(executor.map(resolve_domain, unique_stores['base_url']))

    # --- FIXED SECTION ---
    def format_row(row):
        resolved = row['resolved_domain']
        # Check if resolved is a string and not empty/null
        if isinstance(resolved, str) and resolved:
            is_paid = 'salla.sa' not in resolved
            domain_str = resolved if is_paid else f"salla.sa/{row['store_handle']}"
            return domain_str, is_paid
        else:
            # Fallback if connection failed: treat as unpaid
            return f"salla.sa/{row['store_handle']}", False

    unique_stores[['domain', 'is_paid']] = unique_stores.apply(lambda r: pd.Series(format_row(r)), axis=1)
    # ----------------------

    
    new_data = unique_stores[['domain', 'is_paid']].copy()
    new_data['date_run'] = current_date

    if os.path.exists(OUTPUT_FILE):
        print(f"Appending to existing file: {OUTPUT_FILE}")
        existing_df = pd.read_csv(OUTPUT_FILE)
        if 'is_paid' not in existing_df.columns:
            existing_df['is_paid'] = existing_df['domain'].apply(lambda x: 'salla.sa/' not in str(x))
        
        final_df = pd.concat([existing_df, new_data], ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['domain'], keep='first')
    else:
        print(f"Creating new file: {OUTPUT_FILE}")
        final_df = new_data

    print("Sorting: Paid domains on top...")
    final_df = final_df.sort_values(by='is_paid', ascending=False)
    final_df[['domain', 'date_run']].to_csv(OUTPUT_FILE, index=False)
    
    print(f"Done! Total unique stores: {len(final_df)}")
    print(f"Time taken: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
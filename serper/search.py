import requests
import json
import os
import pandas as pd
from urllib.parse import urlparse
import time
from dotenv import load_dotenv
from .config import DORKS_FILE, OUTPUT_FILE, PAGES_TO_FETCH

load_dotenv()
SERPER_API_KEY = os.getenv("SERPER_API_KEY")


def extract_domain(url):
    try:
        parsed = urlparse(url)
        return parsed.netloc if parsed.netloc else None
    except:
        return None

def fetch_serper_results(dork, api_key, page_num):
    url = "https://google.serper.dev/search"
    
    payload = json.dumps({
        "q": dork,
        "gl": "sa",        # Saudi Arabia
        "hl": "ar",        # Arabic language
        "autocorrect": False,
        "page": page_num   # Pagination
    })
    
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[!] Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[!] Request failed: {e}")
        return None

def serper_search():
    # 1. Load Dorks
    try:
        with open(DORKS_FILE, 'r', encoding='utf-8') as f:
            dorks = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[!] {DORKS_FILE} not found.")
        return

    all_domains = set()

    # 2. Process Dorks
    for dork in dorks:
        print(f"[*] Starting deep scan for: {dork[:50]}...")
        
        for p in range(1, PAGES_TO_FETCH + 1):
            data = fetch_serper_results(dork, SERPER_API_KEY, p)
            
            if not data or 'organic' not in data:
                break
                
            organic = data['organic']
            if not organic:
                break
                
            print(f"    [Page {p}] Found {len(organic)} results.")
            
            for entry in organic:
                link = entry.get('link')
                domain = extract_domain(link)
                if domain:
                    all_domains.add(domain)
            
            # Serper is very fast, but let's be polite
            time.sleep(0.3)

    # 3. Export
    if all_domains:
        df = pd.DataFrame(list(all_domains), columns=["domain"])
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n[+] Success! {len(all_domains)} unique domains saved to {OUTPUT_FILE}")
    else:
        print("\n[!] No domains found.")

import pandas as pd
import whois
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

input_file = "cleaned_data.csv"
output_file = "cleaned_data.csv"

today = datetime.now().date()
df = pd.read_csv(input_file, encoding="utf-8-sig")

# 1. Normalize existing dates but KEEP as object for now to avoid resolution locks
df['added_at'] = pd.to_datetime(df['added_at'], errors='coerce').dt.date
# Convert to datetime, then to object to allow flexible assignment in the loop
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

if 'submitted' not in df.columns:
    df['submitted'] = ""
df['submitted'] = df['submitted'].astype('object').fillna("")

mask = (df['added_at'] == today) & (df['creation_date'].isna())
df_to_process = df[mask]

print(f"Processing {len(df_to_process)} domains (missing creation_date)...")

def normalize_timestamp(value):
    if value is None or pd.isna(value):
        return None # Use None instead of NaT for object-type columns
    try:
        ts = pd.to_datetime(value, errors='coerce', utc=True)
        if pd.isna(ts):
            return None
        return ts.tz_localize(None) # Remove timezone
    except:
        return None

def check_domain(domain):
    print(f"Checking: {domain}")
    try:
        w = whois.whois(domain)
        creation_date = w.creation_date
        if isinstance(creation_date, list):
            creation_date = creation_date[0]
        
        if creation_date:
            processed_date = normalize_timestamp(creation_date)
            if processed_date:
                age = datetime.now().year - processed_date.year
                print(f"✅ Done: {domain}")
                return domain, processed_date, age
    except Exception:
        print(f"❌ Failed: {domain}")
    return domain, None, None

results = {}
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_domain, row['domain']): row['domain'] for _, row in df_to_process.iterrows()}
    for future in as_completed(futures):
        domain, creation_date, age = future.result()
        if creation_date:
            results[domain] = (creation_date, age)

# 2. IMPORTANT: Force the column to 'object' type before updating
# This prevents the 'Cannot losslessly convert units' error
df['creation_date'] = df['creation_date'].astype(object)

for idx, row in df.iterrows():
    domain = row['domain']
    if domain in results:
        creation_date, age = results[domain]
        df.at[idx, 'creation_date'] = creation_date
        df.at[idx, 'age_years'] = age

# 3. Final Clean up: Convert back to simple date format
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce').dt.date
df['added_at'] = pd.to_datetime(df['added_at'], errors='coerce').dt.date

if 'submitted' not in df.columns:
    df['submitted'] = ""
df['submitted'] = df['submitted'].astype('object').fillna("")

df = df.sort_values(by=['added_at', 'submitted', 'creation_date'], ascending=[False, True, False])
df = df.drop_duplicates(subset='domain')
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print("✅ Data updated successfully (no duplicates, only missing filled)")
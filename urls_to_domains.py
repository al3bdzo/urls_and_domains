import csv
import tldextract
from datetime import datetime
import os
import pandas as pd

input_file = "input.csv"
output_file = "cleaned_data.csv"

today = datetime.now().date()

new_rows = []

# Step 1: Extract domains + source (UTF-8 safe)
with open(input_file, "r", encoding="utf-8-sig") as f:
    reader = csv.reader(f)
    
    for row in reader:
        if not row or len(row) < 2:
            continue
        
        url = row[0].strip()
        source = row[1].strip()
        
        if not url:
            continue
        
        ext = tldextract.extract(url)
        
        if ext.domain and ext.suffix:
            domain = f"{ext.domain}.{ext.suffix}"
            new_rows.append((domain, source))

df_input = pd.DataFrame(new_rows, columns=["domain", "source"])

# Step 2: Load existing data (UTF-8 safe)
if os.path.exists(output_file):
    df_existing = pd.read_csv(output_file, encoding="utf-8-sig")
else:
    df_existing = pd.DataFrame(columns=[
        "domain", "source", "creation_date", "age_years", "added_at", "platform", "status", "submitted"
    ])

# Ensure columns exist
for col in ["domain", "source", "creation_date", "age_years", "added_at", "platform", "status", "submitted"]:
    if col not in df_existing.columns:
        df_existing[col] = None

# Clean existing domains
df_existing["domain"] = df_existing["domain"].astype(str).str.strip()
existing_domains = set(df_existing["domain"].dropna())

# Step 3: KEEP ONLY truly new domains
df_new = df_input[~df_input["domain"].isin(existing_domains)].copy()

# 🔥 Fix 1: remove duplicates inside new data
df_new = df_new.drop_duplicates(subset="domain")

# Add extra fields
df_new["creation_date"] = None
df_new["age_years"] = None
df_new["added_at"] = today
df_new["platform"] = ""
df_new["status"] = ""
df_new["submitted"] = ""

# Step 4: Combine
df_all = pd.concat([df_existing, df_new], ignore_index=True)

# Final dedup safety
df_all = df_all.drop_duplicates(subset="domain")

# Step 5: Sort
# Keep today's non-submitted rows at the top of the same added_at date
if "submitted" not in df_all.columns:
    df_all["submitted"] = ""

df_all["submitted"] = df_all["submitted"].astype("object").fillna("")
df_all["added_at"] = pd.to_datetime(df_all["added_at"], errors="coerce")
df_all['creation_date'] = pd.to_datetime(df_all['creation_date'], errors='coerce').dt.date

df_all = df_all.sort_values(by=['added_at', 'submitted', 'creation_date'], ascending=[False, True, False])

# Step 6: Save (UTF-8 for Excel compatibility)
df_all.to_csv(output_file, index=False, encoding="utf-8-sig")

# ✅ Correct count (after dedup)
print(f"✅ Added {len(df_new)} new unique domains (first-source preserved)")
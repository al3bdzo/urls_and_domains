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

# Step 3: Remove duplicates inside input
df_input = df_input.drop_duplicates()

# 🔥 Step 4: Merge sources for existing domains
domain_to_index = {row["domain"]: idx for idx, row in df_existing.iterrows()}

for _, row in df_input.iterrows():
    domain = row["domain"]
    new_source = row["source"]

    if domain in domain_to_index:
        idx = domain_to_index[domain]
        existing_source = df_existing.at[idx, "source"]

        if pd.isna(existing_source) or existing_source == "":
            df_existing.at[idx, "source"] = new_source
        else:
            existing_sources_set = set(s.strip() for s in str(existing_source).split(" | "))
            
            if new_source not in existing_sources_set:
                updated_sources = existing_sources_set.union({new_source})
                df_existing.at[idx, "source"] = " | ".join(sorted(updated_sources))
    else:
        # New domain → add later
        pass

# Step 5: KEEP ONLY truly new domains
df_new = df_input[~df_input["domain"].isin(existing_domains)].copy()

# Remove duplicates inside new data
df_new = df_new.drop_duplicates(subset="domain")

# Add extra fields
df_new["creation_date"] = None
df_new["age_years"] = None
df_new["added_at"] = today
df_new["platform"] = ""
df_new["status"] = ""
df_new["submitted"] = ""

# Step 6: Combine
df_all = pd.concat([df_existing, df_new], ignore_index=True)

# Final dedup safety
df_all = df_all.drop_duplicates(subset="domain")

# Step 7: Sort
df_all['creation_date'] = pd.to_datetime(df_all['creation_date'], errors='coerce').dt.date
df_all['added_at'] = pd.to_datetime(df_all['added_at'], errors='coerce').dt.date

if 'submitted' not in df_all.columns:
    df_all['submitted'] = ""

df_all['submitted'] = df_all['submitted'].astype('object').fillna("")

# ✅ Sort
df_all = df_all.sort_values(
    by=['added_at', 'submitted', 'creation_date'],
    ascending=[False, True, False]
)
# Step 8: Save (UTF-8 for Excel compatibility)
df_all.to_csv(output_file, index=False, encoding="utf-8-sig")

# Correct count
print(f"✅ Added {len(df_new)} new unique domains (sources merged correctly)")
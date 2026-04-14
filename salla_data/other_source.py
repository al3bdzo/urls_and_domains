from google.cloud import bigquery
import tldextract
import csv

# Initialize client (requires GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client()

query = """
SELECT url
FROM `bigquery-public-data.common_crawl.wet`
WHERE url LIKE '%.sa%'
AND REGEXP_CONTAINS(content, r'cdn\\.salla\\.network')
LIMIT 5000
"""

# Run query
query_job = client.query(query)

domains = set()

for row in query_job:
    url = row.url
    
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        domain = f"{ext.domain}.{ext.suffix}"
        domains.add(domain)

# Save results
with open("salla_domains.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["domain"])
    
    for d in sorted(domains):
        writer.writerow([d])

print(f"Saved {len(domains)} unique domains.")
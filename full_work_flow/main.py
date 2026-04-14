import csv
from datetime import datetime
import tldextract

input_file = "input.csv"
output_file = "cleaned_data.csv"

today = datetime.now().date()
new_rows = []
salla_stores = []


def main():
    with open(input_file, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)

        for row in reader:
            if "salla.sa" in row:
                salla_stores.append(row)
                continue

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


main()
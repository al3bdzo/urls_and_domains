# KSA Stores Domains

A project for processing, cleaning, and enriching domain data from Saudi Arabian e-commerce stores. Exports approved rows into a final output CSV.

## Features

- **Domain Import**: Process domains from `input.csv` into `cleaned_data.csv`
- **Salla Store Support**: Special handling for Salla.sa stores with:
  - Automatic domain resolution
  - Paid/unpaid status detection
  - Proper deduplication (won't re-add existing domains)
  - Dummy creation date for unpaid stores
- **WHOIS Enrichment**: Fill missing creation dates via WHOIS lookup
- **Platform Detection**: Detect e-commerce platforms (Salla, Zid, Lak, Adfaz, Muthri)
- **Serper Integration**: Search Google using dorks to find store domains

## Project Layout

```
KSA_Stores_domains/
├── cleaned_data.csv          # Main source dataset
├── Final_output/             # Submitted rows output
│   └── cleaned_data.csv
├── input.csv                 # Raw domain input source
├── dorks.txt                 # Search queries for serper
├── run.py                    # CLI entrypoint
├── requirements.txt          # Python dependencies
├── ksa_stores/               # Core package
│   ├── __init__.py
│   ├── config.py             # Default paths and encoding
│   ├── csv_utils.py          # CSV read/write and normalization
│   ├── schema.py             # Standard CSV column list
│   └── workflows.py          # Import, submit, and processing workflows
└── serper/                   # Serper search integration
    ├── config.py             # Dorks and output settings
    └── search.py             # Google search via Serper API
```

## CSV Schema

| Column | Description |
|--------|-------------|
| `domain` | Store domain or Salla URL |
| `source` | Source of the domain (from input file) |
| `creation_date` | WHOIS creation date or dummy for unpaid |
| `age_years` | Age in years |
| `added_at` | Date added to cleaned_data.csv |
| `platform` | Detected platform (salla, zid, lak, adfaz, muthri) |
| `status` | Domain status (alive, blocked, error, parked, dead_domain, paid, unpaid) |
| `submitted` | Whether submitted to output |
| `phone_number` | Contact phone (if available) |

## Setup

### 1. Create Virtual Environment

```bash
python -m venv .venv
```

### 2. Activate Virtual Environment

**Windows (PowerShell):**
```powershell
.venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```cmd
.venv\Scripts\activate.bat
```

**Linux/Mac:**
```bash
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```env
# Serper API Key (get from https://serper.dev)
SERPER_API_KEY=your_api_key_here
```

## Usage

All commands are run via `python run.py <command>`.

### Import Domains

Reads `input.csv` and imports domains into `cleaned_data.csv`. Automatically handles Salla.sa URLs with special processing.

```bash
python run.py import
```

**What it does:**
- Reads domains from `input.csv`
- For Salla URLs: resolves actual domain, detects paid/unpaid status
- Skips duplicate domains (won't re-add with today's date)
- Sets source from input file (second column)
- For unpaid Salla stores: sets creation_date to `2026-01-01`

**Input CSV format:**
```
salla.sa/store-handle,my-source
example.com
https://store.sa,another-source
```

### Submit Today's Rows

Moves rows with today's date from `cleaned_data.csv` to `Final_output/cleaned_data.csv`.

```bash
python run.py submit
```

### Fill WHOIS Creation Dates

Queries WHOIS for domains added today that are missing creation dates.

```bash
python run.py dates
```

### Detect Platform and Status

Checks each domain to detect the e-commerce platform and status.

```bash
python run.py platform
```

### Serper Search

Searches Google using dorks from `dorks.txt` and saves results to `input.csv`.

```bash
python run.py serper
```

## Salla Store Fixes

The following issues were fixed in the Salla store processing:

1. **Deduplication**: Existing domains are no longer re-added with today's date
2. **Source Field**: Source is now taken from the input file instead of hardcoded "salla"
3. **Unpaid Domains**: Unpaid Salla stores get a dummy creation_date (`2026-01-01`) instead of being empty

## Configuration

Edit `ksa_stores/config.py` to change default paths:

```python
DEFAULT_INPUT_FILE = "input.csv"
DEFAULT_CLEANED_FILE = "cleaned_data.csv"
DEFAULT_OUTPUT_DIR = "Final_output"
DEFAULT_OUTPUT_FILE = "Final_output/cleaned_data.csv"
CSV_ENCODING = "utf-8-sig"
```

Edit `serper/config.py` to change search settings:

```python
DORKS_FILE = "dorks.txt"
OUTPUT_FILE = "input.csv"
PAGES_TO_FETCH = 50
```

## Notes

- CSV files use `utf-8-sig` encoding for Excel compatibility
- Sorting order: `added_at` (desc) → `submitted` (asc) → `creation_date` (desc)
- New domains receive default empty values for `platform`, `status`, and `submitted`
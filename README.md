# KSA Stores Domains

This project processes domain data, cleans and enriches it, and exports approved rows into a final output CSV.

## What changed

A small reusable package was added under `ksa_stores/` to centralize CSV handling, sorting, and workflow logic.

Key improvements:
- shared CSV schema and normalization logic
- consistent sort order for all workflows
- `submitted` flag support in `cleaned_data.csv`
- a command-line entrypoint via `python -m ksa_stores`
- legacy scripts remain but now reuse package code where possible

## Project layout

- `cleaned_data.csv` — main source dataset
- `Final_output/cleaned_data.csv` — submitted rows output
- `input.csv` — raw domain input source
- `ksa_stores/` — shared package module
  - `csv_utils.py` — read/write and normalization helpers
  - `workflows.py` — import and submission workflows
  - `config.py` — default paths and encoding
  - `schema.py` — standard CSV column list
  - `__main__.py` — CLI entrypoint
- `run.py` — simple CLI runner for the package workflows

## Requirements

Install dependencies in the existing virtual environment:

```bash
python -m pip install -r requirements.txt
```

## Usage

Use the simple `run.py` CLI for all tasks.

### Import domains and route Salla input

This reads `input.csv`, and if any URLs contain `salla.sa`, it runs the Salla-aware import flow into `cleaned_data.csv`. Otherwise it imports normal domains into `cleaned_data.csv`.

```bash
python run.py import
```

### Submit today's rows

This moves rows from `cleaned_data.csv` to `Final_output/cleaned_data.csv` and marks them as submitted in `cleaned_data.csv`.

```bash
python run.py submit
```

### Fill missing WHOIS creation dates

```bash
python run.py dates
```

### Detect platform and status

```bash
python run.py platform
## Notes

- CSV files are read and written using `utf-8-sig` for Excel compatibility.
- Sorting is always applied as:
  1. `added_at` descending
  2. `submitted` ascending
  3. `creation_date` descending
- New domains receive default empty values for `platform`, `status`, and `submitted`.

## Next steps

Future refactors can add:
- a `--dry-run` mode for import as well as submit
- validation and backup support before overwrites
- unit tests for core helpers

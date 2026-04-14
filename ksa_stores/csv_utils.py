import os
from pathlib import Path
import pandas as pd
from .config import CSV_ENCODING
from .schema import DEFAULT_COLUMNS


def read_csv(path, columns=None):
    path = Path(path)
    if path.exists():
        return pd.read_csv(path, encoding=CSV_ENCODING)
    return pd.DataFrame(columns=columns or DEFAULT_COLUMNS)


def ensure_columns(df, columns=None):
    columns = columns or DEFAULT_COLUMNS
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_dates(df):
    if "added_at" in df.columns:
        df["added_at"] = pd.to_datetime(df["added_at"], errors="coerce").dt.date
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce").dt.date
    return df


def normalize_text(df):
    for col in ["platform", "status", "submitted"]:
        if col in df.columns:
            df[col] = df[col].astype(object).fillna("")
    return df


def sort_dataframe(df):
    if "added_at" in df.columns:
        df["added_at"] = pd.to_datetime(df["added_at"], errors="coerce")
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce").dt.date
    if "submitted" not in df.columns:
        df["submitted"] = ""
    df["submitted"] = df["submitted"].astype(object).fillna("")
    return df.sort_values(by=["added_at", "submitted", "creation_date"], ascending=[False, True, False])


def save_csv(df, path):
    df = df.reindex(columns=DEFAULT_COLUMNS)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding=CSV_ENCODING)


def backup_csv(path):
    path = Path(path)
    if not path.exists():
        return None
    backup_dir = path.parent / "backup"
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / f"{path.stem}_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df = pd.read_csv(path, encoding=CSV_ENCODING)
    df.to_csv(backup_path, index=False, encoding=CSV_ENCODING)
    return backup_path

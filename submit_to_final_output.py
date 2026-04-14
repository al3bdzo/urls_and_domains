import os
from datetime import date
import pandas as pd

INPUT_FILE = "cleaned_data.csv"
OUTPUT_DIR = "Final_output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "cleaned_data.csv")
CSV_ENCODING = "utf-8-sig"
DEFAULT_COLUMNS = [
    "domain",
    "source",
    "creation_date",
    "age_years",
    "added_at",
    "platform",
    "status",
    "submitted",
]


def read_csv(path, columns=None):
    if os.path.exists(path):
        return pd.read_csv(path, encoding=CSV_ENCODING)
    return pd.DataFrame(columns=columns or [])


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_date_columns(df):
    if "added_at" in df.columns:
        df["added_at"] = pd.to_datetime(df["added_at"], errors="coerce").dt.date
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce").dt.date
    return df


def normalize_text_columns(df):
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


def save_dataframe(df, path):
    df = df.reindex(columns=DEFAULT_COLUMNS)
    df.to_csv(path, index=False, encoding=CSV_ENCODING)


def main():
    df_source = read_csv(INPUT_FILE, DEFAULT_COLUMNS)
    df_target = read_csv(OUTPUT_FILE, DEFAULT_COLUMNS)

    df_source = ensure_columns(df_source, DEFAULT_COLUMNS)
    df_target = ensure_columns(df_target, DEFAULT_COLUMNS)

    df_source = normalize_date_columns(df_source)
    df_source = normalize_text_columns(df_source)

    today = date.today()
    mask = (
        (df_source["added_at"] == today)
        & df_source["creation_date"].notna()
        & df_source["platform"].astype(str).str.strip().ne("")
        & df_source["submitted"].astype(str).str.strip().eq("")
    )

    df_to_submit = df_source[mask].copy()

    if df_to_submit.empty:
        print("⚠️ No eligible rows to submit today.")
    else:
        df_to_submit["submitted"] = "yes"

        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, exist_ok=True)

        df_target = pd.concat([df_target, df_to_submit], ignore_index=True)
        df_target = df_target.drop_duplicates(subset="domain", keep="last")
        df_target = sort_dataframe(df_target)
        save_dataframe(df_target, OUTPUT_FILE)

        print(f"✅ Submitted {len(df_to_submit)} row(s) to {OUTPUT_FILE}")

    df_source = sort_dataframe(df_source)
    save_dataframe(df_source, INPUT_FILE)
    print(f"✅ Updated source file {INPUT_FILE}")


if __name__ == "__main__":
    main()

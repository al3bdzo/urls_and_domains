"""Simple CLI runner for KSA Stores workflows."""

import argparse
from ksa_stores.workflows import (
    process_input_file,
    submit_today_rows,
    fill_creation_dates,
    detect_platforms,
)

from serper.search import serper_search

def main():
    parser = argparse.ArgumentParser(description="Run KSA Stores tasks")
    parser.add_argument(
        "command",
        choices=["import", "submit", "dates", "platform", "serper"],
        help="Task to execute",
    )
    args = parser.parse_args()

    if args.command == "import":
        count, output_path = process_input_file()
        print(f"✅ Imported or updated {count} cleaned rows into {output_path}")
    elif args.command == "submit":
        count, output_path = submit_today_rows()
        print(f"✅ Submitted {count} row(s) from cleaned_data.csv to {output_path}")
    elif args.command == "dates":
        count, output_path = fill_creation_dates()
        print(f"✅ Filled creation_date for {count} row(s) in {output_path}")
    elif args.command == "platform":
        count, output_path = detect_platforms()
        print(f"✅ Updated platform/status for {count} row(s) in {output_path}")
    elif args.command == "serper":
        serper_search()



if __name__ == "__main__":
    main()

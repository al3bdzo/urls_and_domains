import json
import csv

# Configuration
input_filename = 'data.json'
output_filename = 'input.csv'

def convert_json_to_csv():
    try:
        # Load the JSON data
        with open(input_filename, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        # Access the list of data inside 'buckets'
        buckets_data = data.get('buckets', [])

        if not buckets_data:
            print("No data found in the 'buckets' field.")
            return

        # Get headers from the first object keys
        headers = buckets_data[0].keys()

        # Write to CSV
        with open(output_filename, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(buckets_data)

        print(f"Success! {len(buckets_data)} rows written to {output_filename}")

    except FileNotFoundError:
        print(f"Error: {input_filename} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    convert_json_to_csv()
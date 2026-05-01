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

        # Extract only the 'key' column
        urls = [item.get('key', '') for item in buckets_data if item.get('key')]

        # Write to CSV - single column
        with open(output_filename, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['url'])
            for url in urls:
                writer.writerow([url])

        print(f"Success! {len(urls)} URLs written to {output_filename}")

    except FileNotFoundError:
        print(f"Error: {input_filename} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    convert_json_to_csv()
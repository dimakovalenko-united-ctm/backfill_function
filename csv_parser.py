#!/usr/bin/env python
import csv
import json

def csv_to_json(csv_file_path):
    # Initialize an empty list to store the JSON objects
    json_data = []

    # Open the CSV file for reading
    with open("/Users/Dima/Downloads/btc_price_backup_2025-02-18.csv", mode='r', newline='', encoding='utf-8') as csvfile:
        # Create a CSV DictReader object
        reader = csv.DictReader(csvfile)

        # Iterate over each row in the CSV file
        for row in reader:
            # Construct the JSON object based on the given format
            json_obj = {
                "Open": float(row['open']),
                "CryptoName": "Bitcoin",
                "CryptoSymbol": "BTC",
                "Ticker": "BTC-USD",
                "FiatCurrency": "USD",
                "Source": "yahoo-finance",
                "Close": float(row['close']),
                "High": float(row['high']),
                "Low": float(row['low']),
                "Volume": float(row['volume']),
                "timestamp": row['timestamp']
            }

            # Append the JSON object to the list
            json_data.append(json_obj)

    # Convert the list of JSON objects to a JSON string
    return json.dumps(json_data, indent=4)


# Example usage:
# Assuming the CSV file is named 'data.csv' and is located in the current directory.
csv_file_path = 'data.csv'
json_output = csv_to_json(csv_file_path)
print(json_output)
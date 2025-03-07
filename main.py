#!/usr/bin/env python
import functions_framework
import requests
from google.cloud import storage
import csv
import json

class FileDoesNotExistError(FileNotFoundError):  # Inherit from FileNotFoundError
    """Custom exception raised when a file does not exist."""
    def __init__(self, filename, message="File not found"):
        self.filename = filename
        self.message = message
        super().__init__(self.message)  # Initialize the base class

    def __str__(self):  # Optional: Customize the string representation
        return f"{self.message}: {self.filename}"


def get_backup_csv_from_bucket(filename=None ):    
    # Project ID of the bucket's project
    bucket_project_id = "spider-ctm"  # Replace with the actual project ID
    bucket_name = "spider-ctm"  # Replace with your bucket name
    
    
    try:
        # Create a storage client, explicitly providing the project ID
        storage_client = storage.Client(project=bucket_project_id)
        bucket = storage_client.bucket(bucket_name)

        if filename is None:
            blobs = list(bucket.list_blobs())

            if not blobs:
                error = "No objects found in the bucket."
                print(error)
                raise FileDoesNotExistError("latest", error)

            blobs.sort(key=lambda blob: blob.updated, reverse=True)

            blob = blobs[0]
            print(f"No specific file was provided. File {filename} exists {blob.exists()} which is latest")
        else:
            try:
                blob = bucket.blob(filename)                
                print(f"File {filename} exists {blob.exists()}")

            except Exception as e:
                error = f"No Object {filename} in {bucket_name} in project {bucket_project_id}: {e}"
                raise FileDoesNotExistError(filename, error)


        blob_content = blob.download_as_bytes()
        csv_string = blob_content.decode("utf-8")

        
        return csv_string

    except Exception as e:
        print(f"Error processing blob: {e}")
        raise e

def csv_to_json(csv_string):

    json_data = []
    reader = csv.DictReader(csv_string.splitlines())

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
            json_data.append(json_obj)

    print(f"JSON Data records: {len(json_data)}")

    return json_data

def invoke_other_function(parsed_json):

    try:
        # Replace with the URL of the other Cloud Function
        post_end_point = "https://us-central1-dev-test-staging.cloudfunctions.net/post_prices"  

        # Make the POST request
        # import ipdb; ipdb.set_trace()
        response = requests.post(
            post_end_point,
            json=parsed_json,  # Send parsed_json as JSON payload
            headers={"Content-Type": "application/json"} #,  # Important: Set Content-Type header
            #timeout=10  # Optional: Set a timeout for the request
        )

        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        return response.json()  # Return the JSON response from the other function

    except requests.exceptions.RequestException as e:
        error = f"Error invoking other function: {e}"
        print(error)
        raise e

@functions_framework.http
def handler(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = None

    try:
        csv_string = get_backup_csv_from_bucket(name)
    except Exception as e:
        error = f"Error getting CSV from Bucket: {e}"
        print(error)
        raise e

    
    try:
        import ipdb; ipdb.set_trace()

        json_string = csv_to_json(csv_string)
    except Exception as e:
        error = f"Error Converting CSV to JSON: {e}"
        print(error)
        raise e

    # import ipdb; ipdb.set_trace()
    try:
        print(invoke_other_function(json_string))
    except Exception as e:
        print(f"Error invoking other function: {e}")
        raise e
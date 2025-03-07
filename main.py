#!/usr/bin/env python
import functions_framework
import requests
from google.cloud import storage
import csv
import json
import concurrent.futures
from datetime import datetime
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileDoesNotExistError(FileNotFoundError):
    """Custom exception raised when a file does not exist."""
    def __init__(self, filename, message="File not found"):
        self.filename = filename
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}: {self.filename}"

def get_backup_csv_from_bucket(filename, bucket_name="spider-ctm", bucket_project_id="spider-ctm"):
    """
    Retrieves CSV content from a GCS bucket.
    
    Args:
        filename: Name of the file to retrieve
        bucket_name: GCS bucket name
        bucket_project_id: GCP project ID
        
    Returns:
        String content of the CSV file
    
    Raises:
        FileDoesNotExistError: If the file doesn't exist in the bucket
    """
    try:
        # Create a storage client, explicitly providing the project ID
        storage_client = storage.Client(project=bucket_project_id)
        bucket = storage_client.bucket(bucket_name)

        try:
            blob = bucket.blob(filename)
            if not blob.exists():
                error = f"File {filename} does not exist in bucket {bucket_name}"
                logger.error(error)
                raise FileDoesNotExistError(filename, error)
                
            logger.info(f"Downloading file {filename}")
            blob_content = blob.download_as_bytes()
            csv_string = blob_content.decode("utf-8")
            
            return csv_string

        except Exception as e:
            error = f"Error accessing {filename} in {bucket_name}: {e}"
            logger.error(error)
            raise FileDoesNotExistError(filename, error)

    except Exception as e:
        logger.error(f"Error processing blob: {e}")
        raise e

def get_all_files_in_bucket(bucket_name="spider-ctm", bucket_project_id="spider-ctm", prefix="btc_price_backup_"):
    """
    Lists all files in a GCS bucket with a specified prefix.
    
    Args:
        bucket_name: GCS bucket name
        bucket_project_id: GCP project ID
        prefix: File prefix to filter by
        
    Returns:
        List of filenames that match the prefix
    """
    try:
        storage_client = storage.Client(project=bucket_project_id)
        bucket = storage_client.bucket(bucket_name)
        
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            logger.warning(f"No files found with prefix '{prefix}' in bucket {bucket_name}")
            return []
            
        return [blob.name for blob in blobs]
        
    except Exception as e:
        logger.error(f"Error listing files in bucket: {e}")
        raise e

def csv_to_json(csv_string):
    """
    Converts CSV string content to JSON objects.
    
    Args:
        csv_string: CSV content as string
        
    Returns:
        List of dictionaries representing rows from the CSV
    """
    json_data = []
    reader = csv.DictReader(csv_string.splitlines())

    for row in reader:
        # Construct the JSON object based on the given format
        try:
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
        except (ValueError, KeyError) as e:
            logger.warning(f"Skipping row due to parsing error: {e}, Row: {row}")
            continue

    logger.info(f"Parsed {len(json_data)} records from CSV")
    return json_data

def batch_data(data, batch_size=100):
    """
    Splits data into batches of the specified size.
    
    Args:
        data: List of data items
        batch_size: Size of each batch
        
    Returns:
        List of batches, where each batch is a list of data items
    """
    return [data[i:i + batch_size] for i in range(0, len(data), batch_size)]

def invoke_other_function(data_batch):
    """
    Invokes another Cloud Function with the given data.
    
    Args:
        data_batch: List of data items to send
        
    Returns:
        Response from the invoked function
    """
    try:
        # Replace with the URL of the other Cloud Function
        post_endpoint = "https://us-central1-dev-test-staging.cloudfunctions.net/post_prices/prices"

        # Make the POST request
        import ipdb; ipdb.set_trace()
        response = requests.post(
            post_endpoint,
            json=data_batch,
            headers={"Content-Type": "application/json"},
            timeout=60  # Increase timeout for larger batches
        )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        error = f"Error invoking other function: {e}"
        logger.error(error)
        raise e

def process_file(filename):
    """
    Processes a single file: downloads, parses, and sends in batches.
    
    Args:
        filename: Name of the file to process
        
    Returns:
        Dict with processing results
    """
    try:
        logger.info(f"Processing file: {filename}")
        
        # Get CSV content
        csv_string = get_backup_csv_from_bucket(filename)
        
        # Convert to JSON
        json_data = csv_to_json(csv_string)
        
        # Batch the data
        batches = batch_data(json_data, batch_size=100)
        
        # Process each batch
        batch_results = []
        for i, batch in enumerate(batches):
            try:
                logger.info(f"Processing batch {i+1}/{len(batches)} for file {filename}")
                result = invoke_other_function(batch)
                batch_results.append(result)
            except Exception as e:
                logger.error(f"Error processing batch {i+1} for file {filename}: {e}")
                batch_results.append({"error": str(e)})
        
        return {
            "filename": filename,
            "total_records": len(json_data),
            "batches_processed": len(batch_results),
            "success": all("error" not in result for result in batch_results),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        return {
            "filename": filename,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@functions_framework.http
def handler(request):
    """
    HTTP Cloud Function entry point.
    
    Args:
        request: HTTP request object
        
    Returns:
        JSON response with processing results
    """
    # Get parameters from request
    request_json = request.get_json(silent=True) or {}
    request_args = request.args
    
    # Check if specific file was requested
    specific_file = None
    if request_json and 'name' in request_json:
        specific_file = request_json['name']
    elif request_args and 'name' in request_args:
        specific_file = request_args['name']
    
    # Check if max_workers was specified
    max_workers = 5  # Default value
    if request_json and 'max_workers' in request_json:
        try:
            max_workers = int(request_json['max_workers'])
        except ValueError:
            pass
    elif request_args and 'max_workers' in request_args:
        try:
            max_workers = int(request_args['max_workers'])
        except ValueError:
            pass
    
    try:
        # Get files to process
        if specific_file:
            files_to_process = [specific_file]
        else:
            files_to_process = get_all_files_in_bucket()
            logger.info(f"Found {len(files_to_process)} files to process")
            
        # Process files in parallel
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(process_file, filename): filename for filename in files_to_process}
            
            for future in concurrent.futures.as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Completed processing file: {filename}")
                except Exception as e:
                    logger.error(f"Exception processing file {filename}: {e}")
                    results.append({
                        "filename": filename,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
        
        # Return results
        return {
            "status": "completed",
            "files_processed": len(results),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        error = f"Error in handler: {e}"
        logger.error(error)
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, 500
        
        
# For local debugging
if __name__ == "__main__":
    import os
    import argparse
    
    class MockRequest:
        def __init__(self, json_data=None, args=None):
            self.json_data = json_data or {}
            self.args = args or {}
            
        def get_json(self, silent=False):
            return self.json_data
    
    # Parse command line arguments for flexibility
    parser = argparse.ArgumentParser(description='Process BTC price backup files')
    parser.add_argument('--file', help='Specific file to process (optional)')
    parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers (default: 3)')
    parser.add_argument('--prefix', default='btc_price_backup_', help='File prefix to filter by (default: btc_price_backup_)')
    parser.add_argument('--list-only', action='store_true', help='Only list files without processing them')
    args = parser.parse_args()
    
    # Set environment variables needed for GCP authentication if needed
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-key.json"
    
    # If list-only flag is set, just list the files and exit
    if args.list_only:
        print(f"Listing files with prefix '{args.prefix}'...")
        files = get_all_files_in_bucket(prefix=args.prefix)
        for i, file in enumerate(files):
            print(f"{i+1}. {file}")
        print(f"Found {len(files)} files")
        exit(0)
    
    # Create the request with command line arguments
    json_data = {"max_workers": args.workers}
    if args.file:
        json_data["name"] = args.file
        print(f"Processing specific file: {args.file}")
    else:
        print(f"Processing all files with prefix '{args.prefix}'...")
        
    # Execute the handler
    mock_request = MockRequest(json_data=json_data)
    result = handler(mock_request)
    
    # Print the results
    if isinstance(result, tuple):
        # Handle error case where result is (response, status_code)
        print(f"Error (Status {result[1]}):")
        print(json.dumps(result[0], indent=2))
    else:
        # Handle success case
        print("Success:")
        print(json.dumps(result, indent=2))
        
        # Print a summary
        if "results" in result:
            success_count = sum(1 for r in result["results"] if r.get("success", False))
            error_count = len(result["results"]) - success_count
            print(f"\nSummary: {len(result['results'])} files processed, {success_count} successful, {error_count} with errors")
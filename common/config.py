#!/usr/bin/env python
import os
from google.cloud import bigquery
from dotenv import load_dotenv
# Initialize Environment Variables
load_dotenv()


# Load environment variables
def load_env_vars():
    required_vars = ['BIGQUERY_DATASET', 'BIGQUERY_TABLE']
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} environment variable must be set")

# Initialize BigQuery client
bq_client = bigquery.Client()

# Get environment variables
DATASET = os.getenv('BIGQUERY_DATASET')
TABLE = os.getenv('BIGQUERY_TABLE')

load_env_vars()
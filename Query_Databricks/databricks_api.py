import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Get credentials from .env file
warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
token = os.getenv('DATABRICKS_TOKEN')
workspace_url = os.getenv('DATABRICKS_HOST')  # You'll need to add this to your .env file

# Ensure workspace_url starts with https://
if not workspace_url.startswith('https://'):
    workspace_url = f'https://{workspace_url}'

# Define the query
query = "SELECT * FROM kmt_sneha_dev.stores.equipment_sample_data"

# Set up the headers with the token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Construct the API endpoint
query_endpoint = f"{workspace_url}/api/2.0/sql/statements"

# Print execution info
print(f"Execution Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"User: {os.getenv('USER', 'snehadharma')}")
print(f"Querying endpoint: {query_endpoint}")

# Execute the query
try:
    response = requests.post(
        query_endpoint,
        headers=headers,
        json={
            "statement": query,
            "warehouse_id": warehouse_id
        }
    )
    
    # Check if the request was successful
    response.raise_for_status()
    
    # Get the results
    results = response.json()
    
    # Print the results in a readable format
    print("\nQuery Results:")
    print(json.dumps(results, indent=2))
    
except requests.exceptions.RequestException as e:
    print(f"\nError making request: {e}")
    print(f"Response status code: {response.status_code if 'response' in locals() else 'N/A'}")
    print(f"Response text: {response.text if 'response' in locals() else 'N/A'}")
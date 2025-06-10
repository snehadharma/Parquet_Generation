# USING DATABRICKS SQL CONNECTOR 

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv
from databricks import sql
import json
import requests
import uvicorn
import os
from datetime import datetime
import sys 
load_dotenv()


# Get the parent directory (project_root)
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add parent directory to Python path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Now you can import from Shared
from Shared.models import Equipment


# Constants
CURRENT_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
CURRENT_USER = "snehadharma"

app = FastAPI(
    title="Equipment JSON API",
    description="READ-only API for querying equipment data from JSON file",
    version="1.0.0"
)

def read_sql_query(filename: str) -> str:
    """Read SQL query from a file"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(current_dir, 'SQL_Commands', filename)
    
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()
    
def convert_databricks_results_to_equipment(results: dict) -> List[Equipment]:
    equipment_list = []
    
    # Get the data array from the results
    data_array = results['result']['data_array']
    
    for row in data_array:
        # Convert EquipmentType from string to dict
        equipment_type_dict = json.loads(row[6])  # Position 6 is EquipmentType
        
        # Create equipment dict
        equipment_dict = {
            'Active': row[0] == 'true',  # Convert string to boolean
            'BuiltDate': row[1],
            'DateCreated': row[2],
            'DateModified': row[3],
            'Deleted': row[4] == 'true',  # Convert string to boolean
            'EquipmentName': row[5],
            'EquipmentType': equipment_type_dict,
            'Id': int(row[7]),
            'OperatingCompany': row[8],
            'Owner': row[9],
            'ResourceNumber': row[10]
        }
        
        # Convert dict to Equipment object
        equipment = Equipment.model_validate(equipment_dict)
        equipment_list.append(equipment)
    
    return equipment_list

def load_equipment_data_queries_api():
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
        
        return convert_databricks_results_to_equipment(results=results)
        
    except requests.exceptions.RequestException as e:
        print(f"\nError making request: {e}")
        print(f"Response status code: {response.status_code if 'response' in locals() else 'N/A'}")
        print(f"Response text: {response.text if 'response' in locals() else 'N/A'}")

def load_equipment_data_sql_connector():
    # Get credentials from .env file
    server_hostname = os.getenv('DATABRICKS_HOST')  # Your Databricks workspace URL
    http_path = os.getenv('DATABRICKS_HTTP_PATH')  # SQL warehouse HTTP path
    access_token = os.getenv('DATABRICKS_TOKEN')  # Your personal access token

    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        print("Successfully connected to Databricks!")
        # Create a cursor
        cursor = connection.cursor()
        # Execute query
        query = read_sql_query('query_command.sql')
        cursor.execute(query)
        # Fetch and print results
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        equipment_list = [dict(zip(columns, row)) for row in result]
        print(equipment_list)
        return equipment_list 
    
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close cursor and connection
        try:
            cursor.close()
        except:
            pass
        
        try:
            connection.close()
        except:
            pass

def convert_to_equipment(temp: dict) -> List[Equipment]:
    equipment_data = [Equipment(**item) for item in temp]
    return equipment_data

# equipment_data = convert_to_equipment(load_equipment_data_sql_connector())
equipment_data = load_equipment_data_queries_api()

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint returning API status"""
    return {
        "message": "Equipment JSON API is running",
        "timestamp": CURRENT_TIME,
        "user": CURRENT_USER,
        "total_records": len(equipment_data)
    }

@app.get("/api/v1/equipment", response_model=List[Equipment])
async def get_all_equipment():
    """Get all equipment records"""
    try:
        return equipment_data
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving equipment data: {str(e)}"
        )

# Run the application
if __name__ == "__main__":
    # print("Starting FastAPI server...")
    
    # print("\nAvailable Endpoints:")
    # print("1. Root: http://localhost:8000/")
    # print("2. All Equipment: http://localhost:8000/api/v1/equipment")
    
    # # Run the server
    # uvicorn.run(app, host="0.0.0.0", port=8000)

    print(os.getenv('DATABRICKS_TOKEN'))

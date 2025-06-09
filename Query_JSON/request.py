# test_api.py
import requests
import pandas as pd
import os 

BASE_URL = "http://localhost:8000"

def test_api():
    # Test root endpoint
    response = requests.get(f"{BASE_URL}/")
    print("\nRoot endpoint test:")
    print(response.json())

    # Test all equipment endpoint
    response = requests.get(f"{BASE_URL}/api/v1/equipment")
    data = response.json()
    print(f"\nAll equipment endpoint test:")
    print(f"{data}\n")
    print(type(data))
    
    if data:
        # Test single equipment endpoint
        first_id = data[0].get("Id")
        response = requests.get(f"{BASE_URL}/api/v1/equipment/{first_id}")
        print(f"\nSingle equipment endpoint test (ID: {first_id}):")
        print(response.json())

    create_parquet_file(data)

def create_parquet_file(data):
    df = pd.DataFrame(data)
    current_dir = os.path.dirname(os.path.abspath(__file__)) + '\equipment_list.parquet'
    df.to_parquet(current_dir)

    print(df)

if __name__ == "__main__":
    test_api()
# test_api.py
import os
import requests
import pandas as pd

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

    create_parquet_file(data)

def create_parquet_file(data):
    df = pd.DataFrame(data)
    current_dir = os.path.dirname(os.path.abspath(__file__)) + '\equipment_list.parquet'
    df.to_parquet(current_dir)

    print(df)

if __name__ == "__main__":
    test_api()
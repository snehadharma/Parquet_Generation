from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
import uvicorn
import os
from datetime import datetime
import sys 

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
JSON_FILE_PATH = "./equipment_sample_data.json"  # Put this file in the same directory as your Python file

# Initialize FastAPI
app = FastAPI(
    title="Equipment JSON API",
    description="READ-only API for querying equipment data from JSON file",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load JSON Data
# @cache 
def load_equipment_data():
    try:
        with open(JSON_FILE_PATH, 'r') as file:
            return json.load(file)
    except Exception as e: 
        print(f"Error loading JSON file: {e}")
        return []

# Load the data
temp_equipment_data = load_equipment_data()
equipment_data = [Equipment(**item) for item in temp_equipment_data]

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

@app.get("/api/v1/equipment/{equipment_id}", response_model=Equipment)
async def get_equipment_by_id(equipment_id: int):
    """Get a specific equipment record by ID"""
    try:
        equipment = next(
            (item for item in equipment_data if item.Id == equipment_id),
            None
        )
        if equipment:
            return equipment
        raise HTTPException(
            status_code=404,
            detail=f"Equipment with ID {equipment_id} not found"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving equipment: {str(e)}"
        )

# Run the application
if __name__ == "__main__":
    print("Starting FastAPI server...")
    
    print("\nAvailable Endpoints:")
    print("1. Root: http://localhost:8000/")
    print("2. All Equipment: http://localhost:8000/api/v1/equipment")
    print("3. Equipment by ID: http://localhost:8000/api/v1/equipment/{id}")
    
    # Run the server
    uvicorn.run(app, host="0.0.0.0", port=8000)
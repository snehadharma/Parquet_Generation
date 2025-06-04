from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
import uvicorn
from datetime import datetime

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

# Data Models
class EquipmentType(BaseModel):
    Caption: str
    DateCreated: str
    DateModified: str
    Deleted: bool
    Id: int
    Index: int
    Value: str

class Metadata(BaseModel):
    file_path: str
    file_name: str
    file_size: str
    file_modification_time: str

class Equipment(BaseModel):
    Active: bool
    BuiltDate: str
    DateCreated: str
    DateModified: str
    Deleted: bool
    EquipmentName: str
    EquipmentType: EquipmentType
    Id: int
    OperatingCompany: str
    Owner: str
    ResourceNumber: str
    _metadata: Metadata

class EquipmentResponse(BaseModel):
    timestamp: str
    user: str
    total_records: int
    data: List[Equipment]

# Load JSON Data
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
            (item for item in equipment_data if item["Id"] == equipment_id),
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
    print(f"Current Date and Time (UTC): {CURRENT_TIME}")
    print(f"Current User's Login: {CURRENT_USER}")
    print("-" * 80)
    
    print("Starting FastAPI server...")
    
    # Print API information
    print("\nAPI Information:")
    print(f"- Server: http://localhost:8000")
    print(f"- Swagger UI: http://localhost:8000/docs")
    print(f"- ReDoc: http://localhost:8000/redoc")
    
    print("\nAvailable Endpoints:")
    print("1. Root: /")
    print("2. All Equipment: /api/v1/equipment")
    print("3. Equipment by ID: /api/v1/equipment/{id}")
    
    # Run the server
    uvicorn.run(app, host="0.0.0.0", port=8000)
from pydantic import BaseModel

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
    # def __init__(self): 
    #     pass
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
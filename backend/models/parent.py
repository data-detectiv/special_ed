from pydantic import BaseModel

class ParentCreate(BaseModel):
    name: str
    phone_number: str
    email: str
    address: str

class ParentUpdate(BaseModel):
    parent_id: str
    name: str
    phone_number: str
    email: str 
    address: str 
from pydantic import BaseModel

class ParentUpdate(BaseModel):
    parent_id: str
    name: str
    phone_number: str
    email: str 
    address: str 
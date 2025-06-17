from pydantic import BaseModel
from datetime import date 


class StudentUpdate(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    date_of_birth: date 
    gender: str 
    address: str 
    parent_id: str 
    teacher_id: str

class StudentCreate(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    date_of_birth: date 
    gender: str 
    address: str 
    parent_id: str 
    teacher_id: str
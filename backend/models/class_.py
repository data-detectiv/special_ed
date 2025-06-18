from pydantic import BaseModel

class ClassCreate(BaseModel):
    class_id: str
    class_name: str
    grade_level: str
    teacher_id: str
    room_number: str
    schedule: str

class ClassUpdate(BaseModel):
    class_id: str
    class_name: str
    grade_level: str
    teacher_id: str
    room_number: str
    schedule: str
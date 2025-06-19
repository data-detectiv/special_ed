from pydantic import BaseModel

class TeacherCreate(BaseModel):
    name: str
    email: str
    phone_number: str
    class_id: str

class TeacherUpdate(BaseModel):
    teacher_id: str
    name: str
    email: str
    phone_number: str
    class_id: str

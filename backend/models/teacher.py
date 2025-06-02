from pydantic import BaseModel

class TeacherUpdate(BaseModel):
    teacher_id: str
    name: str
    email: str
    phone_number: str
    class_id: str

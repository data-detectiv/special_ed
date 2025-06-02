from pydantic import BaseModel

class ClassUpdate(BaseModel):
    class_id: str
    class_name: str
    grade_level: str
    teacher_id: str
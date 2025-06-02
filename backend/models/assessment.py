from pydantic import BaseModel
from datetime import date

class AssessmentUpdate(BaseModel):
    assessment_id: str
    student_id: str
    assessment_name: str
    assessment_date: date
    assessment_score: float
    assessment_notes: str
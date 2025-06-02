from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


from routes import student, parent, teacher, assessment, class_

app.include_router(student.router)
app.include_router(parent.router)
app.include_router(teacher.router)
app.include_router(assessment.router)
app.include_router(class_.router)

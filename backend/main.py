from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# Import settings to ensure configuration is loaded
from config import settings

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from routes import student, parent, teacher, assessment, class_

app.include_router(student.router)
app.include_router(parent.router)
app.include_router(teacher.router)
app.include_router(assessment.router)
app.include_router(class_.router)

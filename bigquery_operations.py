import os 
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.responses import JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import date
load_dotenv()

app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[""],
#     allow_credentials=True,
#     allow_methods=[""],
#     allow_headers=[""]
# )


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
client = bigquery.Client()

# referencing and getting the table
def get_table(dataset_name, table_name):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_name)
    tabel_ref = bigquery.TableReference(dataset_ref, table_name)
    main_table = client.get_table(tabel_ref)
    return main_table

class ParentItem(BaseModel):
    parent_id: str
    name: str
    phone_number: str
    email: str
    address: str

class StudentItem(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    date_of_birth: date
    gender: str 
    address: str
    parent_id: str
    teacher_id: str

class TeacherItem(BaseModel):
    teacher_id: str
    name: str
    email: str
    phone_number: str
    class_id: str

class AssessmentItem(BaseModel):
    assessment_id: str 
    student_id: str
    assessment_name: str
    assessment_date: date
    assessment_score: float
    assessment_notes: str

class ClassItem(BaseModel):
    class_id: str
    class_name: str
    grade_level: str 
    teacher_id: str

# load data into bigquery
@app.post("/load-parent")
async def load_parent_data(item: ParentItem):
    parent_table = get_table('groups', 'parent')
    query = f"""
        SELECT parent_id FROM `{parent_table.project}.{parent_table.dataset_id}.{parent_table.table_id}`
        WHERE parent_id = @parent_id
        LIMIT 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("parent_id","STRING", item.parent_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = list(query_job)

    if results:
        return {"status": "error", "message": f"Parent with ID '{item.parent_id}' already exists."}
    rows = [item.dict()]
    errors = client.insert_rows_json(parent_table, rows)
    if errors ==  []:
        return JSONResponse(content={"message": "Parent data loaded successfully"})
    return JSONResponse(content={"error": errors}, status_code=400)

@app.post("/load-student")
async def load_student_data(item: StudentItem):
    student_table = get_table('groups', 'student')
    query = f"""
        SELECT student_id FROM `{student_table.project}.{student_table.dataset_id}.{student_table.table_id}`
        WHERE student_id = @student_id
        LIMIT 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameter = [
            bigquery.ScalarQueryParameter("student_id", "STRING", item.student_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = list(query_job)
    if results:
        return {"status": "error", "message": f"Student with ID `{item.student_id}` already exists."}
    rows = [item.dict()]
    errors = client.insert_rows_json(student_table, rows)
    if errors == []:
        return JSONResponse(content={"message": "Student data loaded successfully"})

@app.post("/load-teacher")
async def load_teacher_data(item: TeacherItem):
    teacher_table = get_table('groups', 'teacher')
    query = f"""
        SELECT teacher_id FROM `{teacher_table.project}.{teacher_table.dataset_id}.{teacher_table.table_id}`
        WHERE teacher_id = @teacher_id
        LIMIT 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameter = [
            bigquery.ScalarQueryParameter("teacher_id", "STRING", item.teacher_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = list(query_job)
    if results:
        return {"status": "error", "message": f"Teacher with ID `{item.teacher_id}` already exists."}
    rows = [item.dict()]
    errors = client.insert_rows_json(teacher_table, rows)
    if errors == []:
        return JSONResponse(content={"message": "Teacher data loaded successfully"})

@app.post("/load-assessment")
async def load_assessment_data(item: AssessmentItem):
    assessment_table = get_table('assessment', 'assessment')
    rows = [item.dict()]
    errors = client.insert_rows_json(assessment_table, rows)
    if errors == []:
        return JSONResponse(content={"message": "Assessment data loaded successfully"})
    
@app.post("/load-class")
async def load_class_data(item: ClassItem):
    class_table = get_table('buildings', 'class')
    rows = [item.dict()]
    errors = client.insert_rows_json(class_table, rows)
    if errors == []:
        return JSONResponse(content={"message": "Class data loaded successfully"})

# fetch data from bigquery
@app.get("/fetch/{table_name}")
async def fetch_data(table_name:str):
    valid_tables = {
        "parent": get_table('groups', 'parent'),
        "student": get_table('groups', 'student'),
        "teacher": get_table('groups', 'teacher'),
        "assessment": get_table('assessment', 'assessment'),
        "class": get_table('buildings', 'class')
    }

    if table_name not in valid_tables:
        return {"error": f"Table '{table_name}' not found. Valid options: {list(valid_tables.keys())}"}

    table = valid_tables[table_name]
    sql_query = f"""
        SELECT * FROM `{table.project}.{table.dataset_id}.{table.table_id}`;
    """
    query_job = client.query(sql_query)
    results = query_job.result()

    data = [dict(row.items()) for row in results]


    return {f"{table_name}s": data}

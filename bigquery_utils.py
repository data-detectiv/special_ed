import os 
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from typing import Optional
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import date 
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import pandas as pd
from io import StringIO, BytesIO
load_dotenv()

app = FastAPI()



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
client = bigquery.Client()

class StudentUpdate(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    date_of_birth: date 
    gender: str 
    address: str 
    parent_id: str 
    teacher_id: str

class ParentUpdate(BaseModel):
    parent_id: str
    name: str
    phone_number: str
    email: str 
    address: str 

class TeacherUpdate(BaseModel):
    teacher_id: str
    name: str
    email: str
    phone_number: str
    class_id: str

class AssessmentUpdate(BaseModel):
    assessment_id: str
    student_id: str
    assessment_name: str
    assessment_date: date
    assessment_score: float
    assessment_notes: str

  

class ClassUpdate(BaseModel):
    class_id: str
    class_name: str
    grade_level: str
    teacher_id: str


# referencing and getting the table
def get_table(dataset_name, table_name):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_name)
    tabel_ref = bigquery.TableReference(dataset_ref, table_name)
    return tabel_ref


def upload_data_to_bigquery(df, table_ref, key_column):
    from google.cloud import bigquery

    # Create a temporary table name
    temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_{table_ref.table_id}"

    # Get schema of the target table
    table = client.get_table(table_ref)
    existing_columns = [field.name for field in table.schema]
    df = df[[col for col in df.columns if col in existing_columns]]

    # Ensure date format is correct
    if "date_of_birth" in df.columns:
        df.loc[:, "date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce").dt.date
    
    if "assessment_date" in df.columns:
        df["assessment_date"] = pd.to_datetime(df["assessment_date"], errors="coerce").dt.date
    # Ensure phone number is converted to string
    if "phone_number" in df.columns:
        df["phone_number"] = df["phone_number"].astype(str)
    
    if "grade_level" in df.columns:
        df["grade_level"] = df["grade_level"].astype(str)

    # Upload the new data to the temporary table
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

    # Build MERGE query from temp to main table
    column_list = ", ".join(existing_columns)
    insert_values = ", ".join([f"S.{col}" for col in existing_columns])
    update_clause = ", ".join([f"{col} = S.{col}" for col in existing_columns if col != key_column])

    merge_query = f"""
        MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
        USING `{temp_table_id}` S
        ON T.{key_column} = S.{key_column}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list}) VALUES ({insert_values})
    """

    query_job = client.query(merge_query)
    query_job.result()

    # Optional: delete the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

def fetch_data_from_bigquery(table_ref):
    query = f"""
        SELECT * FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
"""
    query_job = client.query(query)
    results = query_job.result()
    data = [dict(row.items()) for row in results]
    return data

def delete_data_from_bigquery(table_ref, key_column, key_value):
    query = f"""
        DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
        WHERE {key_column} = @value
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("value", "STRING", key_value)
        ]
    )
    print(f"Student with id {key_column} deleted")
    return client.query(query, job_config=job_config).result()

def update_data_in_bigquery():
    query = f"""

"""


# Student
@app.post("/upload-student")
async def upload_data(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            return {"error": "Unsupported file type"}
        
        table_ref = get_table("groups", "student")
        
        upload_data_to_bigquery(df, table_ref, "student_id")
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}

# Parent
@app.post("/upload-parent")
async def upload_data(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            return {"error": "Unsupported file type"}
        
        table_ref = get_table("groups", "parent")
        
        upload_data_to_bigquery(df, table_ref, "parent_id")
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}

# Teacher
@app.post("/upload-teacher")
async def upload_data(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            return {"error": "Unsupported file type"}
        
        table_ref = get_table("groups", "teacher")
        
        upload_data_to_bigquery(df, table_ref, "teacher_id")
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}
    
# Assessment
@app.post("/upload-assessment")
async def upload_data(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            return {"error": "Unsupported file type"}
        
        table_ref = get_table("assessment", "assessment")
        
        upload_data_to_bigquery(df, table_ref, "assessment_id")
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}
    
# Class
@app.post("/upload-class")
async def upload_data(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            return {"error": "Unsupported file type"}
        
        table_ref = get_table("buildings", "class")
        
        upload_data_to_bigquery(df, table_ref, "class_id")
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}

# Get student data
@app.get("/get-student")
async def get_data():
    table_ref = get_table("groups", "student")
    return fetch_data_from_bigquery(table_ref)

# Get parent data
@app.get("/get-parent")
async def get_data():
    table_ref = get_table("groups", "parent")
    return fetch_data_from_bigquery(table_ref)

# Get teacher data
@app.get("/get-teacher")
async def get_data():
    table_ref = get_table("groups", "teacher")
    return fetch_data_from_bigquery(table_ref)

# Get assessment data
@app.get("/get-assessment")
async def get_data():
    table_ref = get_table("assessment", "assessment")
    return fetch_data_from_bigquery(table_ref)

# Get class data
@app.get("/get-class")
async def get_data():
    table_ref = get_table("buildings", "class")
    return fetch_data_from_bigquery(table_ref)

# Delete parent data
@app.delete("/delete-parent/{parent_id}")
async def delete_parent(parent_id: str):
    table_ref = get_table("groups", "parent")
    return delete_data_from_bigquery(table_ref,"parent_id", parent_id)

# Delete student data
@app.delete("/delete-student/{student_id}")
async def delete_student(student_id: str):
    table_ref = get_table("groups", "student")
    return delete_data_from_bigquery(table_ref,"student_id", student_id)

# Delete teacher data
@app.delete("/delete-teacher/{teacher_id}")
async def delete_teacher(teacher_id: str):
    table_ref = get_table("groups", "teacher")
    return delete_data_from_bigquery(table_ref,"teacher_id", teacher_id)

# student update
@app.put("/update-student")
async def update_student(students: list[StudentUpdate]):
    table_ref = get_table("groups", "student")
    try:
        queries = []
        for student in students:
            query = f"""
                UPDATE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
                SET
                    student_id = '{student.student_id}',
                    first_name = '{student.first_name}',
                    last_name = '{student.last_name}',
                    date_of_birth = '{student.date_of_birth}',
                    gender = '{student.gender}',
                    address = '{student.address}',
                    parent_id = '{student.parent_id}',
                    teacher_id = '{student.teacher_id}'
                WHERE
                    student_id = '{student.student_id}'
"""
            queries.append(query)

        query_job = client.query(";\n".join(queries))
        query_job.result()
        return {"message": f"Updated {len(students)} student successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

# parent update
@app.put("/update-parent")
async def update_parent(parents: list[ParentUpdate]):
    table_ref = get_table("groups", "parent")
    try:
        queries = []
        for parent in parents:
            query = f"""
                UPDATE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
                SET
                    parent_id = '{parent.parent_id}',
                    name = '{parent.name}',
                    phone_number = '{parent.phone_number}',
                    email = '{parent.email}',
                    address = '{parent.address}'
                WHERE
                    parent_id = '{parent.parent_id}'
"""
            queries.append(query)

        query_job = client.query(";\n".join(queries))
        query_job.result()
        return {"message": f"Updated {len(parents)} parent successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

# teacher update
@app.put("/update-teacher")
async def update_teacher(teachers: list[TeacherUpdate]):
    table_ref = get_table("groups", "teacher")
    try:
        queries = []
        for teacher in teachers:
            query = f"""
                UPDATE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
                SET
                    teacher_id = '{teacher.teacher_id}',
                    name = '{teacher.name}',
                    email = '{teacher.email}',
                    phone_number = '{teacher.phone_number}',
                    class_id = '{teacher.class_id}'
                WHERE
                    teacher_id = '{teacher.teacher_id}'
"""
            queries.append(query)

        query_job = client.query(";\n".join(queries))
        query_job.result()
        return {"message": f"Updated {len(teachers)} teacher successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

# assessment update
@app.put("/update-assessment")
async def update_assessment(assessments: list[AssessmentUpdate]):
    table_ref = get_table("assessment", "assessment")
    try:
        queries = []
        for assessment in assessments:
            query = f"""
                UPDATE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
                SET
                    assessment_id = '{assessment.assessment_id}',
                    student_id = '{assessment.student_id}',
                    assessment_name = '{assessment.assessment_name}',
                    assessment_date = '{assessment.assessment_date}',
                    assessment_score = {assessment.assessment_score},
                    assessment_notes = '{assessment.assessment_notes}'
                WHERE
                    assessment_id = '{assessment.assessment_id}'
"""
            queries.append(query)

        query_job = client.query(";\n".join(queries))
        query_job.result()
        return {"message": f"Updated {len(assessments)} assessments successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

# class update
@app.put("/update-class")
async def update_class(classes: list[ClassUpdate]):
    table_ref = get_table("buildings", "class")
    try:
        queries = []
        for class_item in classes:
            query = f"""
                UPDATE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
                SET
                    class_id = '{class_item.class_id}',
                    class_name = '{class_item.class_name}',
                    grade_level = '{class_item.grade_level}',
                    teacher_id = '{class_item.teacher_id}'
                WHERE
                    class_id = '{class_item.class_id}'
"""
            queries.append(query)

        query_job = client.query(";\n".join(queries))
        query_job.result()
        return {"message": f"Updated {len(classes)} classes successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

# Delete assessment data
@app.delete("/delete-assessment/{assessment_id}")
async def delete_assessment(assessment_id: str):
    table_ref = get_table("assessment", "assessment")
    return delete_data_from_bigquery(table_ref,"assessment_id", assessment_id)

# Delete class data
@app.delete("/delete-class/{class_id}")
async def delete_class(class_id: str):
    table_ref = get_table("buildings", "class")
    return delete_data_from_bigquery(table_ref,"class_id", class_id)
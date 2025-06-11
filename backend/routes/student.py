from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.student import StudentUpdate
import pandas as pd
from io import StringIO, BytesIO

router = APIRouter()

@router.get("/get-student")
async def get_data():
    table_ref = get_table("groups", "student")
    return fetch_data_from_bigquery(table_ref)

@router.post("/upload-student")
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

@router.put("/update-student")
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

@router.delete("/delete-student/{student_id}")
async def delete_student(student_id: str):
    table_ref = get_table("groups", "student")
    return delete_data_from_bigquery(table_ref,"student_id", student_id)
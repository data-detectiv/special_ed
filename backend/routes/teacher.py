from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.teacher import TeacherUpdate
import pandas as pd
from io import StringIO, BytesIO


router = APIRouter()

@router.get("/get-teacher")
async def get_data():
    table_ref = get_table("groups", "teacher")
    return fetch_data_from_bigquery(table_ref)

@router.post("/upload-teacher")
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

router.put("/update-teacher")
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

@router.delete("/delete-teacher/{teacher_id}")
async def delete_teacher(teacher_id: str):
    table_ref = get_table("groups", "teacher")
    return delete_data_from_bigquery(table_ref,"teacher_id", teacher_id)
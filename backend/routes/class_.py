from fastapi import APIRouter, UploadFile, File, HTTPException
from backend.services.bigquery_service import *
from backend.models.class_ import ClassUpdate
import pandas as pd
from io import StringIO, BytesIO

router = APIRouter()

@router.get("/get-class")
async def get_data():
    table_ref = get_table("buildings", "class")
    return fetch_data_from_bigquery(table_ref)

@router.post("/upload-class")
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

@router.put("/update-class")
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


@router.delete("/delete-class/{class_id}")
async def delete_class(class_id: str):
    table_ref = get_table("buildings", "class")
    return delete_data_from_bigquery(table_ref,"class_id", class_id)
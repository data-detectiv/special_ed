from fastapi import APIRouter, UploadFile, File, HTTPException
from backend.services.bigquery_service import *
from backend.models.parent import ParentUpdate
import pandas as pd
from io import StringIO, BytesIO

router = APIRouter()

@router.get("/get-parent")
async def get_data():
    table_ref = get_table("groups", "parent")
    return fetch_data_from_bigquery(table_ref)

@router.post("/upload-parent")
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

@router.put("/update-parent")
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

@router.delete("/delete-parent/{parent_id}")
async def delete_parent(parent_id: str):
    table_ref = get_table("groups", "parent")
    return delete_data_from_bigquery(table_ref,"parent_id", parent_id)
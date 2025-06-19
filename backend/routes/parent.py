from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.parent import ParentUpdate, ParentCreate
import pandas as pd
from io import StringIO, BytesIO
import re

router = APIRouter()

def get_next_parent_id():
    """Generate the next sequential parent ID (P001, P002, etc.)"""
    table_ref = get_table("groups", "parent")
    try:
        query = f"""
            SELECT parent_id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
            WHERE parent_id LIKE 'P%'
            ORDER BY parent_id DESC
            LIMIT 1
        """
        query_job = client.query(query)
        results = query_job.result()
        if results.total_rows == 0:
            return "P001"
        latest_id = list(results)[0]['parent_id']
        match = re.match(r'P(\d+)', latest_id)
        if match:
            next_num = int(match.group(1)) + 1
            return f"P{next_num:03d}"
        else:
            return "P001"
    except Exception as e:
        print(f"Error generating next parent ID: {e}")
        return "P001"

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

@router.post("/add-parent")
async def add_parents(parents: list[dict]):
    """
    Add multiple new parents from the grid interface.
    Expects a list of dictionaries with parent data (without parent_id).
    """
    table_ref = get_table("groups", "parent")
    try:
        print(f"Received {len(parents)} parents to add:")
        rows_to_insert = []
        for parent_data in parents:
            new_id = get_next_parent_id()
            print(f"Generated ID: {new_id} for parent: {parent_data.get('name', 'Unknown')}")
            row_to_insert = {
                "parent_id": new_id,
                "name": parent_data.get("name", ""),
                "phone_number": parent_data.get("phone_number", ""),
                "email": parent_data.get("email", ""),
                "address": parent_data.get("address", "")
            }
            rows_to_insert.append(row_to_insert)
            print(f"Row to insert: {row_to_insert}")
        print(f"Inserting {len(rows_to_insert)} rows into BigQuery...")
        errors = client.insert_rows_json(
            f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}",
            rows_to_insert
        )
        if errors:
            print(f"BigQuery errors: {errors}")
            raise HTTPException(status_code=400, detail=str(errors))
        print(f"Successfully added {len(parents)} parents")
        return {"message": f"Added {len(parents)} parent(s) successfully"}
    except Exception as e:
        print(f"Error in add_parents: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-parent")
async def update_parent(parents: list[ParentUpdate]):
    table_ref = get_table("groups", "parent")
    try:
        # Debug logging
        print(f"Received {len(parents)} parents to update:")
        for i, parent in enumerate(parents):
            print(f"Parent {i+1}: {parent}")
        
        # Use temporary table approach to avoid streaming buffer issues
        temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_update_{table_ref.table_id}"
        
        # Create temporary table with updated data
        temp_data = []
        for parent in parents:
            temp_data.append({
                "parent_id": parent.parent_id,
                "name": parent.name,
                "phone_number": parent.phone_number,
                "email": parent.email,
                "address": parent.address
            })
        
        # Upload to temporary table
        from google.cloud import bigquery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        
        temp_df = pd.DataFrame(temp_data)
        client.load_table_from_dataframe(temp_df, temp_table_id, job_config=job_config).result()
        print(f"Temporary table created: {temp_table_id}")
        
        # Use MERGE from temp table to main table
        for parent in parents:
            merge_query = f"""
                MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
                USING `{temp_table_id}` S
                ON T.parent_id = S.parent_id
                WHEN MATCHED THEN
                    UPDATE SET
                        name = S.name,
                        phone_number = S.phone_number,
                        email = S.email,
                        address = S.address
            """
            print(f"Executing MERGE query for parent {parent.parent_id}")
            query_job = client.query(merge_query)
            query_job.result()
            print(f"MERGE completed for parent {parent.parent_id}")

        # Clean up temporary table
        client.delete_table(temp_table_id, not_found_ok=True)
        print("Temporary table deleted")

        print("All update operations completed successfully")
        return {"message": f"Updated {len(parents)} parent successfully"}
    except Exception as e:
        print(f"Error in update_parent: {e}")
        # Clean up temporary table on error
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.delete("/delete-parent/{parent_id}")
async def delete_parent(parent_id: str):
    table_ref = get_table("groups", "parent")
    return delete_data_from_bigquery(table_ref,"parent_id", parent_id)
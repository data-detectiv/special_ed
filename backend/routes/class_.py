from fastapi import APIRouter, HTTPException
from typing import List, Optional
import pandas as pd
from google.cloud import bigquery
from services.bigquery_service import get_table, get_bigquery_client
from models.class_ import ClassCreate, ClassUpdate
import re

router = APIRouter()

def get_next_class_id():
    """Generate the next sequential class ID (C001, C002, etc.)"""
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        query = f"""
            SELECT class_id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
            WHERE class_id LIKE 'C%'
            ORDER BY class_id DESC
            LIMIT 1
        """
        query_job = client.query(query)
        results = query_job.result()
        if results.total_rows == 0:
            return "C001"
        latest_id = list(results)[0]['class_id']
        match = re.match(r'C(\d+)', latest_id)
        if match:
            next_num = int(match.group(1)) + 1
            return f"C{next_num:03d}"
        else:
            return "C001"
    except Exception as e:
        print(f"Error generating next class ID: {e}")
        return "C001"
    finally:
        client.close()

@router.post("/create-class")
async def create_class(classes: List[ClassCreate]):
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        data = []
        for class_item in classes:
            data.append({
                "class_id": class_item.class_id,
                "class_name": class_item.class_name,
                "grade_level": class_item.grade_level,
                "teacher_id": class_item.teacher_id,
                "room_number": class_item.room_number,
                "schedule": class_item.schedule
            })
        
        df = pd.DataFrame(data)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        return {"message": f"Created {len(classes)} class successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.get("/get-classes")
async def get_classes():
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        query = f"SELECT * FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`"
        df = client.query(query).to_dataframe()
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.put("/update-class")
async def update_class(classes: list[ClassUpdate]):
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        # Debug logging
        print(f"Received {len(classes)} classes to update:")
        for i, class_item in enumerate(classes):
            print(f"Class {i+1}: {class_item}")
        
        # Use temporary table approach to avoid streaming buffer issues
        temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_update_{table_ref.table_id}"
        
        # Create temporary table with updated data
        temp_data = []
        for class_item in classes:
            temp_data.append({
                "class_id": class_item.class_id,
                "class_name": class_item.class_name,
                "grade_level": class_item.grade_level,
                "teacher_id": class_item.teacher_id,
                "room_number": class_item.room_number,
                "schedule": class_item.schedule
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
        for class_item in classes:
            merge_query = f"""
                MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
                USING `{temp_table_id}` S
                ON T.class_id = S.class_id
                WHEN MATCHED THEN
                    UPDATE SET
                        class_name = S.class_name,
                        grade_level = S.grade_level,
                        teacher_id = S.teacher_id,
                        room_number = S.room_number,
                        schedule = S.schedule
            """
            print(f"Executing MERGE query for class {class_item.class_id}")
            query_job = client.query(merge_query)
            query_job.result()
            print(f"MERGE completed for class {class_item.class_id}")

        # Clean up temporary table
        client.delete_table(temp_table_id, not_found_ok=True)
        print("Temporary table deleted")

        print("All update operations completed successfully")
        return {"message": f"Updated {len(classes)} class successfully"}
    except Exception as e:
        print(f"Error in update_class: {e}")
        # Clean up temporary table on error
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.delete("/delete-class/{class_id}")
async def delete_class(class_id: str):
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        query = f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE class_id = '{class_id}'"
        query_job = client.query(query)
        query_job.result()
        return {"message": f"Deleted class {class_id} successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.post("/add-class")
async def add_classes(classes: list[dict]):
    """
    Add multiple new classes from the grid interface.
    Expects a list of dictionaries with class data (without class_id).
    """
    table_ref = get_table("groups", "class")
    client = get_bigquery_client()
    try:
        print(f"Received {len(classes)} classes to add:")
        rows_to_insert = []
        for class_data in classes:
            new_id = get_next_class_id()
            print(f"Generated ID: {new_id} for class: {class_data.get('class_name', 'Unknown')}")
            row_to_insert = {
                "class_id": new_id,
                "class_name": class_data.get("class_name", ""),
                "grade_level": class_data.get("grade_level", ""),
                "teacher_id": class_data.get("teacher_id", ""),
                "room_number": class_data.get("room_number", ""),
                "schedule": class_data.get("schedule", "")
            }
            rows_to_insert.append(row_to_insert)
            print(f"Row to insert: {row_to_insert}")
        print(f"Inserting {len(rows_to_insert)} rows into BigQuery...")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        df = pd.DataFrame(rows_to_insert)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Successfully added {len(classes)} classes")
        return {"message": f"Added {len(classes)} class(es) successfully"}
    except Exception as e:
        print(f"Error in add_classes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close() 
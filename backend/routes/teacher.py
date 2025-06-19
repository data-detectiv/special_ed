from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.teacher import TeacherUpdate, TeacherCreate
import pandas as pd
from io import StringIO, BytesIO
import re


router = APIRouter()

def get_next_teacher_id():
    """Generate the next sequential teacher ID (T001, T002, etc.)"""
    table_ref = get_table("groups", "teacher")
    try:
        query = f"""
            SELECT teacher_id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
            WHERE teacher_id LIKE 'T%'
            ORDER BY teacher_id DESC
            LIMIT 1
        """
        query_job = client.query(query)
        results = query_job.result()
        if results.total_rows == 0:
            return "T001"
        latest_id = list(results)[0]['teacher_id']
        match = re.match(r'T(\d+)', latest_id)
        if match:
            next_num = int(match.group(1)) + 1
            return f"T{next_num:03d}"
        else:
            return "T001"
    except Exception as e:
        print(f"Error generating next teacher ID: {e}")
        return "T001"

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

@router.post("/add-teacher")
async def add_teachers(teachers: list[dict]):
    """
    Add multiple new teachers from the grid interface.
    Expects a list of dictionaries with teacher data (without teacher_id).
    """
    table_ref = get_table("groups", "teacher")
    try:
        print(f"Received {len(teachers)} teachers to add:")
        rows_to_insert = []
        for teacher_data in teachers:
            new_id = get_next_teacher_id()
            print(f"Generated ID: {new_id} for teacher: {teacher_data.get('name', 'Unknown')}")
            row_to_insert = {
                "teacher_id": new_id,
                "name": teacher_data.get("name", ""),
                "email": teacher_data.get("email", ""),
                "phone_number": teacher_data.get("phone_number", ""),
                "class_id": teacher_data.get("class_id", "")
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
        print(f"Successfully added {len(teachers)} teachers")
        return {"message": f"Added {len(teachers)} teacher(s) successfully"}
    except Exception as e:
        print(f"Error in add_teachers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-teacher")
async def update_teacher(teachers: list[TeacherUpdate]):
    table_ref = get_table("groups", "teacher")
    try:
        # Debug logging
        print(f"Received {len(teachers)} teachers to update:")
        for i, teacher in enumerate(teachers):
            print(f"Teacher {i+1}: {teacher}")
        
        # Use temporary table approach to avoid streaming buffer issues
        temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_update_{table_ref.table_id}"
        
        # Create temporary table with updated data
        temp_data = []
        for teacher in teachers:
            temp_data.append({
                "teacher_id": teacher.teacher_id,
                "name": teacher.name,
                "email": teacher.email,
                "phone_number": teacher.phone_number,
                "class_id": teacher.class_id
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
        for teacher in teachers:
            merge_query = f"""
                MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
                USING `{temp_table_id}` S
                ON T.teacher_id = S.teacher_id
                WHEN MATCHED THEN
                    UPDATE SET
                        name = S.name,
                        email = S.email,
                        phone_number = S.phone_number,
                        class_id = S.class_id
            """
            print(f"Executing MERGE query for teacher {teacher.teacher_id}")
            query_job = client.query(merge_query)
            query_job.result()
            print(f"MERGE completed for teacher {teacher.teacher_id}")

        # Clean up temporary table
        client.delete_table(temp_table_id, not_found_ok=True)
        print("Temporary table deleted")

        print("All update operations completed successfully")
        return {"message": f"Updated {len(teachers)} teacher successfully"}
    except Exception as e:
        print(f"Error in update_teacher: {e}")
        # Clean up temporary table on error
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.delete("/delete-teacher/{teacher_id}")
async def delete_teacher(teacher_id: str):
    table_ref = get_table("groups", "teacher")
    return delete_data_from_bigquery(table_ref,"teacher_id", teacher_id)
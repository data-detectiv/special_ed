from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.student import StudentUpdate, StudentCreate
import pandas as pd
from io import StringIO, BytesIO
from uuid import uuid4
from typing import List, Dict, Any
import re
from google.cloud import bigquery


router = APIRouter()

def get_next_student_id():
    """Generate the next sequential student ID (S001, S002, etc.)"""
    table_ref = get_table("groups", "student")
    try:
        # Get all existing student IDs
        query = f"""
            SELECT student_id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
            WHERE student_id LIKE 'S%'
            ORDER BY student_id DESC
            LIMIT 1
        """
        query_job = client.query(query)
        results = query_job.result()
        
        if results.total_rows == 0:
            return "S001"
        
        # Get the highest existing ID
        latest_id = list(results)[0]['student_id']
        
        # Extract the number part and increment
        match = re.match(r'S(\d+)', latest_id)
        if match:
            next_num = int(match.group(1)) + 1
            return f"S{next_num:03d}"
        else:
            return "S001"
            
    except Exception as e:
        print(f"Error generating next ID: {e}")
        return "S001"

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

@router.post("/add-student")
async def add_students(students: List[Dict[str, Any]]):
    """
    Add multiple new students from the grid interface.
    Expects a list of dictionaries with student data (without student_id).
    """
    table_ref = get_table("groups", "student")
    try:
        # Debug logging
        print(f"Received {len(students)} students to add:")
        for i, student in enumerate(students):
            print(f"Student {i+1}: {student}")
        
        rows_to_insert = []
        for student_data in students:
            # Generate new sequential ID for each student
            new_id = get_next_student_id()
            print(f"Generated ID: {new_id} for student: {student_data.get('first_name', 'Unknown')}")
            
            # Create row with all required fields
            row_to_insert = {
                "student_id": new_id,
                "first_name": student_data.get("first_name", ""),
                "last_name": student_data.get("last_name", ""),
                "date_of_birth": student_data.get("date_of_birth", ""),
                "gender": student_data.get("gender", ""),
                "address": student_data.get("address", ""),
                "parent_id": student_data.get("parent_id", ""),
                "teacher_id": student_data.get("teacher_id", "")
            }
            rows_to_insert.append(row_to_insert)
            print(f"Row to insert: {row_to_insert}")

        # Insert all rows at once
        print(f"Inserting {len(rows_to_insert)} rows into BigQuery...")
        errors = client.insert_rows_json(
            f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}", 
            rows_to_insert
        )
        
        if errors:
            print(f"BigQuery errors: {errors}")
            raise HTTPException(status_code=400, detail=str(errors))
        
        print(f"Successfully added {len(students)} students")
        return {"message": f"Added {len(students)} student(s) successfully"}
    except Exception as e:
        print(f"Error in add_students: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-student")
async def update_student(students: list[StudentUpdate]):
    table_ref = get_table("groups", "student")
    try:
        # Debug logging
        print(f"Received {len(students)} students to update:")
        for i, student in enumerate(students):
            print(f"Student {i+1}: {student}")
        
        # Use temporary table approach to avoid streaming buffer issues
        temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_update_{table_ref.table_id}"
        
        # Create temporary table with updated data
        temp_data = []
        for student in students:
            temp_data.append({
                "student_id": student.student_id,
                "first_name": student.first_name,
                "last_name": student.last_name,
                "date_of_birth": student.date_of_birth,
                "gender": student.gender,
                "address": student.address,
                "parent_id": student.parent_id,
                "teacher_id": student.teacher_id
            })
        
        # Upload to temporary table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        
        temp_df = pd.DataFrame(temp_data)
        client.load_table_from_dataframe(temp_df, temp_table_id, job_config=job_config).result()
        print(f"Temporary table created: {temp_table_id}")
        
        # Use MERGE from temp table to main table
        for student in students:
            merge_query = f"""
                MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
                USING `{temp_table_id}` S
                ON T.student_id = S.student_id
                WHEN MATCHED THEN
                    UPDATE SET
                        first_name = S.first_name,
                        last_name = S.last_name,
                        date_of_birth = S.date_of_birth,
                        gender = S.gender,
                        address = S.address,
                        parent_id = S.parent_id,
                        teacher_id = S.teacher_id
            """
            print(f"Executing MERGE query for student {student.student_id}")
            query_job = client.query(merge_query)
            query_job.result()
            print(f"MERGE completed for student {student.student_id}")

        # Clean up temporary table
        client.delete_table(temp_table_id, not_found_ok=True)
        print("Temporary table deleted")

        print("All update operations completed successfully")
        return {"message": f"Updated {len(students)} student successfully"}
    except Exception as e:
        print(f"Error in update_student: {e}")
        # Clean up temporary table on error
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@router.delete("/delete-student/{student_id}")
async def delete_student(student_id: str):
    table_ref = get_table("groups", "student")
    return delete_data_from_bigquery(table_ref,"student_id", student_id)

@router.post("/insert-student")
async def insert_student(student: StudentCreate):
    """
    Insert a single student (legacy endpoint for backward compatibility).
    Use /add-student for multiple students from the grid interface.
    """
    table_ref = get_table("groups", "student")
    new_id = get_next_student_id()
    row_to_insert = {
        "student_id": new_id,
        "first_name": student.first_name,
        "last_name": student.last_name,
        "date_of_birth": student.date_of_birth,
        "gender": student.gender,
        "address": student.address,
        "parent_id": student.parent_id,
        "teacher_id": student.teacher_id
    }

    errors = client.insert_rows_json(f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}", [row_to_insert])
    if errors:
        raise HTTPException(status_code=400, detail=str(errors))
    return {"message": "Inserted", "id": new_id}
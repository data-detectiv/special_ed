from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.assessment import AssessmentUpdate, AssessmentCreate
import pandas as pd
from io import StringIO, BytesIO
import re

router = APIRouter()

def get_next_assessment_id():
    """Generate the next sequential assessment ID (A001, A002, etc.)"""
    table_ref = get_table("assessment", "assessment")
    try:
        query = f"""
            SELECT assessment_id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
            WHERE assessment_id LIKE 'A%'
            ORDER BY assessment_id DESC
            LIMIT 1
        """
        query_job = client.query(query)
        results = query_job.result()
        if results.total_rows == 0:
            return "A001"
        latest_id = list(results)[0]['assessment_id']
        match = re.match(r'A(\d+)', latest_id)
        if match:
            next_num = int(match.group(1)) + 1
            return f"A{next_num:03d}"
        else:
            return "A001"
    except Exception as e:
        print(f"Error generating next assessment ID: {e}")
        return "A001"

@router.get("/get-assessment")
async def get_data():
    table_ref = get_table("assessment", "assessment")
    return fetch_data_from_bigquery(table_ref)

@router.post("/upload-assessment")
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

@router.post("/add-assessment")
async def add_assessments(assessments: list[dict]):
    """
    Add multiple new assessments from the grid interface.
    Expects a list of dictionaries with assessment data (without assessment_id).
    """
    table_ref = get_table("assessment", "assessment")
    try:
        print(f"Received {len(assessments)} assessments to add:")
        rows_to_insert = []
        for assessment_data in assessments:
            new_id = get_next_assessment_id()
            print(f"Generated ID: {new_id} for assessment: {assessment_data.get('assessment_name', 'Unknown')}")
            row_to_insert = {
                "assessment_id": new_id,
                "student_id": assessment_data.get("student_id", ""),
                "assessment_name": assessment_data.get("assessment_name", ""),
                "assessment_date": assessment_data.get("assessment_date", ""),
                "assessment_score": assessment_data.get("assessment_score", 0),
                "assessment_notes": assessment_data.get("assessment_notes", "")
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
        print(f"Successfully added {len(assessments)} assessments")
        return {"message": f"Added {len(assessments)} assessment(s) successfully"}
    except Exception as e:
        print(f"Error in add_assessments: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-assessment")
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

@router.delete("/delete-assessment/{assessment_id}")
async def delete_assessment(assessment_id: str):
    table_ref = get_table("assessment", "assessment")
    return delete_data_from_bigquery(table_ref,"assessment_id", assessment_id)
from fastapi import APIRouter, UploadFile, File, HTTPException
from services.bigquery_service import *
from models.assessment import AssessmentUpdate
import pandas as pd
from io import StringIO, BytesIO

router = APIRouter()


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
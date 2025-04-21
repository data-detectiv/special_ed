import os 
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from typing import Optional
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import date 
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
import pandas as pd
from io import StringIO, BytesIO
load_dotenv()

app = FastAPI()



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
client = bigquery.Client()

# referencing and getting the table
def get_table(dataset_name, table_name):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_name)
    tabel_ref = bigquery.TableReference(dataset_ref, table_name)
    return tabel_ref




def upload_data_to_bigquery(df, table_ref, key_column):
    from google.cloud import bigquery

    # Create a temporary table name
    temp_table_id = f"{table_ref.project}.{table_ref.dataset_id}.temp_{table_ref.table_id}"

    # Get schema of the target table
    table = client.get_table(table_ref)
    existing_columns = [field.name for field in table.schema]
    df = df[[col for col in df.columns if col in existing_columns]]

    # Ensure date format is correct
    if "date_of_birth" in df.columns:
        df.loc[:, "date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce").dt.date

    # Upload the new data to the temporary table
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

    # Build MERGE query from temp to main table
    column_list = ", ".join(existing_columns)
    insert_values = ", ".join([f"S.{col}" for col in existing_columns])
    update_clause = ", ".join([f"{col} = S.{col}" for col in existing_columns if col != key_column])

    merge_query = f"""
        MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` T
        USING `{temp_table_id}` S
        ON T.{key_column} = S.{key_column}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list}) VALUES ({insert_values})
    """

    query_job = client.query(merge_query)
    query_job.result()

    # Optional: delete the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)


# Student
@app.post("/upload-student")
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

# Parent
@app.post("/upload-parent")
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

# Teacher
@app.post("/upload-teacher")
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

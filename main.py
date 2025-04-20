from fastapi import FastAPI, UploadFile, File, Form
import pandas as pd
from io import StringIO, BytesIO
from bigquery_operations import upload_data_to_bigquery, get_table

app = FastAPI()



@app.post("/upload-student/")
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
        
        upload_data_to_bigquery(df, table_ref)
        return {"message": "File uploaded to BigQuery"}
    except Exception as e:
        return {"error": str(e)}

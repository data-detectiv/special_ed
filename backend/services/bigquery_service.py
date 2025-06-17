from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date 
import pandas as pd
import os
import json
import streamlit as st


credentials_info = st.secrets['gcp_service_account']
credentials = service_account.Credentials.from_service_account_info(credentials_info)
def get_bigquery_client():
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Try to get credentials from environment variable first, then fall back to default credentials
# def get_bigquery_client():
#     try:
#         # Check if GOOGLE_APPLICATION_CREDENTIALS is set (for service account key file)
#         if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
#             return bigquery.Client()
        
#         # Check if GCP_SERVICE_ACCOUNT_JSON is set (for JSON credentials in env var)
#         elif os.getenv('GCP_SERVICE_ACCOUNT_JSON'):
#             credentials_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_JSON'))
#             credentials = service_account.Credentials.from_service_account_info(credentials_info)
#             return bigquery.Client(credentials=credentials, project=credentials.project_id)
        
#         # Fall back to default credentials (for local development or when using gcloud auth)
#         else:
#             return bigquery.Client()
            
#     except Exception as e:
#         print(f"Error initializing BigQuery client: {e}")
#         # Fall back to default credentials
#         return bigquery.Client()

# # Initialize the client
client = get_bigquery_client()

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
    
    if "assessment_date" in df.columns:
        df["assessment_date"] = pd.to_datetime(df["assessment_date"], errors="coerce").dt.date
    # Ensure phone number is converted to string
    if "phone_number" in df.columns:
        df["phone_number"] = df["phone_number"].astype(str)
    
    if "grade_level" in df.columns:
        df["grade_level"] = df["grade_level"].astype(str)

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

def fetch_data_from_bigquery(table_ref):
    query = f"""
        SELECT * FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
"""
    query_job = client.query(query)
    results = query_job.result()
    data = [dict(row.items()) for row in results]
    return data

def delete_data_from_bigquery(table_ref, key_column, key_value):
    query = f"""
        DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
        WHERE {key_column} = @value
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("value", "STRING", key_value)
        ]
    )
    print(f"Student with id {key_column} deleted")
    return client.query(query, job_config=job_config).result()

def update_data_in_bigquery():
    query = f"""

"""
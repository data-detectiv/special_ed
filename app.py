import streamlit as st
from bigquery_data import BigqueryData



st.set_page_config(page_title="Special_Ed Portal", layout="wide")
st.title("📚 Special_Ed Admin Dashboard")

tabs = st.tabs(["👨‍🎓 Students", "👨‍👩‍👧 Parents", "👩‍🏫 Teachers", "📊 Assessments/Class"])



# --- Students Tab ---
with tabs[0]:
    st.subheader("Upload Student Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="student_upload_file")
    upload = BigqueryData("student", file)
    upload.upload_to_bq()
    upload.get_table_operations()


# --- Parents Tab ---
with tabs[1]:
    st.subheader("Upload Parent Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="parent_upload_file")
    upload = BigqueryData("parent", file)
    upload.upload_to_bq()
    upload.get_table_operations()

# --- Assessments Tab ---
with tabs[3]:
    st.subheader("Upload or Edit Assessment Scores")
    upload_type = ["Upload assessment data", "Upload class data"]
    selection = st.selectbox("Select Option to Upload", upload_type)
    if selection == upload_type[0]:
        file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="assessment_upload_file")
        upload = BigqueryData("assessment", file)
        response = upload.upload_to_bq()
        upload.get_table_operations()
    else:
        file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="class_upload_file")
        upload = BigqueryData("class", file)
        response = upload.upload_to_bq()
        upload.get_table_operations()

# --- Teachers Tab ---
with tabs[2]:
    st.subheader("Upload Teachers Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="teacher_upload_file")
    upload = BigqueryData("teacher", file)
    upload.upload_to_bq()
    upload.get_table_operations()


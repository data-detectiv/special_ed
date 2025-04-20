import streamlit as st
import pandas as pd
from streamlit_extras.switch_page_button import switch_page
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import requests



st.set_page_config(page_title="Special_Ed Portal", layout="wide")
st.title("📚 Special_Ed Admin Dashboard")

tabs = st.tabs(["👨‍🎓 Students", "👨‍👩‍👧 Parents", "👩‍🏫 Teachers", "📊 Assessments"])

# --- Students Tab ---
with tabs[0]:
    st.subheader("Upload Student Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="student_upload")

    if file:
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        st.dataframe(df)
        if st.button("📤 Upload to BigQuery"):
            with st.spinner("uploading"):
                files = {"file": (file.name, file.getvalue())}
                data = {"table": "student"}
                response = requests.post("http://127.0.0.1:8000/upload-student", files=files, data=data)

                if response.status_code == 200:
                    result = response.json()
                    if "message" in result:
                        st.success(result["message"])
                    else:
                        st.error(result.get("error", "something went wrong"))
                else:
                    st.error("Upload failed")
          
    st.subheader("📄 Current Students in Database")

    # Simulate data from BigQuery for now
    data = pd.DataFrame({
        "StudentID": [1, 2],
        "Name": ["John Doe", "Jane Smith"],
        "Class": ["Class A", "Class B"],
        "Age": [10, 11]
    })
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_pagination()
    gb.configure_default_column(editable=True)
    gb.configure_column("Delete", headerCheckboxSelection=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        data,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.MANUAL,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
    )

    updated_data = grid_response["data"]


    # Example for updating
    if st.button("💾 Save Updates"):
        # Push updated_data to BigQuery
        st.success("Updates saved.")

    # Delete logic (you’d track selected row and delete from BigQuery)
    st.markdown("Click checkbox in 'Delete' column and press the button below.")
    if st.button("❌ Delete Selected"):
        # BigQuery delete logic here
        st.warning("Selected rows deleted.")


# --- Parents Tab ---
with tabs[1]:
    st.subheader("Upload Student Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="parent_upload")

    if file:
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        st.dataframe(df)
        if st.button("📤 Upload to BigQuery"):
            with st.spinner("uploading"):
                files = {"file": (file.name, file.getvalue())}
                data = {"table": "student"}
                response = requests.post("http://127.0.0.1:8000/upload-student", files=files, data=data)

                if response.status_code == 200:
                    result = response.json()
                    if "message" in result:
                        st.success(result["message"])
                    else:
                        st.error(result.get("error", "something went wrong"))
                else:
                    st.error("Upload failed")
          
    st.subheader("📄 Current Students in Database")

    # Simulate data from BigQuery for now
    data = pd.DataFrame({
        "StudentID": [1, 2],
        "Name": ["John Doe", "Jane Smith"],
        "Class": ["Class A", "Class B"],
        "Age": [10, 11]
    })
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_pagination()
    gb.configure_default_column(editable=True)
    gb.configure_column("Delete", headerCheckboxSelection=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        data,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.MANUAL,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
    )

    updated_data = grid_response["data"]


    # Example for updating
    if st.button("💾 Save Updates"):
        # Push updated_data to BigQuery
        st.success("Updates saved.")

    # Delete logic (you’d track selected row and delete from BigQuery)
    st.markdown("Click checkbox in 'Delete' column and press the button below.")
    if st.button("❌ Delete Selected"):
        # BigQuery delete logic here
        st.warning("Selected rows deleted.")

# --- Teachers Tab ---
with tabs[2]:
    st.subheader("Upload Student Data")
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="teacher_upload")

    if file:
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        st.dataframe(df)
        if st.button("📤 Upload to BigQuery"):
            with st.spinner("uploading"):
                files = {"file": (file.name, file.getvalue())}
                data = {"table": "student"}
                response = requests.post("http://127.0.0.1:8000/upload-student", files=files, data=data)

                if response.status_code == 200:
                    result = response.json()
                    if "message" in result:
                        st.success(result["message"])
                    else:
                        st.error(result.get("error", "something went wrong"))
                else:
                    st.error("Upload failed")
          
    st.subheader("📄 Current Students in Database")

    # Simulate data from BigQuery for now
    data = pd.DataFrame({
        "StudentID": [1, 2],
        "Name": ["John Doe", "Jane Smith"],
        "Class": ["Class A", "Class B"],
        "Age": [10, 11]
    })
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_pagination()
    gb.configure_default_column(editable=True)
    gb.configure_column("Delete", headerCheckboxSelection=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        data,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.MANUAL,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
    )

    updated_data = grid_response["data"]


    # Example for updating
    if st.button("💾 Save Updates"):
        # Push updated_data to BigQuery
        st.success("Updates saved.")

    # Delete logic (you’d track selected row and delete from BigQuery)
    st.markdown("Click checkbox in 'Delete' column and press the button below.")
    if st.button("❌ Delete Selected"):
        # BigQuery delete logic here
        st.warning("Selected rows deleted.")

# --- Assessments Tab ---
with tabs[3]:
    st.subheader("Enter or Edit Assessment Scores by Class")

    # Simulated class list
    class_list = ["Class A", "Class B"]
    selected_class = st.selectbox("Select Class", class_list)

    # Simulated student list for selected class
    class_data = pd.DataFrame({
        "StudentID": [101, 102],
        "Name": ["Alice", "Bob"],
        "Score": [None, None]
    })

    edited = AgGrid(class_data, editable=True)["data"]

    if st.button("📝 Submit Scores"):
        # Send scores to BigQuery
        st.success("Scores submitted!")


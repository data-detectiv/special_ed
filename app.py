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
    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="student_upload_file")

    if file:
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        st.dataframe(df)
        if st.button("📤 Upload to BigQuery", key="student_upload_btn"):
            with st.spinner("uploading"):
                files = {"file": (file.name, file.getvalue())}
                data = {"table": "student"}
                response = requests.post("http://127.0.0.1:8000/upload-student/", files=files, data=data)

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
    try:
        # Fetch data
        response = requests.get("http://127.0.0.1:8000/get-student")
        response.raise_for_status()
        data = pd.DataFrame(response.json())

        if data.empty:
            st.warning("No student data found in the database")
            st.stop()
        data.reset_index(drop=True, inplace=True)
        
        
        # Configure grid
        gb = GridOptionsBuilder.from_dataframe(data)
        
        # Enhanced selection configuration
        gb.configure_selection(
            selection_mode='multiple',
            use_checkbox=True,
            pre_selected_rows=[],  # Initialize with empty selection
            header_checkbox=True,  # Enable checkbox in header
            groupSelectsChildren=True,
            groupSelectsFiltered=True
        )
        
        # Additional grid options
        gb.configure_grid_options(
            enableRangeSelection=True,
            suppressRowClickSelection=False,  # Ensure row clicks register
            rowSelection='multiple'  # Explicitly set selection mode
        )
        
        grid_options = gb.build()

        # Display grid with more configuration
        grid_response = AgGrid(
            data,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=300,
            theme='streamlit',
            fit_columns_on_grid_load=True,
            reload_data=False,  # Changed to False to prevent reset
            allow_unsafe_jscode=True,  # Enable advanced features
            enable_enterprise_modules=True  # Enable enterprise features
        )

        selected_rows = grid_response["selected_rows"]
        
        if isinstance(selected_rows, list) and len(selected_rows) > 0:
            selected_df = pd.DataFrame(selected_rows)
            st.write("Selected Rows:", selected_df)

            if st.button("Delete Selected Rows", type="primary"):
                student_ids = selected_df['student_id'].tolist()

                with st.status("Deleting rows...") as status:
                    try:
                        for student_id in student_ids:
                            delete_response = requests.delete(f"http://127.0.0.1:8000/delete-student/{student_id}")
                            delete_response.raise_for_status()

                        status.update(label="Deletion completed!", state="complete")

                        st.rerun()
                    except requests.exceptions.RequestException as e:
                        status.update(label="Deletion failed!", status="error")
                        st.error(f"Failed to delete students: {e}")
        elif isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
            st.write("Selected Rows:", selected_rows)
            if st.button("Delete Selected Rows", type="primary"):
                selected_df = pd.DataFrame(selected_rows)

                student_ids = selected_df['student_id'].tolist()

                with st.spinner("Deleting selected rows"):
                    success_count = 0
                    for student_id in student_ids:
                        try:
                            
                            response = requests.delete(
                                f"http://127.0.0.1:8000/delete-student/{student_id}"
                            )
                            response.raise_for_status()
                            success_count += 1
                        except requests.exceptions.RequestException:
                            st.error(f"Failed to delete student {student_id}")
                    
                    if success_count > 0:
                        st.success(f"Deleted {success_count} students successfully")
                        st.rerun()  # Refresh the data
        else:
            st.warning("No rows selected - please select rows to delete")
           

    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data: {e}")
        st.stop()



    # Example for updating
    if st.button("💾 Save Updates", key="student_save_btn"):
        # Push updated_data to BigQuery
        st.success("Updates saved.")


# # --- Parents Tab ---
# with tabs[1]:
#     st.subheader("Upload Parent Data")
#     file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="parent_upload_file")

#     if file:
#         df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
#         st.dataframe(df)
#         if st.button("📤 Upload to BigQuery", key="parent_upload_btn"):
#             with st.spinner("uploading"):
#                 files = {"file": (file.name, file.getvalue())}
#                 data = {"table": "parent"}
#                 response = requests.post("http://127.0.0.1:8000/upload-parent", files=files, data=data)

#                 if response.status_code == 200:
#                     result = response.json()
#                     if "message" in result:
#                         st.success(result["message"])
#                     else:
#                         st.error(result.get("error", "something went wrong"))

#                 else:
#                     st.error("Upload failed")
          
#     st.subheader("📄 Current Parents in Database")

#     # Simulate data from BigQuery for now
#     try:
#         response = requests.get("http://127.0.0.1:8000/get-parent")
#         response.raise_for_status()
#     except requests.exceptions.RequestException as e:
#         st.error(f"Failed to fetch data: {e}")
#     data = pd.DataFrame(response.json())

#     # Reset the index and build the grid options
#     data = data.reset_index(drop=True)
#     gb = GridOptionsBuilder.from_dataframe(data)
#     gb.configure_selection("multiple", use_checkbox=True)
#     gb.configure_pagination()
#     gb.configure_default_column(editable=True)
#     gridOptions = gb.build()

#     grid_response = AgGrid(
#         data,
#         gridOptions=gridOptions,
#         update_mode=GridUpdateMode.SELECTION_CHANGED,
#         allow_unsafe_jscode=True,
#         fit_columns_on_grid_load=True,
#         key="parent_grid"
#     )
#     st.write("Grid Response:", grid_response)
#     # selected rows
#     selected_rows = grid_response.selected_rows
#     st.write("Selected Rows:", selected_rows)
    

#     # delete button
#     if st.button("Delete"):
#         st.write("Selected Rows:", selected_rows)
#         if selected_rows:
#             for row in selected_rows:
#                 id = row.get("parent_id")
#                 print(f"Deleting parent_id: {id}")
#                 if id:
#                     requests.delete(f"http://127.0.0.1:8000/delete-parent/{id}")
#             st.success("Deleted!")
#             st.experimental_rerun()
#         else:
#             st.warning("Please select at least one student to delete.")
   



#     # Example for updating
#     if st.button("💾 Save Updates", key="parent_save_btn"):
#         # Push updated_data to BigQuery
#         st.success("Updates saved.")


# # --- Teachers Tab ---
# with tabs[2]:
#     st.subheader("Upload Teachers Data")
#     file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="teacher_upload_file")

#     if file:
#         df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
#         st.dataframe(df)
#         if st.button("📤 Upload to BigQuery", key="teacher_upload_btn"):
#             with st.spinner("uploading"):
#                 files = {"file": (file.name, file.getvalue())}
#                 data = {"table": "teacher"}
#                 response = requests.post("http://127.0.0.1:8000/upload-teacher", files=files, data=data)

#                 if response.status_code == 200:
#                     result = response.json()
#                     if "message" in result:
#                         st.success(result["message"])
#                     else:
#                         st.error(result.get("error", "something went wrong"))

#                 else:
#                     st.error("Upload failed")
          
#     st.subheader("📄 Current Teachers in Database")

#     # Simulate data from BigQuery for now
#     try:
#         response = requests.get("http://127.0.0.1:8000/get-teacher")
#         response.raise_for_status()
#     except requests.exceptions.RequestException as e:
#         st.error(f"Failed to fetch data: {e}")
#     data = pd.DataFrame(response.json())
#     gb = GridOptionsBuilder.from_dataframe(data)
#     gb.configure_pagination()
#     gb.configure_default_column(editable=True)
#     gb.configure_column("Delete", headerCheckboxSelection=True)
#     gridOptions = gb.build()

#     grid_response = AgGrid(
#         data,
#         gridOptions=gridOptions,
#         update_mode=GridUpdateMode.MANUAL,
#         allow_unsafe_jscode=True,
#         fit_columns_on_grid_load=True,
#         key="teacher_grid"
#     )

#     updated_data = grid_response["data"]


#     # Example for updating
#     if st.button("💾 Save Updates", key="teacher_save_btn"):
#         # Push updated_data to BigQuery
#         st.success("Updates saved.")


# # --- Assessments Tab ---
# with tabs[3]:
#     st.subheader("Enter or Edit Assessment Scores by Class")

#     # Simulated class list
#     class_list = ["Class A", "Class B"]
#     selected_class = st.selectbox("Select Class", class_list)

#     # Simulated student list for selected class
#     class_data = pd.DataFrame({
#         "StudentID": [101, 102],
#         "Name": ["Alice", "Bob"],
#         "Score": [None, None]
#     })

#     edited = AgGrid(class_data, editable=True, key="assessment_grid")["data"]

#     if st.button("📝 Submit Scores", key="assessment_btn"):
#         # Send scores to BigQuery
#         st.success("Scores submitted!")

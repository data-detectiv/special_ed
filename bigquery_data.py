import streamlit as st
import pandas as pd
# from streamlit_extras.switch_page_button import switch_page
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import requests

class BigqueryData:
    def __init__(self, table, file):
        self.table = table
        self.file = file

    def upload_to_bq(self):
          if self.file:
            df = pd.read_csv(self.file) if self.file.name.endswith(".csv") else pd.read_excel(self.file)
            st.dataframe(df)
            if st.button("📤 Upload to BigQuery", key=f"{self.table}_upload_btn"):
                with st.spinner("uploading"):
                    files = {"file": (self.file.name, self.file.getvalue())}
                    data = {"table": f"{self.table}"}
                    response = requests.post(f"http://127.0.0.1:8000/upload-{self.table}/", files=files, data=data)

                    if response.status_code == 200:
                        result = response.json()
                        if "message" in result:
                            st.success(result["message"])
                        else:
                            st.error(result.get("error", "something went wrong"))

                    else:
                        st.error("Upload failed")
       
          
    def get_table_operations(self):
       st.subheader(f"📄 Current {self.table}s in Database")

        # Simulate data from BigQuery for now
       try:
            # Fetch data
            response = requests.get(f"http://127.0.0.1:8000/get-{self.table}")
            response.raise_for_status()
            data = pd.DataFrame(response.json())

            if data.empty:
                st.warning(f"No {self.table} data found in the database")
                st.stop()
            data.reset_index(drop=True, inplace=True)
            
            
            # Configure grid
            gb = GridOptionsBuilder.from_dataframe(data)

            # make columns editable
            gb.configure_default_column(
                editable=True,
                filterable=True,
                sortable=True,
                resizable=True

            )
            
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

            edited_df = pd.DataFrame(grid_response['data'])
            updates = edited_df.to_dict("records")

            selected_rows = grid_response["selected_rows"]
            with st.expander("Actions"):
                
            
                
            # update button
                if st.button("Update Changes", key=f"{self.table}_btn_update"):
                    try:
                        update_response = requests.put(f"http://127.0.0.1:8000/update-{self.table}", json=updates)
                        update_response.raise_for_status()
                        st.success("Change saved successfully!")
                        st.rerun()
                    except requests.exceptions.RequestException as e:
                        st.error(f"Failed to update data: {e}")
            
            
                if isinstance(selected_rows, list) and len(selected_rows) > 0:
                    selected_df = pd.DataFrame(selected_rows)

                    if st.button("Delete Selected Rows", type="primary", key=f"{self.table}_btn_delete"):
                        student_ids = selected_df[f'{self.table}_id'].tolist()

                        with st.status("Deleting rows...") as status:
                            try:
                                for student_id in student_ids:
                                    delete_response = requests.delete(f"http://127.0.0.1:8000/delete-{self.table}/{table_id}")
                                    delete_response.raise_for_status()

                                status.update(label="Deletion completed!", state="complete")

                                st.rerun()
                            except requests.exceptions.RequestException as e:
                                status.update(label="Deletion failed!", status="error")
                                st.error(f"Failed to delete students: {e}")
                elif isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
                    if st.button("Delete Selected Rows", type="primary", key=f"{self.table}_btn_delete"):
                        selected_df = pd.DataFrame(selected_rows)

                        table_ids = selected_df[f'{self.table}_id'].tolist()

                        with st.spinner("Deleting selected rows"):
                            success_count = 0
                            for table_id in table_ids:
                                try:
                                    response = requests.delete(
                                        f"http://127.0.0.1:8000/delete-{self.table}/{table_id}"
                                    )
                                    response.raise_for_status()
                                    success_count += 1
                                except requests.exceptions.RequestException:
                                    st.error(f"Failed to delete student {id}")
                            
                            if success_count > 0:
                                st.success(f"Deleted {success_count} students successfully")
                                # st.rerun()  # Refresh the data
       except requests.exceptions.RequestException as e:
            st.error(f"Failed to fetch data: {e}")
            st.stop()
                
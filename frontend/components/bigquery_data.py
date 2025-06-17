import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import requests
from config import get_backend_url

class BigqueryData:
    def __init__(self, table, file):
        self.table = table
        self.file = file
        self.backend_url = get_backend_url()

    def upload_to_bq(self):
          if self.file:
            df = pd.read_csv(self.file) if self.file.name.endswith(".csv") else pd.read_excel(self.file)
            st.dataframe(df)
            if st.button("📤 Upload to BigQuery", key=f"{self.table}_upload_btn"):
                with st.spinner("uploading"):
                    files = {"file": (self.file.name, self.file.getvalue())}
                    data = {"table": f"{self.table}"}
                    response = requests.post(f"{self.backend_url}/upload-{self.table}/", files=files, data=data)

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
            response = requests.get(f"{self.backend_url}/get-{self.table}")
            response.raise_for_status()
            data = pd.DataFrame(response.json())

            if data.empty:
                st.warning(f"No {self.table} data found in the database")
                # Create empty dataframe with proper columns for adding new data
                if 'data' in st.session_state and f'{self.table}_columns' in st.session_state:
                    data = pd.DataFrame(columns=st.session_state[f'{self.table}_columns'])
                else:
                    st.stop()
            else:
                data.reset_index(drop=True, inplace=True)
                # Store column structure for new rows
                st.session_state[f'{self.table}_columns'] = data.columns.tolist()
                # Store original data for comparison
                st.session_state[f'{self.table}_original_data'] = data.to_dict("records")
            
            # Add new row functionality
            col1, col2 = st.columns([1, 4])
            with col1:
                if st.button("➕ Add New Row", key=f"{self.table}_add_btn"):
                    # Create a new empty row
                    new_row = {}
                    for col in data.columns:
                        if col.endswith('_id'):
                            new_row[col] = None  # ID will be auto-generated
                        else:
                            new_row[col] = ""
                    
                    # Add the new row to the dataframe
                    new_df = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)
                    st.session_state[f'{self.table}_data'] = new_df
                    st.rerun()
            
            # Use session state data if available (for new rows)
            if f'{self.table}_data' in st.session_state:
                data = st.session_state[f'{self.table}_data']
            
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
                rowSelection='multiple',  # Explicitly set selection mode
                editType='fullRow',  # Enable full row editing
                stopEditingWhenCellsLoseFocus=True
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
                
                # Save all changes button (includes new rows and updates)
                if st.button("💾 Save All Changes", key=f"{self.table}_btn_save_all", type="primary"):
                    try:
                        # Separate new rows from existing rows
                        new_rows = []
                        existing_rows = []
                        
                        st.write(f"Debug: Total updates received: {len(updates)}")
                        
                        # Get the original data to compare
                        original_data = st.session_state.get(f'{self.table}_original_data', [])
                        original_ids = {row.get(f'{self.table}_id') for row in original_data}
                        st.write(f"Debug: Original IDs: {original_ids}")
                        
                        for i, row in enumerate(updates):
                            st.write(f"Debug: Row {i}: {row}")
                            
                            # Check if this is a new row (no ID or empty ID)
                            id_col = f'{self.table}_id'
                            if id_col in row:
                                # Check if ID is None, empty string, or NaN
                                id_value = row[id_col]
                                st.write(f"Debug: Row {i} ID value: '{id_value}' (type: {type(id_value)})")
                                
                                if id_value is None or id_value == "" or (isinstance(id_value, float) and pd.isna(id_value)):
                                    # This is a new row - remove the ID field and add to new_rows
                                    new_row = {k: v for k, v in row.items() if k != id_col and v != "" and not pd.isna(v)}
                                    if new_row:  # Only add if not empty
                                        new_rows.append(new_row)
                                        st.write(f"Debug: Added to new_rows (empty ID): {new_row}")
                                elif id_value not in original_ids:
                                    # This is a new row that was added in this session
                                    new_row = {k: v for k, v in row.items() if k != id_col and v != "" and not pd.isna(v)}
                                    if new_row:  # Only add if not empty
                                        new_rows.append(new_row)
                                        st.write(f"Debug: Added to new_rows (new ID): {new_row}")
                                else:
                                    # This is an existing row
                                    existing_rows.append(row)
                                    st.write(f"Debug: Added to existing_rows: {row}")
                            else:
                                # No ID column found - treat as new row
                                new_row = {k: v for k, v in row.items() if v != "" and not pd.isna(v)}
                                if new_row:
                                    new_rows.append(new_row)
                                    st.write(f"Debug: No ID column, added to new_rows: {new_row}")
                        
                        # Debug logging
                        st.write(f"Debug: Found {len(new_rows)} new rows and {len(existing_rows)} existing rows")
                        if new_rows:
                            st.write(f"Debug: New rows to add: {new_rows}")
                        if existing_rows:
                            st.write(f"Debug: Existing rows to update: {existing_rows}")
                        
                        # Handle new rows
                        if new_rows:
                            st.write(f"Debug: Sending POST request to {self.backend_url}/add-{self.table}")
                            add_response = requests.post(f"{self.backend_url}/add-{self.table}", json=new_rows)
                            st.write(f"Debug: Response status: {add_response.status_code}")
                            st.write(f"Debug: Response content: {add_response.text}")
                            add_response.raise_for_status()
                            st.success(f"Added {len(new_rows)} new {self.table}(s) successfully!")
                        
                        # Handle existing row updates
                        if existing_rows:
                            st.write(f"Debug: Sending PUT request to {self.backend_url}/update-{self.table}")
                            update_response = requests.put(f"{self.backend_url}/update-{self.table}", json=existing_rows)
                            st.write(f"Debug: Response status: {update_response.status_code}")
                            st.write(f"Debug: Response content: {update_response.text}")
                            update_response.raise_for_status()
                            st.success(f"Updated {len(existing_rows)} existing {self.table}(s) successfully!")
                        
                        # Clear session state and refresh
                        if f'{self.table}_data' in st.session_state:
                            del st.session_state[f'{self.table}_data']
                        if f'{self.table}_original_data' in st.session_state:
                            del st.session_state[f'{self.table}_original_data']
                        st.rerun()
                        
                    except requests.exceptions.RequestException as e:
                        st.error(f"Failed to save data: {e}")
                        st.write(f"Debug: Full error details: {str(e)}")
            
                # update button (for existing rows only)
                if st.button("Update Changes", key=f"{self.table}_btn_update"):
                    try:
                        # Filter out new rows (rows without ID)
                        existing_updates = []
                        for row in updates:
                            id_col = f'{self.table}_id'
                            if id_col in row and row[id_col] is not None and row[id_col] != "" and not pd.isna(row[id_col]):
                                existing_updates.append(row)
                        
                        if existing_updates:
                            update_response = requests.put(f"{self.backend_url}/update-{self.table}", json=existing_updates)
                            update_response.raise_for_status()
                            st.success("Changes saved successfully!")
                            st.rerun()
                        else:
                            st.warning("No existing rows to update. Use 'Save All Changes' to add new rows.")
                    except requests.exceptions.RequestException as e:
                        st.error(f"Failed to update data: {e}")
            
                if isinstance(selected_rows, list) and len(selected_rows) > 0:
                    selected_df = pd.DataFrame(selected_rows)

                    if st.button("Delete Selected Rows", type="primary", key=f"{self.table}_btn_delete"):
                        student_ids = selected_df[f'{self.table}_id'].tolist()

                        with st.status("Deleting rows...") as status:
                            try:
                                for student_id in student_ids:
                                    delete_response = requests.delete(f"{self.backend_url}/delete-{self.table}/{student_id}")
                                    delete_response.raise_for_status()

                                status.update(label="Deletion completed!", state="complete")

                                st.rerun()
                            except requests.exceptions.RequestException as e:
                                status.update(label="Deletion failed!", status="error")
                                st.error(f"Failed to delete {self.table}s: {e}")
                elif isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
                    if st.button("Delete Selected Rows", type="primary", key=f"{self.table}_btn_delete"):
                        selected_df = pd.DataFrame(selected_rows)

                        table_ids = selected_df[f'{self.table}_id'].tolist()

                        with st.spinner("Deleting selected rows"):
                            success_count = 0
                            for table_id in table_ids:
                                try:
                                    response = requests.delete(
                                        f"{self.backend_url}/delete-{self.table}/{table_id}"
                                    )
                                    response.raise_for_status()
                                    success_count += 1
                                except requests.exceptions.RequestException:
                                    st.error(f"Failed to delete {self.table} {table_id}")
                            
                            if success_count > 0:
                                st.success(f"Deleted {success_count} {self.table}(s) successfully")
                                st.rerun()
       except requests.exceptions.RequestException as e:
            st.error(f"Failed to fetch data: {e}")
            st.stop()
                
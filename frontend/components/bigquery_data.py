import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import requests
from config import get_backend_url
import datetime

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

        # Use session state data if available (for new rows or edits)
        if f'{self.table}_data' in st.session_state:
            data = st.session_state[f'{self.table}_data']
        else:
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
                st.session_state[f'{self.table}_data'] = data
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to fetch data: {e}")
                st.stop()
        
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
        
        # Configure grid
        gb = GridOptionsBuilder.from_dataframe(data)
        gb.configure_default_column(
            editable=True,
            filterable=True,
            sortable=True,
            resizable=True
        )
        gb.configure_selection(
            selection_mode='multiple',
            use_checkbox=True,
            pre_selected_rows=[],
            header_checkbox=True,
            groupSelectsChildren=True,
            groupSelectsFiltered=True
        )
        gb.configure_grid_options(
            enableRangeSelection=True,
            suppressRowClickSelection=False,
            rowSelection='multiple',
            editType='fullRow',
            stopEditingWhenCellsLoseFocus=True
        )
        grid_options = gb.build()
        grid_response = AgGrid(
            data,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=300,
            theme='streamlit',
            fit_columns_on_grid_load=True,
            reload_data=False,
            allow_unsafe_jscode=True,
            enable_enterprise_modules=True
        )
        edited_df = pd.DataFrame(grid_response['data'])
        # Persist edits in session state
        st.session_state[f'{self.table}_data'] = edited_df
        updates = edited_df.to_dict("records")
        
        # Get selected rows from grid response
        selected_rows = grid_response.get("selected_rows", [])
        
        # --- Custom CSS for tighter button layout and style ---
        st.markdown(
            """
            <style>
            div[data-testid="column"] { gap: 0.01rem !important; }
            div[data-testid="stButton"] button {
                background-color: #ff4b4b;
                color: white;
                border-radius: 15px;
                font-size: 1.2em;
                margin-right: 0.2em;
                margin-bottom: 0.2em;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # --- Save and Delete Buttons Side by Side ---
        delete_enabled = isinstance(selected_rows, (list, pd.DataFrame)) and len(selected_rows) > 0
        col1, col2 = st.columns([1, 1])
        with col1:
            save_clicked = st.button("💾 Save Changes", key=f"{self.table}_btn_save_all")
        with col2:
            delete_clicked = st.button("🗑️ Delete Selected Rows", key=f"{self.table}_btn_delete", disabled=not delete_enabled)

        # Save logic
        if save_clicked:
            try:
                # Separate new rows from existing rows
                new_rows = []
                existing_rows = []
                # Get the original data to compare
                original_data = st.session_state.get(f'{self.table}_original_data', [])
                original_ids = {row.get(f'{self.table}_id') for row in original_data}
                for i, row in enumerate(updates):
                    # Check if this is a new row (no ID or empty ID)
                    id_col = f'{self.table}_id'
                    if id_col in row:
                        # Check if ID is None, empty string, or NaN
                        id_value = row[id_col]
                        if id_value is None or id_value == "" or (isinstance(id_value, float) and pd.isna(id_value)):
                            # This is a new row - remove the ID field and add to new_rows
                            new_row = {k: v for k, v in row.items() if k != id_col and v != "" and not pd.isna(v)}
                            if new_row:  # Only add if not empty
                                new_rows.append(new_row)
                        elif id_value not in original_ids:
                            # This is a new row that was added in this session
                            new_row = {k: v for k, v in row.items() if k != id_col and v != "" and not pd.isna(v)}
                            if new_row:  # Only add if not empty
                                new_rows.append(new_row)
                        else:
                            # This is an existing row
                            existing_rows.append(row)
                    else:
                        # No ID column found - treat as new row
                        new_row = {k: v for k, v in row.items() if v != "" and not pd.isna(v)}
                        if new_row:
                            new_rows.append(new_row)
                # Convert all date fields to strings in 'YYYY-MM-DD' format
                def convert_dates(row):
                    for k, v in row.items():
                        if isinstance(v, datetime.date):
                            row[k] = v.strftime("%Y-%m-%d")
                    return row
                new_rows = [convert_dates(row) for row in new_rows]
                existing_rows = [convert_dates(row) for row in existing_rows]
                # Handle new rows
                if new_rows:
                    add_response = requests.post(f"{self.backend_url}/add-{self.table}", json=new_rows)
                    add_response.raise_for_status()
                    st.success(f"Added {len(new_rows)} new {self.table}(s) successfully!")
                # Handle existing row updates
                if existing_rows:
                    update_response = requests.put(f"{self.backend_url}/update-{self.table}", json=existing_rows)
                    update_response.raise_for_status()
                    st.success(f"Updated {len(existing_rows)} existing {self.table}(s) successfully!")
                # Clear session state and refresh
                if f'{self.table}_data' in st.session_state:
                    del st.session_state[f'{self.table}_data']
                if f'{self.table}_original_data' in st.session_state:
                    del st.session_state[f'{self.table}_original_data']
                st.rerun()
            except requests.exceptions.RequestException as e:
                error_str = str(e)
                if "streaming buffer" in error_str:
                    st.error("Some rows are still being processed by BigQuery and cannot be updated yet. Please wait a few minutes and try again.")
                else:
                    st.error(f"Failed to save data: {e}")

        # Delete logic
        if delete_clicked:
            try:
                # Extract IDs from selected rows
                ids = []
                # Check if selected_rows is a DataFrame
                if isinstance(selected_rows, pd.DataFrame):
                    id_col = f'{self.table}_id'
                    if id_col in selected_rows.columns:
                        valid_ids = selected_rows[id_col].dropna()
                        ids = valid_ids.tolist()
                elif selected_rows and isinstance(selected_rows[0], str):
                    for row_index_str in selected_rows:
                        try:
                            row_index = int(row_index_str)
                            if row_index < len(edited_df):
                                row_data = edited_df.iloc[row_index].to_dict()
                                id_col = f'{self.table}_id'
                                if id_col in row_data and row_data[id_col] is not None and not pd.isna(row_data[id_col]):
                                    ids.append(row_data[id_col])
                        except (ValueError, IndexError):
                            continue
                else:
                    for row in selected_rows:
                        if isinstance(row, dict):
                            id_col = f'{self.table}_id'
                            if id_col in row and row[id_col] is not None:
                                ids.append(row[id_col])
                if not ids:
                    st.warning("No valid IDs found in selected rows")
                    return
                with st.spinner("Deleting selected rows..."):
                    success_count = 0
                    for row_id in ids:
                        try:
                            response = requests.delete(f"{self.backend_url}/delete-{self.table}/{row_id}")
                            response.raise_for_status()
                            success_count += 1
                        except requests.exceptions.RequestException as e:
                            error_str = str(e)
                            if "streaming buffer" in error_str:
                                st.error(f"Row {row_id} is still being processed by BigQuery and cannot be deleted yet. Please wait a few minutes and try again.")
                            else:
                                st.error(f"Failed to delete {self.table} {row_id}: {e}")
                    if success_count > 0:
                        st.success(f"Deleted {success_count} {self.table}(s) successfully")
                        if f'{self.table}_data' in st.session_state:
                            del st.session_state[f'{self.table}_data']
                        if f'{self.table}_original_data' in st.session_state:
                            del st.session_state[f'{self.table}_original_data']
                        st.rerun()
            except Exception as e:
                st.error(f"An error occurred during deletion: {e}")
                st.write(f"Debug - selected_rows type: {type(selected_rows)}")
                st.write(f"Debug - selected_rows content: {selected_rows}")
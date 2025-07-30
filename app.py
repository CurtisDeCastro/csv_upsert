import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import re

st.title("📄 CSV Uploader")

def get_session():
    return Session.builder.configs({
        "user": st.secrets["snowflake_user"],
        "password": st.secrets["snowflake_password"],
        "account": st.secrets["snowflake_account"],
        "warehouse": st.secrets["snowflake_warehouse"],
        "database": st.secrets["snowflake_database"],
        "schema": st.secrets["snowflake_schema"],
    }).create()

session = get_session()

uploaded_file = st.file_uploader(
    "Upload a CSV for Guest Log (name, log_date), Org Entities, or PolicyRiskData (see docs)", 
    type=["csv"]
)

# --- Table Definitions and Validation Rules ---

TABLES = {
    "guest_log": {
        "fq_table": f"{st.secrets['snowflake_database']}.{st.secrets['snowflake_schema']}.GUEST_LOG",
        "primary_keys": ["NAME", "LOG_DATE"],
        "columns": {
            "NAME": str,
            "LOG_DATE": "date"
        }
    },
    "org_entities": {
        "fq_table": "DEMO_STAGING.CUSTOMER360.ORG_ENTITIES",
        "primary_keys": ["ID"],
        "columns": {
            "ACCOUNT_NAME": str,
            "ENTITY_NAME": str,
            "ENTITY_TYPE": str,
            "ENTITY_ICON": str,
            "ENTITY_ID": str,
            "ENTITY_PARENT_ID": str,
            "ENTITY_ATTRIBUTE_1": str,
            "ENTITY_ATTRIBUTE_2": str,
            "ENTITY_ATTRIBUTE_3": str,
            "TRANSFORMED_PARENT_ID": str,
            "ACCOUNT_GUID": str,
            "SIGMA_DOLLAR_OPPORTUNITY_ESTIMATE": float,
            "RELATED_SFDC_OPP_GUID": str,
            "IMAGE_URL": str,
            "ID": str
        }
    },
    "policyriskdata": {
        "fq_table": "DEMO_STAGING.AIG_Writeback.PolicyRiskData",
        "primary_keys": ["POLICY_ID"],
        "columns": {
            "POLICY_ID": str,
            "CUSTOMER_ID": str,
            "CUSTOMER_NAME": str,
            "DATE_OF_BIRTH": "date",
            "POLICY_START_DATE": "date",
            "POLICY_END_DATE": "date",
            "POLICY_TYPE": str,
            "PREMIUM_AMOUNT": float,
            "CLAIM_COUNT": int,
            "LAST_CLAIM_DATE": "date",
            "TOTAL_CLAIM_AMOUNT": float,
            "UNDERWRITER_ID": str,
            "RISK_SCORE": float,
            "RISK_CATEGORY": str,
            "REGION": str,
            "OCCUPATION": str,
            "SMOKER_STATUS": "bool",
            "BMI": float,
            "COVERAGE_AMOUNT": float,
            "ACTUARIAL_EXPECTED_LOSS": float,
            "REINSURED": "bool",
            "REINSURER_NAME": str,
            "POLICY_STATUS": str,
            "DATA_UPLOAD_TIMESTAMP": "datetime",
            "SOURCE_SYSTEM": str
        }
    }
}

def normalize_name(name):
    """Normalize a string for matching: lower, remove spaces, underscores, hyphens."""
    return re.sub(r'[\s_\-]', '', name).lower()

def match_table(filename):
    """Determine which table the file is for, based on filename."""
    fname = normalize_name(filename)
    if "guestlog" in fname:
        return "guest_log"
    elif "orgentities" in fname or "orgentity" in fname:
        return "org_entities"
    elif "policyriskdata" in fname or "policyrisk" in fname:
        return "policyriskdata"
    return None

def build_column_map(uploaded_cols, required_cols):
    """Return a mapping from uploaded col name to required col name, using normalization."""
    norm_required = {normalize_name(col): col for col in required_cols}
    col_map = {}
    for c in uploaded_cols:
        norm = normalize_name(c)
        if norm in norm_required:
            col_map[c] = norm_required[norm]
    return col_map

def validate_and_cast_row(row, col_map, col_types):
    """Validate and cast a single row. Returns (casted_row_dict, error_reason or None)"""
    casted = {}
    for orig_col, req_col in col_map.items():
        val = row[orig_col]
        expected_type = col_types[req_col]
        # Missing value check (treat empty string or NaN as missing)
        if pd.isnull(val) or (isinstance(val, str) and val.strip() == ""):
            return None, f"Missing value in column '{orig_col}'"
        # Type casting
        try:
            if expected_type == "date":
                dt = pd.to_datetime(val, errors="coerce")
                if pd.isnull(dt):
                    return None, f"Invalid date in column '{orig_col}'"
                casted[req_col] = dt.date()
            elif expected_type == "datetime":
                dt = pd.to_datetime(val, errors="coerce")
                if pd.isnull(dt):
                    return None, f"Invalid datetime in column '{orig_col}'"
                casted[req_col] = dt
            elif expected_type == "bool":
                s = str(val).strip().lower()
                if s in ["true", "1", "yes"]:
                    casted[req_col] = True
                elif s in ["false", "0", "no"]:
                    casted[req_col] = False
                else:
                    return None, f"Invalid boolean in column '{orig_col}'"
            elif expected_type == int:
                # Accept int, or float that is integer, or string that can be cast
                if isinstance(val, int):
                    casted[req_col] = val
                elif isinstance(val, float) and val.is_integer():
                    casted[req_col] = int(val)
                elif isinstance(val, str):
                    f = float(val)
                    if f.is_integer():
                        casted[req_col] = int(f)
                    else:
                        return None, f"Non-integer value in column '{orig_col}'"
                else:
                    return None, f"Invalid integer in column '{orig_col}'"
            elif expected_type == float:
                # Accept int, float, or string that can be cast
                if isinstance(val, (int, float)):
                    casted[req_col] = float(val)
                elif isinstance(val, str):
                    casted[req_col] = float(val)
                else:
                    return None, f"Invalid float in column '{orig_col}'"
            else:  # string
                s = str(val)
                if s.strip() == "":
                    return None, f"Missing value in column '{orig_col}'"
                casted[req_col] = s
        except Exception:
            return None, f"Type error in column '{orig_col}'"
    return casted, None

def insert_rows_update_duplicates(valid_rows, session, fq_table, pk_cols, col_types):
    """
    Insert new rows. If a row is a duplicate (PK exists), check if the row is identical to the existing record.
    - If identical, skip.
    - If different, update the record.
    Returns: inserted_count, updated_count, skipped_count, new_rows_df, updated_rows_df, skipped_rows_df
    """
    if not valid_rows:
        return 0, 0, 0, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df = pd.DataFrame(valid_rows)
    # Remove duplicates on PK within the upload itself
    df = df.drop_duplicates(subset=pk_cols)
    # Get all columns for PKs in upload from Snowflake
    existing = session.table(fq_table).filter(
        " OR ".join([
            " AND ".join([f"{pk} = '{row[pk]}'" if col_types[pk] != int and col_types[pk] != float else f"{pk} = {row[pk]}" for pk in pk_cols])
            for _, row in df.iterrows()
        ])
    ).to_pandas() if not df.empty else pd.DataFrame()
    # If no existing, just insert all
    if existing.empty:
        inserted = 0
        for _, r in df.iterrows():
            values = []
            for col in df.columns:
                val = r[col]
                if pd.isnull(val):
                    values.append("NULL")
                elif col_types[col] == "date":
                    values.append(f"'{val}'")
                elif col_types[col] == "datetime":
                    values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif col_types[col] == "bool":
                    values.append("TRUE" if val else "FALSE")
                elif col_types[col] == int or col_types[col] == float:
                    values.append(str(val))
                else:
                    values.append("'" + str(val).replace("'", "''") + "'")
            sql = f"INSERT INTO {fq_table} ({', '.join(df.columns)}) VALUES ({', '.join(values)})"
            session.sql(sql).collect()
            inserted += 1
        return inserted, 0, 0, df, pd.DataFrame(), pd.DataFrame()
    # Normalize types for comparison
    for col in df.columns:
        if col in col_types:
            if col_types[col] == str:
                existing[col] = existing[col].astype(str)
                df[col] = df[col].astype(str)
            elif col_types[col] == "date":
                existing[col] = pd.to_datetime(existing[col], errors="coerce").dt.date
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif col_types[col] == "datetime":
                existing[col] = pd.to_datetime(existing[col], errors="coerce")
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif col_types[col] == int:
                existing[col] = pd.to_numeric(existing[col], errors="coerce").astype("Int64")
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif col_types[col] == float:
                existing[col] = pd.to_numeric(existing[col], errors="coerce")
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif col_types[col] == "bool":
                existing[col] = existing[col].astype(bool)
                df[col] = df[col].astype(bool)
    # Merge to classify new, update, skip
    merged = df.merge(existing, on=pk_cols, how='left', suffixes=('', '_existing'), indicator=True)
    new_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    # For rows with PK match, compare all columns
    updated_rows = []
    skipped_rows = []
    for idx, row in merged[merged['_merge'] == 'both'].iterrows():
        is_identical = True
        for col in df.columns:
            if col in pk_cols:
                continue
            val_new = row[col]
            val_exist = row.get(f"{col}_existing", None)
            # For NaN/None, treat as equal if both are null
            if pd.isnull(val_new) and pd.isnull(val_exist):
                continue
            if isinstance(val_new, float) and isinstance(val_exist, float):
                if pd.isnull(val_new) and pd.isnull(val_exist):
                    continue
                if abs(val_new - val_exist) > 1e-9:
                    is_identical = False
                    break
            elif str(val_new) != str(val_exist):
                is_identical = False
                break
        if is_identical:
            skipped_rows.append(row[df.columns])
        else:
            updated_rows.append(row[df.columns])
    # Insert new rows
    inserted = 0
    for _, r in new_rows.iterrows():
        values = []
        for col in df.columns:
            val = r[col]
            if pd.isnull(val):
                values.append("NULL")
            elif col_types[col] == "date":
                values.append(f"'{val}'")
            elif col_types[col] == "datetime":
                values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif col_types[col] == "bool":
                values.append("TRUE" if val else "FALSE")
            elif col_types[col] == int or col_types[col] == float:
                values.append(str(val))
            else:
                values.append("'" + str(val).replace("'", "''") + "'")
        sql = f"INSERT INTO {fq_table} ({', '.join(df.columns)}) VALUES ({', '.join(values)})"
        session.sql(sql).collect()
        inserted += 1
    # Update changed rows
    updated = 0
    for _, r in pd.DataFrame(updated_rows).iterrows():
        set_clause = []
        for col in df.columns:
            if col in pk_cols:
                continue
            val = r[col]
            if pd.isnull(val):
                set_clause.append(f"{col}=NULL")
            elif col_types[col] == "date":
                set_clause.append(f"{col}='{val}'")
            elif col_types[col] == "datetime":
                set_clause.append(f"{col}='{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif col_types[col] == "bool":
                set_clause.append(f"{col}={'TRUE' if val else 'FALSE'}")
            elif col_types[col] == int or col_types[col] == float:
                set_clause.append(f"{col}={val}")
            else:
                set_clause.append(f"{col}='" + str(val).replace("'", "''") + "'")
        where_clause = " AND ".join([
            f"{pk}='{r[pk]}'" if col_types[pk] != int and col_types[pk] != float else f"{pk}={r[pk]}"
            for pk in pk_cols
        ])
        sql = f"UPDATE {fq_table} SET {', '.join(set_clause)} WHERE {where_clause}"
        session.sql(sql).collect()
        updated += 1
    skipped = len(skipped_rows)
    return inserted, updated, skipped, new_rows, pd.DataFrame(updated_rows), pd.DataFrame(skipped_rows)

def colored_box(markdown, color="#f8d7da", text_color="#212529", border_color=None, icon=None):
    # Modern, high-contrast error/warning/info banners
    # color: background color, text_color: text color, border_color: border color, icon: optional emoji
    border = f"border-left: 8px solid {border_color};" if border_color else ""
    icon_html = f"<span style='font-size:1.5em;vertical-align:middle;margin-right:0.5em;'>{icon}</span>" if icon else ""
    st.markdown(
        f"""
        <div style="background: {color}; color: {text_color}; {border} padding: 1.1em 1.2em; border-radius: 10px; margin-bottom: 1.2em; font-weight: 600; font-size: 1.1em; box-shadow: 0 2px 12px 0 rgba(0,0,0,0.04); display: flex; align-items: center;">
            {icon_html}{markdown}
        </div>
        """,
        unsafe_allow_html=True
    )

if uploaded_file:
    # --- File type check ---
    if not uploaded_file.name.lower().endswith('.csv'):
        colored_box("❌ Only CSV files are accepted.", color="#ff1744", text_color="#fff", border_color="#b71c1c", icon="🚫")
        st.stop()

    # Always show a preview of the uploaded file, even if it is invalid or will be rejected
    try:
        preview_df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
        uploaded_file.seek(0)
    except Exception as e:
        colored_box(f"❌ Could not read CSV for preview: {e}", color="#ff1744", text_color="#fff", border_color="#b71c1c", icon="🚫")
        st.stop()

    # Show the file name immediately below the uploader
    st.markdown(
        f"<div style='margin-bottom: 1em; font-size: 1.1em;'><b>File:</b> {uploaded_file.name}</div>",
        unsafe_allow_html=True
    )

    # --- Validation and processing banners (immediately below file name) ---
    banners = []
    error_banner = None
    warning_banner = None
    rejected_rows = []
    rejected_reasons = []
    valid_rows = []
    inserted = updated = skipped = 0
    new_rows = updated_rows = skipped_rows = pd.DataFrame()
    show_inserted = show_updated = show_skipped = False

    filename = uploaded_file.name
    table_key = match_table(filename)
    if not table_key:
        error_banner = "❌ Unrecognized file name. Please upload a file with 'guest_log', 'org_entities', or 'policyriskdata' in the name (spaces/underscores/hyphens allowed)."
    else:
        table_def = TABLES[table_key]
        required_cols = list(table_def["columns"].keys())
        col_types = table_def["columns"]
        pk_cols = table_def["primary_keys"]
        fq_table = table_def["fq_table"]

        # Read CSV as string, do not infer types
        try:
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
        except Exception as e:
            error_banner = f"❌ Could not read CSV: {e}"

        if not error_banner:
            # --- Column name normalization and mapping ---
            col_map = build_column_map(df.columns, required_cols)
            missing = [col for col in required_cols if col not in col_map.values()]
            extra = [c for c in df.columns if c not in col_map]
            if missing:
                error_banner = f"❌ Missing required columns: {', '.join(missing)}<br>Required columns are (case/spacing/underscore/hyphen-insensitive):<br>{', '.join(required_cols)}"
            elif extra:
                error_banner = f"❌ Extra columns detected. Please remove these columns and try again.<br>Extra columns preview shown below."
            elif set(col_map.values()) != set(required_cols):
                error_banner = "❌ Column mapping error. Please check your file's columns."

        if not error_banner:
            # --- Row-level validation and casting ---
            for idx, row in df.iterrows():
                casted, reason = validate_and_cast_row(row, col_map, col_types)
                if casted is not None:
                    valid_rows.append(casted)
                else:
                    rejected_rows.append(row)
                    rejected_reasons.append(reason)

            if len(valid_rows) == 0:
                error_banner = "❌ All rows are invalid. No data uploaded."
            elif len(rejected_rows) > 0:
                warning_banner = f"⚠️ {len(rejected_rows)} row(s) were rejected due to missing or invalid values. Only valid rows will be uploaded."

        if not error_banner and len(valid_rows) > 0:
            # --- Insert/update/skip logic ---
            inserted, updated, skipped, new_rows, updated_rows, skipped_rows = insert_rows_update_duplicates(
                valid_rows, session, fq_table, pk_cols, col_types
            )
            show_inserted = inserted > 0
            show_updated = updated > 0
            show_skipped = skipped > 0

    # --- Banners (immediately below file name) ---
    if error_banner:
        # Modern, high-contrast error banner
        colored_box(
            error_banner,
            color="#ff1744",  # bright red
            text_color="#fff",
            border_color="#b71c1c",
            icon="🚫"
        )
    elif warning_banner:
        # Modern, high-contrast warning banner
        colored_box(
            warning_banner,
            color="#fff700",  # bright yellow
            text_color="#222",
            border_color="#ffd600",
            icon="⚠️"
        )
    elif not error_banner and not warning_banner:
        # Modern, high-contrast success banner
        colored_box(
            "Upload complete!",
            color="#00e676",  # bright green
            text_color="#222",
            border_color="#00c853",
            icon="✅"
        )

    # --- Section: Rejected rows (red background, after banners) ---
    if (rejected_rows and len(rejected_rows) > 0) or (error_banner and "All rows are invalid" in error_banner):
        st.markdown(
            "<div style='margin-top:1em; margin-bottom:0.5em; font-size:1.2em;'><b>🚫 Rejected: {}</b></div>".format(len(rejected_rows)),
            unsafe_allow_html=True
        )
        if rejected_rows:
            preview = pd.DataFrame(rejected_rows)
            preview["Reason"] = rejected_reasons
            st.markdown(
                """
                <div style="background-color: #ff1744; color: #fff; padding: 0.5em 0.5em 0.5em 0.5em; border-radius: 8px;">
                """,
                unsafe_allow_html=True
            )
            st.dataframe(preview.reset_index(drop=True))
            st.markdown("</div>", unsafe_allow_html=True)
        elif error_banner and "All rows are invalid" in error_banner:
            st.markdown(
                """
                <div style="background-color: #ff1744; color: #fff; padding: 0.5em 0.5em 0.5em 0.5em; border-radius: 8px;">
                No valid rows found in the file.
                </div>
                """,
                unsafe_allow_html=True
            )

    # --- Section: Skipped rows (yellow background, after rejected) ---
    if show_skipped and skipped > 0:
        st.markdown(
            "<div style='margin-top:1em; margin-bottom:0.5em; font-size:1.2em;'><b>⏭️ Skipped (identical to existing): {}</b></div>".format(skipped),
            unsafe_allow_html=True
        )
        st.markdown(
            """
            <div style="background-color: #fff700; color: #222; padding: 0.5em 0.5em 0.5em 0.5em; border-radius: 8px;">
            """,
            unsafe_allow_html=True
        )
        st.dataframe(skipped_rows.reset_index(drop=True))
        st.markdown("</div>", unsafe_allow_html=True)

    # --- Section: Updated rows (blue background, after skipped) ---
    if show_updated and updated > 0:
        st.markdown(
            "<div style='margin-top:1em; margin-bottom:0.5em; font-size:1.2em;'><b>🔄 Updated: {}</b></div>".format(updated),
            unsafe_allow_html=True
        )
        st.markdown(
            """
            <div style="background-color: #2196f3; color: #fff; padding: 0.5em 0.5em 0.5em 0.5em; border-radius: 8px;">
            """,
            unsafe_allow_html=True
        )
        st.dataframe(updated_rows.reset_index(drop=True))
        st.markdown("</div>", unsafe_allow_html=True)

    # --- Section: Inserted rows (green header, after updated) ---
    if show_inserted and inserted > 0:
        st.markdown(
            "<div style='margin-top:1em; margin-bottom:0.5em; font-size:1.2em;'><b>🆕 Inserted: {}</b></div>".format(inserted),
            unsafe_allow_html=True
        )
        st.dataframe(new_rows.reset_index(drop=True))

    # --- Section: File preview (always at the bottom) ---
    st.markdown("<hr style='margin:2em 0 1em 0;'/>", unsafe_allow_html=True)
    st.markdown("### 📋 File Preview")
    st.dataframe(preview_df)

    # --- Extra: If extra columns, show preview of extra columns (for error) ---
    if error_banner and "Extra columns detected" in error_banner and 'df' in locals() and extra:
        st.markdown(
            """
            <div style="background-color: #ff1744; color: #fff; padding: 0.5em 0.5em 0.5em 0.5em; border-radius: 8px;">
            """,
            unsafe_allow_html=True
        )
        st.dataframe(df[extra].head())
        st.markdown("</div>", unsafe_allow_html=True)

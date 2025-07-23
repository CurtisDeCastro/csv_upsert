import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

st.title("📄 Guest Log CSV Uploader")

# Get Snowflake session from configs in .streamlit/secrets.toml
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

uploaded_file = st.file_uploader("Upload a CSV with 'name' and 'log_date' (MM/DD/YYYY)", type="csv")

if uploaded_file:
    # Load into Pandas
    df = pd.read_csv(uploaded_file)

    # Validate columns
    if not {'name', 'log_date'}.issubset(df.columns):
        st.error("CSV must contain 'name' and 'log_date' columns.")
    else:
        # Parse dates & dedupe the upload
        df['log_date'] = pd.to_datetime(df['log_date'], format="%m/%d/%Y").dt.date
        df = df[['name', 'log_date']].drop_duplicates()

        # Pull existing rows (use fully qualified table name)
        fq_table = f"{st.secrets['snowflake_database']}.{st.secrets['snowflake_schema']}.GUEST_LOG"
        existing = (
            session
            .table(fq_table)
            .select("name", "log_date")
            .to_pandas()
        )
        existing.columns = [c.lower() for c in existing.columns]
        existing['log_date'] = pd.to_datetime(existing['log_date']).dt.date

        # Merge to classify new vs existing
        merged = df.merge(existing, on=['name', 'log_date'], how='left', indicator=True)
        new_rows = merged.loc[merged['_merge'] == 'left_only', ['name', 'log_date']]
        dup_rows = merged.loc[merged['_merge'] == 'both', ['name', 'log_date']]

        # Insert only new rows
        inserted = 0
        for _, r in new_rows.iterrows():
            session.sql(
                f"INSERT INTO {fq_table} (NAME, LOG_DATE) "
                f"VALUES ('{r['name']}', '{r['log_date']}')"
            ).collect()
            inserted += 1

        skipped = len(df) - inserted

        # Show summary
        st.success("✅ Upload complete!")
        st.write(f"🆕 Inserted: {inserted}")
        st.write(f"⚠️ Skipped total: {skipped}")

        if inserted:
            st.markdown("#### Newly inserted rows:")
            st.dataframe(new_rows.reset_index(drop=True))

        if not dup_rows.empty:
            st.markdown("#### Rejected duplicate rows:")
            st.dataframe(dup_rows.reset_index(drop=True))

# Sigma workbook link
st.markdown(
    """
    <a href="https://app.sigmacomputing.com/papercrane/workbook/My-Workbook-6DJ4AC2pPSvleDcbp7nmxl">
        <button style="padding:0.5em 1em;font-size:16px;margin-top:20px;">
            Go to Sigma Workbook
        </button>
    </a>
    """,
    unsafe_allow_html=True
)

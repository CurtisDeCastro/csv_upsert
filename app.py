import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

import os

# --- AIG Branding: High Contrast Custom CSS ---
AIG_BLUE = "#003366"   # Much darker blue for contrast
AIG_DARK = "#000000"   # Black for maximum contrast
AIG_LIGHT = "#FFFFFF"  # White background for contrast
AIG_FONT = "'Segoe UI', 'Arial', sans-serif"
AIG_SUCCESS_BG = "#E6FFED"  # Very light green for success, but still white-ish
AIG_WARNING_BG = "#FFF3CD"  # Very light yellow for warning

st.markdown(
    f"""
    <style>
    body, .stApp {{
        background-color: {AIG_LIGHT};
        font-family: {AIG_FONT};
        color: {AIG_DARK};
    }}
    .aig-header {{
        color: {AIG_BLUE};
        font-size: 2.5rem;
        font-weight: 900;
        border-bottom: 4px solid {AIG_BLUE};
        padding-bottom: 0.3em;
        margin-bottom: 1.2em;
        letter-spacing: 1px;
        background: {AIG_LIGHT};
        text-shadow: 0 2px 4px #0001;
    }}
    .aig-card {{
        background: {AIG_LIGHT};
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.10);
        padding: 2em 2em 1.5em 2em;
        margin-bottom: 2em;
        border: 2px solid {AIG_BLUE};
        color: {AIG_DARK};
    }}
    .aig-success {{
        color: {AIG_DARK};
        background: {AIG_SUCCESS_BG};
        border-left: 8px solid #218838;
        padding: 1em;
        border-radius: 8px;
        margin-bottom: 1em;
        font-weight: 800;
        font-size: 1.1em;
    }}
    .aig-warning {{
        color: #856404;
        background: {AIG_WARNING_BG};
        border-left: 8px solid #FF8800;
        padding: 1em;
        border-radius: 8px;
        margin-bottom: 1em;
        font-weight: 800;
        font-size: 1.1em;
    }}
    .aig-table th {{
        background-color: {AIG_BLUE} !important;
        color: #FFFFFF !important;
        font-weight: 900 !important;
    }}
    .aig-card, .aig-header, .aig-success, .aig-warning, .aig-table, .stApp, body {{
        color: {AIG_DARK} !important;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# --- Set cookies with SameSite=None; Secure for session and xsrf ---
def set_cookie_headers():
    # This function attempts to set the relevant cookies with SameSite=None; Secure
    # Streamlit does not provide a direct API, so we use a workaround with st.markdown and JS
    # This will set cookies in the browser for session and xsrf if they exist
    js = """
    <script>
    function setCookieWithAttrs(name) {
        var value = null;
        // Try to get the cookie value
        var cookies = document.cookie.split(';');
        for (var i=0; i<cookies.length; i++) {
            var c = cookies[i].trim();
            if (c.indexOf(name + "=") == 0) {
                value = c.substring((name + "=").length, c.length);
                break;
            }
        }
        if (value !== null) {
            // Set again with SameSite=None; Secure
            document.cookie = name + "=" + value + "; path=/; SameSite=None; Secure";
        }
    }
    setCookieWithAttrs("session");
    setCookieWithAttrs("xsrf-token");
    setCookieWithAttrs("XSRF-TOKEN");
    </script>
    """
    st.markdown(js, unsafe_allow_html=True)

set_cookie_headers()

st.markdown('<div class="aig-header">Guest Log CSV Uploader</div>', unsafe_allow_html=True)

# ─── Build a Snowpark session from your Streamlit secrets ───────────────────
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

# ─── File uploader UI ───────────────────────────────────────────────────────
with st.container():
    st.markdown(
        f'<div class="aig-card">',
        unsafe_allow_html=True
    )
    uploaded_file = st.file_uploader(
        '<span style="color:#000;font-weight:700;">Upload a CSV with <b>name</b> and <b>log_date</b> (MM/DD/YYYY)</span>', 
        type="csv", 
        help="The CSV must contain columns: name, log_date (MM/DD/YYYY format)."
    )
    st.markdown('</div>', unsafe_allow_html=True)

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    if not {'name', 'log_date'}.issubset(df.columns):
        st.error("CSV must contain 'name' and 'log_date' columns.")
    else:
        df['log_date'] = pd.to_datetime(df['log_date'], format="%m/%d/%Y").dt.date
        df = df[['name', 'log_date']].drop_duplicates()
        
        # Pull existing
        existing_df = (
            session
            .table("GUEST_LOG")
            .select("name", "log_date")
            .to_pandas()
        )
        existing_df.columns = [c.lower() for c in existing_df.columns]
        existing_df['log_date'] = pd.to_datetime(existing_df['log_date']).dt.date
        
        # Merge for new vs duplicate
        merged = df.merge(existing_df, on=['name','log_date'], how='left', indicator=True)
        new_rows = merged[merged['_merge']=='left_only'][['name','log_date']]
        dup_rows = merged[merged['_merge']=='both'][['name','log_date']]

        # Insert new
        inserted = 0
        for _, row in new_rows.iterrows():
            session.sql(
                f"INSERT INTO GUEST_LOG (NAME, LOG_DATE) "
                f"VALUES ('{row['name']}', '{row['log_date']}')"
            ).collect()
            inserted += 1
        skipped = len(df) - inserted

        st.markdown(
            f'<div class="aig-success">✅ <span style="color:#000;">Upload complete!</span></div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="aig-success">🆕 <span style="color:#000;">Inserted: {inserted}</span></div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="aig-warning">⚠️ <span style="color:#000;">Skipped total: {skipped}</span></div>',
            unsafe_allow_html=True
        )

        if inserted:
            st.markdown(
                f'<span style="color:{AIG_BLUE};font-weight:900;font-size:1.1em;background:#fff;padding:0.2em 0.5em;border-radius:4px;">Newly inserted rows:</span>',
                unsafe_allow_html=True
            )
            st.dataframe(
                new_rows.reset_index(drop=True),
                use_container_width=True,
                hide_index=True
            )
        if not dup_rows.empty:
            st.markdown(
                f'<span style="color:{AIG_DARK};font-weight:900;font-size:1.1em;background:#fff;padding:0.2em 0.5em;border-radius:4px;">Rejected duplicate rows:</span>',
                unsafe_allow_html=True
            )
            st.dataframe(
                dup_rows.reset_index(drop=True),
                use_container_width=True,
                hide_index=True
            )

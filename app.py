"""
A simple Streamlit web application that serves as a control panel for the
Vectis Data Factory. It provides buttons to fetch mock permit data and
run it through the AI processing pipeline, displaying live logs of the process.
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta

# Import the logic you built in agent_main
from agent_main import run_permit_agent, save_permit_to_db

# Import new ingestion spokes
from ingest_new_york import get_new_york_data
from ingest_chicago import get_chicago_data
from ingest_san_francisco import get_san_francisco_data
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data
from ingest_la import get_la_data

st.set_page_config(page_title="Vectis Data Factory", layout="wide")

st.title("🏭 Vectis Serverless Data Factory")
st.markdown("---")

# Define SODAPY_APP_TOKEN and cutoff_date
# For local development, set SODAPY_APP_TOKEN in your environment variables.
# For Streamlit Cloud, set it in .streamlit/secrets.toml
SODAPY_APP_TOKEN = os.getenv("SODAPY_APP_TOKEN") or st.secrets["SODAPY_APP_TOKEN"]
CUTOFF_DATE = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

# --- STEP 1: INGESTION ---
def get_incoming_data():
    all_records = []
    
    # Fetch data from each city's Socrata API
    all_records.extend(get_new_york_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_chicago_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_san_francisco_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_austin_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_san_antonio_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_fort_worth_data(SODAPY_APP_TOKEN, CUTOFF_DATE))
    all_records.extend(get_la_data(SODAPY_APP_TOKEN, CUTOFF_DATE))

    return all_records

# --- THE CONTROL PANEL ---
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. Ingest")
    if st.button("Fetch New Permits", type="primary"):
        # Load data into Session State so it persists
        st.session_state['raw_permits'] = get_incoming_data()
        st.success(f"Fetched {len(st.session_state['raw_permits'])} permits from Queue.")

    st.subheader("2. Run Pipeline")
    run_btn = st.button("Process & Upload")

with col2:
    st.subheader("Live Factory Logs")
    
    # Check if we have data to process
    if 'raw_permits' in st.session_state and run_btn:
        
        # Create a progress bar (Great for UX!)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_permits = len(st.session_state['raw_permits'])
        
        # --- THE PIPELINE LOOP ---
        for index, permit in enumerate(st.session_state['raw_permits']):
            
            # Update Status
            status_text.text(f"Processing ID: {permit['id']}...")
            
            # A. CALL INTELLIGENCE (Gemini)
            # We pass the description to your Agent
            classification_result = run_permit_agent(permit['desc'])
            
            if classification_result:
                # B. CALL STORAGE (Supabase)
                # We pass the clean Pydantic object + the ID
                success = save_permit_to_db(classification_result, permit['id'])
                
                if success:
                    st.toast(f"✅ {permit['id']}: Saved as {classification_result.category}")
                else:
                    st.error(f"❌ {permit['id']}: DB Save Failed")
            else:
                st.warning(f"⚠️ {permit['id']}: AI could not classify")

            # Update Progress Bar
            progress_bar.progress((index + 1) / total_permits)
            
        status_text.text("Pipeline Complete!")
        st.balloons()

    # View the Raw Data Queue
    if 'raw_permits' in st.session_state:
        st.dataframe(pd.DataFrame(st.session_state['raw_permits']))
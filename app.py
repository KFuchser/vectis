import streamlit as st
import pandas as pd
# Import the logic you built in agent_main
from agent_main import run_permit_agent, save_permit_to_db

st.set_page_config(page_title="Vectis Data Factory", layout="wide")

st.title("🏭 Vectis Serverless Data Factory")
st.markdown("---")

# --- STEP 1: INGESTION (Mock Data for Testing) ---
# In the real version, this function would call the Socrata API
def get_incoming_data():
    return [
        {"id": "FW-2024-001", "desc": "New construction of a 2500 sqft single family home with detached garage."},
        {"id": "FW-2024-002", "desc": "Commercial finish-out for Starbucks shell. HVAC and electrical work included."},
        {"id": "FW-2024-003", "desc": "Repairing fence damaged by storm. No structural changes."},
        {"id": "FW-2024-004", "desc": "Invalid entry test data 12345."} # Let's see how the AI handles "Dirty Data"
    ]

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
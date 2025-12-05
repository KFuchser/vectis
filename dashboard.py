import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# 1. Load Environment & Connect to Database
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    st.error("Supabase credentials not found. Check your .env file.")
    st.stop()

supabase: Client = create_client(url, key)

# 2. Fetch Data Function
@st.cache_data(ttl=60) # Cache for 60 seconds so it feels fast
def get_permit_data():
    # CONNECT TO THE NEW TABLE 'permits'
    response = supabase.table('permits').select("*").execute()
    df = pd.DataFrame(response.data)

    if df.empty:
        return df

    # Type Conversion (Critical for Math/Charts)
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    df['processing_days'] = pd.to_numeric(df['processing_days'], errors='coerce')
    
    # Convert dates
    if 'issued_date' in df.columns:
        df['issued_date'] = pd.to_datetime(df['issued_date'])
        
    return df

# --- DASHBOARD LAYOUT ---
st.set_page_config(page_title="Vectis Velocity 50", layout="wide")

st.title("⚡ Vectis Velocity 50: Market Pulse")
st.markdown("Real-time Permit Velocity & Bureaucratic Efficiency Index")

# 3. Load Data (This defines 'df')
df = get_permit_data()

if df.empty:
    st.warning("No data found in Supabase. Run 'ingest_velocity_50.py' first.")
    st.stop()

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter Logic")
cities = df['city'].unique().tolist() if 'city' in df.columns else []
selected_city = st.sidebar.multiselect("Select City", cities, default=cities)

# Filter the dataframe based on selection
if selected_city:
    df = df[df['city'].isin(selected_city)]

# --- VELOCITY METRICS ROW (The New Stuff) ---
st.markdown("### ⏱️ Bureaucratic Velocity Scores")
col1, col2, col3, col4 = st.columns(4)

# Filter for completed permits to calculate speed
completed = df[df['processing_days'].notnull()]

with col1:
    # Key Metric: Average Speed
    if not completed.empty:
        avg_speed = completed['processing_days'].mean()
        st.metric("Avg Approval Time", f"{avg_speed:.1f} Days", delta_color="inverse")
    else:
        st.metric("Avg Approval Time", "N/A")

with col2:
    # Volume
    st.metric("Total Permits Issued", len(df))

with col3:
    # Strategic Count
    strategic = len(df[df['complexity_tier'] == 'Strategic'])
    st.metric("Strategic Projects", strategic)

with col4:
    # Valuation
    total_val = df['valuation'].sum()
    st.metric("Total Pipeline Value", f"${total_val:,.0f}")

st.divider()

# --- CHARTS ROW ---
c1, c2 = st.columns((2, 1))

with c1:
    st.markdown("#### 📅 Velocity Trend (Last 30 Days)")
    if not df.empty and 'issued_date' in df.columns:
        # Altair Chart: Valuation over time
        chart = alt.Chart(df).mark_bar().encode(
            x='issued_date:T',
            y='valuation:Q',
            color='complexity_tier:N',
            # FIX HERE: Changed 'project_name' to 'description'
            tooltip=['description', 'valuation', 'processing_days', 'permit_id']
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

with c2:
    st.markdown("#### 🏗️ Complexity Mix")
    if not df.empty:
        # Simple breakdown of Strategic vs Commodity
        tier_counts = df['complexity_tier'].value_counts().reset_index()
        tier_counts.columns = ['Tier', 'Count']
        st.dataframe(tier_counts, hide_index=True, use_container_width=True)
        
        # Mini Chart for Speed by Tier
        if not completed.empty:
            st.caption("Avg Days by Tier:")
            st.bar_chart(completed.groupby('complexity_tier')['processing_days'].mean())

# --- DATA GRID ---
st.markdown("### 📋 Recent Permits")
st.dataframe(
    df[['city', 'complexity_tier', 'processing_days', 'valuation', 'description', 'permit_id']],
    use_container_width=True,
    hide_index=True
)
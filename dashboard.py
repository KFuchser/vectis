import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from datetime import date, datetime

# Load environment variables
load_dotenv()

# --- CONFIG ---
st.set_page_config(page_title="Vectis Indices", page_icon="🏛️", layout="wide")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# --- THEME ---
VECTIS_BLUE = "#1C2B39"
VECTIS_BRONZE = "#C87F42"
VECTIS_BG = "#F0F4F8"

# --- DATA FETCH ---
@st.cache_data(ttl=300)
def fetch_data_robust():
    if not SUPABASE_URL:
        st.error("Supabase URL not set")
        return pd.DataFrame()
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # BATCH FETCHING
    all_rows = []
    batch_size = 2000 
    start = 0
    
    placeholder = st.empty()
    placeholder.text("⏳ Connecting to Database...")
    
    while True:
        try:
            response = supabase.table('permits').select(
                "city, permit_id, applied_date, issued_date, valuation, complexity_tier, project_category, status"
            ).range(start, start + batch_size - 1).execute()
            
            rows = response.data
            if not rows:
                break
                
            all_rows.extend(rows)
            placeholder.text(f"⏳ Loaded {len(all_rows)} records...")
            
            if len(rows) < batch_size:
                break
                
            start += batch_size
            
        except Exception as e:
            st.error(f"Database Error: {e}")
            break
            
    placeholder.empty()
    
    df = pd.DataFrame(all_rows)
    
    if df.empty:
        return df

    # --- DATA NORMALIZATION ---
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    
    if 'project_category' in df.columns:
        df['project_category'] = df['project_category'].fillna("Unclassified")
    else:
        df['project_category'] = "Unclassified"

    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
    
    # Valid Mask: Keep Active (NaN) + Valid Positive Durations
    mask_valid = (df['velocity'].isna()) | (df['velocity'] >= 0)
    df = df[mask_valid]
            
    return df

# --- UI LAYOUT ---
st.title("🏛️ VECTIS INDICES")
st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
st.markdown("---")

# Load Data
df = fetch_data_robust()

if df.empty:
    st.error("DATABASE IS EMPTY. Please run 'ingest_velocity_50.py'.")
    st.stop()

# --- SIDEBAR CONFIG ---
with st.sidebar:
    st.header("Configuration")
    
    # 1. City Filter
    available_cities = sorted(df['city'].unique().tolist())
    selected_cities = st.multiselect(
        "Jurisdiction", 
        available_cities,
        default=available_cities
    )
    
    # 2. Date Filter
    min_db_date = df['applied_date'].min().date() if pd.notnull(df['applied_date'].min()) else date(2023,1,1)
    max_db_date = date.today()
    
    start_date, end_date = st.date_input(
        "Analysis Window",
        value=(min_db_date, max_db_date),
        min_value=min_db_date,
        max_value=max_db_date
    )
    
    st.divider()
    st.header("Strategic Filters")
    
    # 3. Valuation Filter (FIXED: Number Input for Precision)
    min_val = st.number_input(
        "Minimum Project Valuation ($)",
        min_value=0,
        value=0,
        step=1000,
        help="Enter 0 to include permits with missing valuation (common in Austin)."
    )
    
    # 4. Same-Day Filter
    exclude_same_day = st.checkbox("Exclude Same-Day Issuance", value=True, help="Removes Over-the-Counter permits (0 days).")
    
    st.caption(f"Total Database Records: {len(df):,}")

# --- FILTERING ---
mask_city = df['city'].isin(selected_cities)
mask_val = df['valuation'] >= min_val
mask_date = (df['applied_date'].dt.date >= start_date) & (df['applied_date'].dt.date <= end_date)

filtered_df = df[mask_city & mask_val & mask_date]

# --- METRICS CALCULATION ---
issued_df = filtered_df.dropna(subset=['velocity'])

# Apply Same-Day Exclude Logic ONLY to Velocity Metrics
if exclude_same_day:
    # Filter out 0-day velocity from metrics
    filtered_df_vol = filtered_df[ (filtered_df['velocity'] > 0) | (filtered_df['velocity'].isna()) ]
    issued_df_speed = issued_df[issued_df['velocity'] > 0]
else:
    filtered_df_vol = filtered_df
    issued_df_speed = issued_df

# METRICS ROW
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", f"{len(filtered_df_vol):,}", "Active + Completed")
with col2:
    total_val = filtered_df_vol['valuation'].sum() / 1000000
    st.metric("Pipeline Value", f"${total_val:,.1f}M", "Total CapEx")
with col3:
    if not issued_df_speed.empty:
        speed = issued_df_speed['velocity'].median()
        st.metric("Velocity Score", f"{speed:.0f} Days", "Median Time to Issue")
    else:
        st.metric("Velocity Score", "N/A", "-")
with col4:
    if not issued_df_speed.empty:
        risk = issued_df_speed['velocity'].std()
        st.metric("Friction Risk", f"±{risk:.0f} Days", "Std Dev")
    else:
        st.metric("Friction Risk", "N/A", "-")

st.markdown("---")

# --- CHARTS ---
if filtered_df_vol.empty:
    st.info("No records match filters.")
else:
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📉 Bureaucracy Leaderboard")
        if not issued_df_speed.empty:
            leaderboard = issued_df_speed.groupby('city')['velocity'].agg(['median', 'std', 'count']).reset_index()
            leaderboard.columns = ['Jurisdiction', 'Speed (Days)', 'Risk (±Days)', 'Volume']
            st.dataframe(leaderboard, use_container_width=True, hide_index=True)
        else:
            st.info("No completed permits (only active ones found).")

        st.subheader("📈 Velocity Trends")
        if not issued_df_speed.empty:
            chart_df = issued_df_speed.copy()
            chart_df['week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
            trend = chart_df.groupby(['week', 'city'])['velocity'].median().reset_index()
            
            line = alt.Chart(trend).mark_line(point=True).encode(
                x='week', y='velocity', color='city', tooltip=['city', 'week', 'velocity']
            ).properties(height=300)
            st.altair_chart(line, use_container_width=True)

    with c2:
        st.subheader("🧠 Project Categories")
        # Bar Chart
        cat_counts = filtered_df_vol['project_category'].value_counts().reset_index()
        cat_counts.columns = ['Category', 'Count']
        
        bar = alt.Chart(cat_counts).mark_bar().encode(
            x='Count', 
            y=alt.Y('Category', sort='-x'), 
            color=alt.value(VECTIS_BLUE),
            tooltip=['Category', 'Count']
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)
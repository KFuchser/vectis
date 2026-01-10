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
def fetch_data():
    if not SUPABASE_URL:
        st.error("Supabase URL not set")
        return pd.DataFrame()
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        # Fetch broad dataset
        response = supabase.table('permits').select(
            "city, permit_id, applied_date, issued_date, valuation, complexity_tier, project_category, status"
        ).execute()
        
        df = pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"Data Fetch Error: {e}")
        return pd.DataFrame()
    
    if df.empty:
        return df

    # --- TYPE CONVERSION ---
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    
    # --- LOGIC: SEPARATE ACTIVE vs. COMPLETED ---
    # 1. Calculate Velocity only where possible
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days

    # 2. Logic Fix: Do NOT drop active permits. 
    # Only drop records that are logically impossible (Negative Duration)
    # We keep NaNs (Active permits) and valid positives.
    mask_impossible = df['velocity'] < 0
    df = df[~mask_impossible]  # Keep everything EXCEPT negative durations

    # 3. Handle Missing Categories
    if 'project_category' in df.columns:
        df['project_category'] = df['project_category'].fillna("Unclassified (Pending AI)")
    else:
        df['project_category'] = "Unclassified (Pending AI)"
            
    return df

# --- UI LAYOUT ---
st.title("🏛️ VECTIS INDICES")
st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
st.markdown("---")

# Main Data Logic
df = fetch_data()

if df.empty:
    st.warning("No data found in Supabase. Run the ingest script!")
    st.stop()

# --- SIDEBAR CONFIG ---
with st.sidebar:
    st.header("Configuration")
    
    # 1. City Filter
    available_cities = df['city'].unique().tolist()
    selected_cities = st.multiselect(
        "Jurisdiction", 
        available_cities,
        default=available_cities
    )
    
    # 2. Date Filter
    # Default to 2024-01-01 to avoid showing ancient legacy data
    min_date = date(2024, 1, 1)
    max_date = date.today()
    
    try:
        db_min = df['applied_date'].min().date()
        if pd.notnull(db_min) and db_min < min_date:
            min_date_slider = db_min
        else:
            min_date_slider = min_date
    except:
        min_date_slider = min_date

    start_date, end_date = st.date_input(
        "Analysis Window",
        value=(min_date, max_date),
        min_value=min_date_slider,
        max_value=max_date
    )
    
    # 3. Valuation Filter
    min_val = st.slider("Minimum Project Valuation", 0, 1000000, 0, step=10000)
    st.caption("Note: Austin valuation data is often $0.00 due to API limits.")

# --- FILTER LOGIC ---
mask = (
    (df['city'].isin(selected_cities)) & 
    (df['valuation'] >= min_val) &
    (df['applied_date'].dt.date >= start_date) &
    (df['applied_date'].dt.date <= end_date)
)
filtered_df = df[mask]

# --- SEPARATE DATAFRAMES FOR METRICS ---
# Volume uses everything. Velocity uses only completed items.
issued_df = filtered_df.dropna(subset=['velocity'])

# METRICS ROW
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", f"{len(filtered_df):,}", "Active + Completed")
with col2:
    total_val = filtered_df['valuation'].sum() / 1000000
    st.metric("Pipeline Value", f"${total_val:,.1f}M", "Total CapEx")
with col3:
    if not issued_df.empty:
        avg_speed = issued_df['velocity'].median()
        st.metric("Velocity Score", f"{avg_speed:.0f} Days", "Median Time to Issue")
    else:
        st.metric("Velocity Score", "N/A", "No Completed Permits")
with col4:
    if not issued_df.empty:
        std_dev = issued_df['velocity'].std()
        st.metric("Friction Risk", f"±{std_dev:.0f} Days", "Uncertainty (Std Dev)")
    else:
         st.metric("Friction Risk", "N/A", "No Data")

st.markdown("---")

# --- CHARTS ---
if filtered_df.empty:
    st.info("No records match your current filters.")
else:
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📉 Bureaucracy Leaderboard")
        
        # Calculate stats only on issued permits
        leaderboard = issued_df.groupby('city')['velocity'].agg(['median', 'std', 'count']).reset_index()
        leaderboard.columns = ['Jurisdiction', 'Speed (Lower is Better)', 'Uncertainty (Std Dev)', 'Sample Size']
        
        st.dataframe(
            leaderboard.style.format({
                'Speed (Lower is Better)': '{:.1f} Days',
                'Uncertainty (Std Dev)': '±{:.1f} Days'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        st.subheader("📈 Velocity Trends")
        # Ensure we copy to avoid SettingWithCopy warning
        chart_df = issued_df.copy()
        chart_df['issue_week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
        chart_data = chart_df.groupby(['issue_week', 'city'])['velocity'].median().reset_index()
        
        line = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X('issue_week', title='Week of Issuance'),
            y=alt.Y('velocity', title='Median Days to Issue'),
            color=alt.Color('city', scale=alt.Scale(range=[VECTIS_BLUE, VECTIS_BRONZE, '#A0A0A0'])),
            tooltip=['city', 'issue_week', 'velocity']
        ).properties(height=300)
        
        st.altair_chart(line, use_container_width=True)

    with c2:
        st.subheader("🧠 AI Complexity Mix")
        # Use filtered_df (Total Volume) for the Pie Chart, not just Issued
        tier_counts = filtered_df['complexity_tier'].value_counts().reset_index()
        tier_counts.columns = ['Tier', 'Count']
        
        base = alt.Chart(tier_counts).encode(theta=alt.Theta("Count", stack=True))
        pie = base.mark_arc(outerRadius=120).encode(
            color=alt.Color("Tier", scale=alt.Scale(domain=['Strategic', 'Commodity', 'Unknown'], range=[VECTIS_BRONZE, '#A0A0A0', VECTIS_BLUE])),
            order=alt.Order("Count", sort="descending"),
            tooltip=["Tier", "Count"]
        )
        text = base.mark_text(radius=140).encode(
            text="Count",
            order=alt.Order("Count", sort="descending"),
            color=alt.value("black")  
        )
        st.altair_chart(pie + text, use_container_width=True)
        
        st.markdown("---")
        
        st.subheader("🔎 Category Drill-Down")
        cat_counts = filtered_df['project_category'].value_counts().reset_index()
        cat_counts.columns = ['Category', 'Count']
        
        bar = alt.Chart(cat_counts).mark_bar().encode(
            x=alt.X('Count', title=None),
            y=alt.Y('Category', sort='-x', title=None),
            color=alt.value(VECTIS_BLUE),
            tooltip=['Category', 'Count']
        ).properties(height=250)
        
        st.altair_chart(bar, use_container_width=True)
import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# --- 1. VECTIS BRAND CONFIG ---
st.set_page_config(
    page_title="Vectis Indices | Risk Intelligence",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CSS STYLING ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;600;800&family=JetBrains+Mono&display=swap');
    html, body, [class*="css"] { font-family: 'Inter Tight', sans-serif; color: #1C2B39; }
    .stMetric { font-family: 'JetBrains Mono', monospace; }
    div[data-testid="stMetricValue"] { color: #C87F42 !important; font-weight: 700; }
    h1, h2, h3 { color: #1C2B39 !important; font-weight: 800; letter-spacing: -0.5px; }
    section[data-testid="stSidebar"] { background-color: #F0F4F8; border-right: 1px solid #d1d5db; }
    </style>
    """, unsafe_allow_html=True)

# --- 3. DATA CONNECTION ---
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
except:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

if not url or not key:
    st.error("Credentials missing.")
    st.stop()

supabase: Client = create_client(url, key)

@st.cache_data(ttl=600)
def load_data():
    """
    Fetches ALL data and handles missing dates gracefully.
    """
    # 1. Pagination Loop
    all_records = []
    batch_size = 1000
    offset = 0
    
    while True:
        response = supabase.table('permits').select("*").range(offset, offset + batch_size - 1).execute()
        records = response.data
        all_records.extend(records)
        if len(records) < batch_size:
            break
        offset += batch_size

    df = pd.DataFrame(all_records)
    
    if df.empty:
        return df

    # 2. Type Conversion
    df['issued_date'] = pd.to_datetime(df['issued_date'])
    df['applied_date'] = pd.to_datetime(df['applied_date'])
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    
    # --- FIX 1: THE CEILING FILTER ---
    # Remove any permit issued in the future (tomorrow +). 
    # This kills the "Aug 2026" Fort Worth bug.
    df = df[df['issued_date'] <= pd.Timestamp.now()]

    # 3. Calculate Velocity
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
    
    # 4. Soft Filter
    # Keep rows even if velocity is NaN (for Volume), remove only negative errors
    mask = (df['velocity'].isna()) | (df['velocity'] >= 0)
    df = df[mask].copy()
    
    return df

# --- 4. MAIN DASHBOARD ---
def main():
    st.title("VECTIS INDICES")
    st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
    st.markdown("---")

    with st.spinner('Accessing Data Factory...'):
        df = load_data()

    if df.empty:
        st.warning("No data found.")
        st.stop()

    # --- SIDEBAR CONTROLS ---
    st.sidebar.header("Configuration")
    
    # 1. City Filter
    cities = sorted(df['city'].unique().tolist())
    selected_cities = st.sidebar.multiselect("Jurisdiction", cities, default=cities)
    
    # 2. Date Filter
    min_date = df['issued_date'].min().date()
    max_date = df['issued_date'].max().date()
    date_range = st.sidebar.date_input("Analysis Window", [min_date, max_date])

    # 3. NEW: Strategic Filters (The "Noise Canceller")
    st.sidebar.markdown("---")
    st.sidebar.subheader("Strategic Filters")
    
    # Valuation Slider
    min_val = st.sidebar.select_slider(
        "Minimum Project Valuation",
        options=[0, 1000, 10000, 50000, 100000, 500000, 1000000],
        value=0,
        help="Filter out small 'Over-the-Counter' permits to see real construction friction."
    )

    # Toggle for Same-Day
    exclude_fast = st.sidebar.checkbox("Exclude Same-Day Issuance", value=False, help="Removes permits issued instantly (Velocity = 0).")

    # --- APPLY FILTERS ---
    # Base Filters
    mask = (df['city'].isin(selected_cities)) & \
           (df['issued_date'].dt.date >= date_range[0]) & \
           (df['issued_date'].dt.date <= date_range[1]) & \
           (df['valuation'] >= min_val)
    
    filtered_df = df[mask]

    # Special logic for "Exclude Same-Day"
    if exclude_fast:
        filtered_df = filtered_df[filtered_df['velocity'] > 0]

    # --- KPI ROW ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Volume", f"{len(filtered_df):,}", delta="Permits Issued")
        
    with col2:
        val_sum = filtered_df['valuation'].sum()
        st.metric("Pipeline Value", f"${val_sum/1e6:,.1f}M")

    with col3:
        # VELOCITY SCORE
        # Note: If we filtered out all the 0s, this number will jump up significantly
        if not filtered_df['velocity'].isna().all():
            velocity_score = filtered_df['velocity'].median()
            st.metric("Velocity Score", f"{velocity_score:.0f} Days", delta_color="inverse")
        else:
            st.metric("Velocity Score", "N/A")

    with col4:
        if not filtered_df['velocity'].isna().all():
            variance = filtered_df['velocity'].std()
            st.metric("Friction Risk", f"±{variance:.0f} Days")
        else:
            st.metric("Friction Risk", "N/A")

    # --- CHART ROW ---
    st.markdown("### 📉 Velocity Trends")
    
    # Prepare Chart Data
    # Drop NaNs for charting speed
    chart_data = filtered_df.dropna(subset=['velocity']).copy()
    chart_data['Week'] = chart_data['issued_date'].dt.to_period('W').apply(lambda r: r.start_time)
    
    line_chart = alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X('Week', title='Week of Issuance'),
        y=alt.Y('median(velocity)', title='Median Days to Issue'),
        color=alt.Color('city', scale=alt.Scale(scheme='tableau10')),
        tooltip=['city', 'Week', 'median(velocity)', 'count()']
    ).properties(height=350).interactive()
    
    st.altair_chart(line_chart, use_container_width=True)

    # --- DEEP DIVE ---
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("### 🏗️ High-Value Projects")
        top_projects = filtered_df.nlargest(10, 'valuation')[['city', 'description', 'valuation', 'velocity', 'permit_id']]
        st.dataframe(
            top_projects,
            column_config={
                "valuation": st.column_config.NumberColumn("Valuation", format="$%d"),
                "velocity": st.column_config.NumberColumn("Days", format="%d d"),
            },
            hide_index=True, use_container_width=True
        )

    with c2:
        st.markdown("### 📊 Market Share")
        bar_chart = alt.Chart(filtered_df).mark_bar().encode(
            x=alt.X('count()', title='Volume'),
            y=alt.Y('city', sort='-x', title=None),
            color=alt.Color('city', legend=None)
        )
        st.altair_chart(bar_chart, use_container_width=True)

if __name__ == "__main__":
    main()
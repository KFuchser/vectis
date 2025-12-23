"""
The main Streamlit dashboard for visualizing and analyzing permit data.
"""
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

# --- 2. CSS STYLING (The "Slate & Bronze" Identity) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;600;800&family=JetBrains+Mono&display=swap');
    
    html, body, [class*="css"] { 
        font-family: 'Inter Tight', sans-serif; 
        color: #1C2B39; 
    }
    
    .stMetric { font-family: 'JetBrains Mono', monospace; }
    div[data-testid="stMetricValue"] { color: #C87F42 !important; font-weight: 700; }
    h1, h2, h3 { color: #1C2B39 !important; font-weight: 800; letter-spacing: -0.5px; }
    
    /* Metric Card Styling */
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border-left: 4px solid #C87F42;
        padding: 10px;
        box-shadow: 0px 2px 4px rgba(0,0,0,0.05);
    }
    
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
    st.error("Credentials missing. Check .env or Streamlit Secrets.")
    st.stop()

supabase: Client = create_client(url, key)

@st.cache_data(ttl=600)
def load_data():
    """
    Fetches data from 'permits' (The ACTUAL table).
    Includes Outlier Caps and Robust Valuation Mapping.
    """
    all_records = []
    batch_size = 1000
    offset = 0
    
    while True:
        # Fetch from 'permits'
        response = supabase.table('permits').select("*").range(offset, offset + batch_size - 1).execute()
        records = response.data
        all_records.extend(records)
        if len(records) < batch_size:
            break
        offset += batch_size

    df = pd.DataFrame(all_records)
    
    if df.empty:
        return df

    # --- 1. CLEANING & MAPPING ---
    column_map = {
        'issue_date': 'issued_date',
        'work_description': 'description',
        'est_value': 'valuation',
        'project_cost': 'valuation',
        'total_job_valuation': 'valuation' # The Critical Fix for Austin
    }
    df = df.rename(columns=column_map)

    # --- 2. ROBUST TYPE CONVERSION ---
    # Fix Dates
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    
    # Fix Valuation (Handle '$', ',', and text)
    if 'valuation' in df.columns:
        df['valuation'] = df['valuation'].astype(str).str.replace(r'[$,]', '', regex=True)
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    else:
        df['valuation'] = 0.0

    # --- 3. LOGIC FILTERS ---
    # A. The "Time Traveler" Patch (No Futures)
    df = df[df['issued_date'] <= pd.Timestamp.now()]

    # B. Calculate Velocity
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days

    # C. THE SANITY CAPS (Outlier Protection)
    # Exclude negative durations (clerical error)
    df.loc[df['velocity'] < 0, 'velocity'] = None 
    # Exclude "Ancient" holds (Outliers > 10 years likely data errors)
    df.loc[df['velocity'] > 3650, 'velocity'] = None 

    return df

# --- 4. MAIN DASHBOARD ---
def main():
    st.title("VECTIS INDICES")
    st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
    st.markdown("---")

    with st.spinner('Accessing Data Factory...'):
        df = load_data()

    if df.empty:
        st.warning("System Online. Awaiting Data Ingestion...")
        st.stop()

    # --- SIDEBAR CONTROLS ---
    st.sidebar.header("Configuration")
    
    # 1. City Filter
    cities = sorted(df['city'].unique().tolist())
    selected_cities = st.sidebar.multiselect("Jurisdiction", cities, default=cities)
    
   # 2. Date Filter
    # Fix: Ensure the column name string is closed properly
    if not df['issued_date'].isnull().all():
        min_date = df['issued_date'].min().date()
        max_date = df['issued_date'].max().date()
        date_range = st.sidebar.date_input("Analysis Window", [min_date, max_date])
    else:
        date_range = [pd.Timestamp.now().date(), pd.Timestamp.now().date()]

    # 3. Strategic Filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("Strategic Filters")
    
    min_val = st.sidebar.select_slider(
        "Minimum Project Valuation",
        options=[0, 1000, 10000, 50000, 100000, 500000, 1000000],
        value=0,
        help="Filter out small 'Over-the-Counter' permits to see real construction friction."
    )

    exclude_fast = st.sidebar.checkbox("Exclude Same-Day Issuance", value=False, help="Removes permits issued instantly (Velocity = 0).")
    
    # [FIXED] Indentation & Logic for Austin Advisory
    if "Austin" in selected_cities:
        st.info(
            "**📉 Data Source Advisory:** "
            "Austin permit valuations are currently under-reported due to limitations in the city's public API. "
            "We capture valuation where available (via 'Estimated Project Cost'), but many records default to $0. "
            "Volume and Velocity metrics remain 100% accurate."
        )

    # NEW: The "San Antonio Fix" (Deduplication)
    collapse_projects = st.sidebar.checkbox(
        "Collapse Duplicate Projects", 
        value=True, 
        help="San Antonio copies the Total Project Value to every sub-permit. Check this to count the PROJECT only once."
    )

    # --- APPLY FILTERS ---
    start_date = date_range[0]
    end_date = date_range[1] if len(date_range) > 1 else date_range[0]

    mask = (df['city'].isin(selected_cities)) & \
           (df['issued_date'].dt.date >= start_date) & \
           (df['issued_date'].dt.date <= end_date) & \
           (df['valuation'] >= min_val)
    
    filtered_df = df[mask].copy()

    if exclude_fast:
        filtered_df = filtered_df[filtered_df['velocity'] > 0]

    # NEW: DEDUPLICATION LOGIC (Aggressive)
    if collapse_projects and not filtered_df.empty:
        before_count = len(filtered_df)
        filtered_df = filtered_df.sort_values('issued_date', ascending=True)
        filtered_df = filtered_df.drop_duplicates(
            subset=['city', 'valuation'], 
            keep='first'
        )
        after_count = len(filtered_df)
        
        if before_count != after_count:
            st.sidebar.caption(f"📉 Collapsed {before_count - after_count} duplicate sub-permits.")

    if filtered_df.empty:
        st.warning("No records match the current filters.")
        st.stop()
        
    # --- KPI ROW ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Volume", f"{len(filtered_df):,}", delta="Active Permits")
        
    with col2:
        val_sum = filtered_df['valuation'].sum()
        # [FIXED] Added the help tooltip here directly
        st.metric(
            "Pipeline Value", 
            f"${val_sum/1e6:,.1f}M", 
            delta="Total CapEx",
            help="⚠️ Austin Data Note: Valuation may be under-reported due to API limitations."
        )

    with col3:
        if not filtered_df['velocity'].isna().all():
            velocity_score = filtered_df['velocity'].median()
            st.metric("Velocity Score", f"{velocity_score:.0f} Days", delta_color="inverse", help="Median days from Application to Issuance")
        else:
            st.metric("Velocity Score", "N/A")

    with col4:
        if not filtered_df['velocity'].isna().all():
            variance = filtered_df['velocity'].std()
            st.metric("Friction Risk", f"±{variance:.0f} Days", delta_color="off", help="Standard Deviation (Uncertainty)")
        else:
            st.metric("Friction Risk", "N/A")

    # --- THE LEADERBOARD ---
    st.markdown("### 🏛️ Bureaucracy Leaderboard (The 'Compare & Shame')")
    
    leaderboard = filtered_df.groupby('city').agg(
        Velocity_Median=('velocity', 'median'),
        Friction_Risk=('velocity', 'std'),
        Volume=('velocity', 'count')
    ).reset_index().sort_values('Velocity_Median')

    leaderboard['Velocity_Median'] = leaderboard['Velocity_Median'].map('{:.1f} Days'.format)
    leaderboard['Friction_Risk'] = leaderboard['Friction_Risk'].fillna(0).map('±{:.1f} Days'.format)
    
    st.dataframe(
        leaderboard,
        column_config={
            "city": "Jurisdiction",
            "Velocity_Median": "Speed (Lower is Better)",
            "Friction_Risk": "Uncertainty (Std Dev)",
            "Volume": "Sample Size"
        },
        hide_index=True,
        use_container_width=True
    )

    # --- CHARTS ROW ---
    c_chart1, c_chart2 = st.columns([2, 1])
    
    with c_chart1:
        st.markdown("#### 📉 Velocity Trends")
        chart_data = filtered_df.dropna(subset=['velocity']).copy()
        if not chart_data.empty:
            chart_data['Week'] = chart_data['issued_date'].dt.to_period('W').apply(lambda r: r.start_time)
            
            line_chart = alt.Chart(chart_data).mark_line(point=True).encode(
                x=alt.X('Week', title='Week of Issuance'),
                y=alt.Y('median(velocity)', title='Median Days to Issue'),
                color=alt.Color('city', scale=alt.Scale(scheme='tableau10')),
                tooltip=['city', 'Week', 'median(velocity)', 'count()']
            ).properties(height=350).interactive()
            st.altair_chart(line_chart, use_container_width=True)
        else:
            st.info("Insufficient data for trends.")

    with c_chart2:
        st.markdown("#### 🧠 AI Complexity Mix")
        if 'complexity_tier' in filtered_df.columns:
            filtered_df['complexity_tier'] = filtered_df['complexity_tier'].fillna('Standard')
            
            pie_chart = alt.Chart(filtered_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('count()', stack=True),
                color=alt.Color('complexity_tier', 
                                scale=alt.Scale(domain=['Strategic', 'Commodity', 'Standard'], 
                                                range=['#C87F42', '#A9A9A9', '#1C2B39']),
                                legend=alt.Legend(title="Tier")),
                tooltip=['complexity_tier', 'count()']
            ).properties(height=350)
            st.altair_chart(pie_chart, use_container_width=True)
        else:
            st.info("AI Classification not yet run.")

    # --- DEEP DIVE ---
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

if __name__ == "__main__":
    main()
import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from datetime import date, timedelta

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

# --- DATA FETCH WITH PROGRESS BAR ---
@st.cache_data(ttl=300)
def fetch_data_robust():
    if not SUPABASE_URL:
        st.error("Supabase URL not set")
        return pd.DataFrame()
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. Get Total Count First (Fast Check)
    try:
        count_response = supabase.table('permits').select('id', count='exact', head=True).execute()
        total_count = count_response.count
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

    if total_count == 0:
        return pd.DataFrame()

    # 2. Batched Fetching (Smart Pagination)
    all_rows = []
    batch_size = 3000 # Larger batch for speed
    
    # Create a placeholder for the progress bar
    progress_text = f"Fetching {total_count} records from Supabase..."
    my_bar = st.progress(0, text=progress_text)
    
    for i, start in enumerate(range(0, total_count, batch_size)):
        end = start + batch_size - 1
        try:
            response = supabase.table('permits').select(
                "city, permit_id, applied_date, issued_date, valuation, complexity_tier, project_category, status"
            ).range(start, end).execute()
            
            all_rows.extend(response.data)
            
            # Update Progress
            percent_complete = min((len(all_rows) / total_count), 1.0)
            my_bar.progress(percent_complete, text=f"Loading: {len(all_rows)} / {total_count} records...")
            
        except Exception as e:
            st.error(f"Batch Error at {start}: {e}")
            break
            
    my_bar.empty() # Clear bar when done
    
    df = pd.DataFrame(all_rows)
    
    if df.empty:
        return df

    # --- DATA PROCESSING ---
    # Type Conversion
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    
    # Velocity Calculation
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days

    # Logic: Keep Active (NaN) and Valid Completed (>=0). Drop Impossible (<0).
    mask_valid = (df['velocity'].isna()) | (df['velocity'] >= 0)
    df = df[mask_valid]

    # Handle Missing Categories
    if 'project_category' in df.columns:
        df['project_category'] = df['project_category'].fillna("Unclassified (Pending AI)")
    else:
        df['project_category'] = "Unclassified (Pending AI)"
            
    return df

# --- UI LAYOUT ---
st.title("🏛️ VECTIS INDICES")
st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
st.markdown("---")

# Fetch Data
df = fetch_data_robust()

if df.empty:
    st.warning("No data found. Please run the ingestion script.")
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
    # Default: Last 12 Months
    today = date.today()
    default_start = today.replace(year=today.year - 1)
    
    # Guardrail for clean date inputs
    try:
        start_date, end_date = st.date_input(
            "Analysis Window",
            value=(default_start, today),
            max_value=today
        )
    except:
        start_date, end_date = default_start, today
    
    # 3. Valuation Filter
    min_val = st.slider("Minimum Project Valuation", 0, 1000000, 0, step=10000)
    st.caption("Note: Austin valuation data is often $0.00.")
    
    # Debug Stat
    st.divider()
    st.caption(f"🔌 Total Records Loaded: {len(df):,}")

# --- FILTER LOGIC ---
mask = (
    (df['city'].isin(selected_cities)) & 
    (df['valuation'] >= min_val) &
    (df['applied_date'].dt.date >= start_date) &
    (df['applied_date'].dt.date <= end_date)
)
filtered_df = df[mask]

# --- SEPARATE DATAFRAMES ---
# issued_df is ONLY for Velocity stats (Completed permits)
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
    st.info("No records match your filters.")
else:
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📉 Bureaucracy Leaderboard")
        
        # Leaderboard uses issued_df (Velocity)
        if not issued_df.empty:
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
        else:
            st.info("No completed permits in selection.")
        
        st.subheader("📈 Velocity Trends")
        if not issued_df.empty:
            chart_df = issued_df.copy()
            chart_df['issue_week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
            # Group by Week + City for readable trend lines
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
        # Uses filtered_df (All Volume)
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
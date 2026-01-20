import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Vectis Command Console")
DAILY_CARRY_COST = 500 

# --- STYLING ---
st.markdown("""
    <style>
    .metric-card { background-color: #F0F4F8; border-left: 5px solid #C87F42; padding: 15px; }
    h1, h2, h3 { color: #1C2B39; font-family: 'Inter', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

# --- STRICT DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 1. Fetch Data (Select ALL to ensure we don't miss columns)
        response = supabase.table('permits').select("*").execute()
        if not response.data: return pd.DataFrame()
        df = pd.DataFrame(response.data)

        # 2. SCHEMA ALIGNMENT (The Fix)
        # Your DB uses 'issued_date', dashboard expects 'issue_date'. We standardize here.
        if 'issued_date' in df.columns:
            df = df.rename(columns={'issued_date': 'issue_date'})
            
        # 3. TYPE CONVERSION
        # Convert dates to datetime objects
        if 'issue_date' in df.columns:
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        
        if 'applied_date' in df.columns:
            df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')

        # 4. VELOCITY LOGIC (Priority: Processing Days)
        # We prefer the DB's pre-calculated integer if available
        if 'processing_days' in df.columns:
            df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
            df['velocity'] = None

        # Fallback: Calculate Issue - Applied if DB velocity is missing
        # Only runs if 'velocity' is still NaN
        if 'issue_date' in df.columns and 'applied_date' in df.columns:
            mask_calc = df['velocity'].isna() & df['issue_date'].notna() & df['applied_date'].notna()
            df.loc[mask_calc, 'velocity'] = (df.loc[mask_calc, 'issue_date'] - df.loc[mask_calc, 'applied_date']).dt.days

        # 5. STRICT FILTERING (Remove Noise)
        # Drop rows where we have NO velocity data (avoid "0 day" illusion for unknowns)
        df = df.dropna(subset=['velocity'])
        
        # Drop "Instant" permits (0 days) - User Directive: Exclude OTC
        df = df[df['velocity'] > 0]
        
        # Drop Data Errors (Negative days or > 10 years)
        df = df[(df['velocity'] >= 0) & (df['velocity'] < 3650)]

        # 6. Metadata Cleanup
        if 'complexity_tier' not in df.columns: 
            df['complexity_tier'] = 'Unknown'
        df['complexity_tier'] = df['complexity_tier'].fillna('Unknown')
        
        # Check for valuation column variations
        val_cols = ['valuation', 'job_value', 'est_project_cost', 'jobvalue']
        found_val = False
        for c in val_cols:
            if c in df.columns:
                df['valuation'] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                found_val = True
                break
        if not found_val:
            df['valuation'] = 0

        return df

    except Exception as e:
        st.error(f"Data Pipeline Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.caption("v4.0 | SOURCE: public.permits")

if df.empty:
    st.warning("No valid data found matching criteria. Check Database.")
    # Debug helper: Only shows if data load fails completely
    # st.write("Debug info: Check if 'issued_date' exists in Supabase table.")
    st.stop()

# Filter: Tiers (Default to excluding Unknown/Commodity if possible)
all_tiers = sorted(df['complexity_tier'].unique().tolist())
default_tiers = [t for t in all_tiers if t != 'Commodity']
if not default_tiers: default_tiers = all_tiers

selected_tiers = st.sidebar.multiselect("Permit Class", all_tiers, default=default_tiers)
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

if df_filtered.empty:
    st.info("No records match the selected filters.")
    st.stop()

# --- HERO SECTION: LEADERBOARD ---
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("_Median Days to Issue (Excluding OTC/Instant)_")

# Group & Calculate
leaderboard = df_filtered.groupby('city').agg(
    median_days=('velocity', 'median'),
    volume=('velocity', 'count'),
    total_value=('valuation', 'sum')
).reset_index()

# Benchmark Logic
benchmark = leaderboard['median_days'].min()
leaderboard['delay'] = leaderboard['median_days'] - benchmark
leaderboard['tax'] = leaderboard['delay'] * DAILY_CARRY_COST

leaderboard = leaderboard.sort_values('median_days')

st.dataframe(
    leaderboard,
    use_container_width=True,
    hide_index=True,
    column_config={
        "city": st.column_config.TextColumn("Jurisdiction"),
        "median_days": st.column_config.NumberColumn(
            "Median Velocity", 
            format="%d days",
            help="Median Processing Days (Source: DB)"
        ),
        "volume": st.column_config.NumberColumn("Volume (n)"),
        "delay": st.column_config.NumberColumn(
            "Delay vs Best", 
            format="+%d days"
        ),
        "tax": st.column_config.ProgressColumn(
            "Cost of Delay ($)",
            format="$%d",
            min_value=0,
            max_value=int(leaderboard['tax'].max() * 1.1) if leaderboard['tax'].max() > 0 else 1000,
            help=f"Implied liability at ${DAILY_CARRY_COST}/day"
        ),
    }
)

st.divider()

# --- SECTION 2: VELOCITY MATRIX ---
st.markdown("### 🧩 Velocity Matrix")
st.caption("Median Days to Issue by City & Class")

matrix = df_filtered.groupby(['city', 'complexity_tier'])['velocity'].median().reset_index()

heatmap = alt.Chart(matrix).mark_rect().encode(
    x=alt.X('complexity_tier', title='Permit Class'),
    y=alt.Y('city', title='Jurisdiction'),
    color=alt.Color('velocity', title='Days', scale={'scheme': 'orangered'}),
    tooltip=['city', 'complexity_tier', 'velocity']
).properties(height=350)

text = heatmap.mark_text().encode(
    text=alt.Text('velocity', format='.0f'),
    color=alt.value('black')
)

st.altair_chart(heatmap + text, use_container_width=True)

# --- SECTION 3: METRICS ---
st.divider()
c1, c2, c3 = st.columns(3)

with c1:
    st.metric("Total Active Permits", f"{len(df_filtered)}")
with c2:
    val_millions = df_filtered['valuation'].sum() / 1_000_000
    st.metric("Pipeline Value", f"${val_millions:.1f}M")
with c3:
    st.metric("Data Source", "Live Supabase Feed")
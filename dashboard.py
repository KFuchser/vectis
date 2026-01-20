import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Vectis Command Console")
st.markdown("""<style>.metric-card { background-color: #F0F4F8; border-left: 5px solid #C87F42; padding: 15px; }</style>""", unsafe_allow_html=True)

# --- STRICT DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    # 1. Connect
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase = create_client(url, key)
    
    # 2. Fetch Raw Data (No Filters Yet)
    response = supabase.table('permits').select("*").execute()
    if not response.data: return pd.DataFrame()
    df = pd.DataFrame(response.data)

    # 3. TYPE CONVERSION (Crucial)
    # Convert dates to datetime objects so math works
    cols_to_date = ['issue_date', 'applied_date', 'status_date']
    for col in cols_to_date:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 4. VELOCITY LOGIC (The "Honest" Math)
    # Priority A: Use 'processing_days' from DB if it exists and is valid
    if 'processing_days' in df.columns:
        df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
    else:
        df['velocity'] = None # No column, no speed

    # Priority B: Calculate Issue - Applied if Priority A failed/missing
    # We only do this for rows where 'velocity' is still NaN
    mask_calc = df['velocity'].isna() & df['issue_date'].notna() & df['applied_date'].notna()
    df.loc[mask_calc, 'velocity'] = (df.loc[mask_calc, 'issue_date'] - df.loc[mask_calc, 'applied_date']).dt.days

    # 5. THE GREAT FILTER (Remove Garbage)
    # Drop rows where we STILL don't have a velocity
    df = df.dropna(subset=['velocity'])
    
    # Drop "Instant" permits (0 days) -> These are OTC/Vending Machine permits, not Projects.
    df = df[df['velocity'] > 0]
    
    # Drop Negative errors and 10-year outliers
    df = df[(df['velocity'] >= 0) & (df['velocity'] < 3650)]

    # 6. Metadata cleanup
    if 'complexity_tier' not in df.columns: df['complexity_tier'] = 'Unknown'
    df['complexity_tier'] = df['complexity_tier'].fillna('Unknown')
    
    if 'valuation' in df.columns:
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    else:
        df['valuation'] = 0

    return df

df = load_data()

# --- SIDEBAR ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.caption("v3.1 | LOGIC: EXCLUDE 0-DAY")

if df.empty:
    st.error("No valid data found after filtering. Check Database.")
    st.stop()

# Filter: Tiers
all_tiers = sorted(df['complexity_tier'].unique().tolist())
selected_tiers = st.sidebar.multiselect("Permit Class", all_tiers, default=all_tiers)
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

if df_filtered.empty:
    st.warning("No data for selected filters.")
    st.stop()

# --- MAIN VIEW: THE MATRIX ---
st.markdown("### 🧩 The Velocity Matrix")
st.markdown("Median Days to Issue by City & Class (Excluding 0-day OTC permits)")

# Pivot Table Calculation
matrix = df_filtered.groupby(['city', 'complexity_tier']).agg(
    median_days=('velocity', 'median'),
    count=('velocity', 'count')
).reset_index()

# 1. The Heatmap (Visual)
heatmap = alt.Chart(matrix).mark_rect().encode(
    x=alt.X('complexity_tier', title='Permit Class'),
    y=alt.Y('city', title='Jurisdiction'),
    color=alt.Color('median_days', title='Days', scale={'scheme': 'reds'}),
    tooltip=['city', 'complexity_tier', 'median_days', 'count']
).properties(height=350)

text = heatmap.mark_text().encode(
    text=alt.Text('median_days', format='.0f'),
    color=alt.value('black')
)

st.altair_chart(heatmap + text, use_container_width=True)

# 2. The Raw Data Table (Proof)
with st.expander("View Underlying Data (The Truth Table)", expanded=True):
    st.dataframe(
        matrix.pivot(index='city', columns='complexity_tier', values='median_days'),
        use_container_width=True
    )

st.divider()

# --- HERO METRICS (Aggregated) ---
st.markdown("### 🏛️ Aggregate Performance")
col1, col2, col3 = st.columns(3)

avg_velocity = df_filtered['velocity'].median()
total_vol = len(df_filtered)
total_val = df_filtered['valuation'].sum()

col1.metric("Median Speed (All Cities)", f"{avg_velocity:.0f} Days")
col2.metric("Total Active Permits", f"{total_vol}")
col3.metric("Pipeline Value", f"${total_val/1_000_000:.1f}M")

# --- TRENDS ---
st.markdown("### 📉 Velocity Trends")
chart_data = df_filtered.copy()
if 'issue_date' in chart_data.columns:
    chart_data['month'] = chart_data['issue_date'].dt.to_period('M').apply(lambda r: r.start_time)
    trend = chart_data.groupby(['city', 'month'])['velocity'].median().reset_index()
    
    line = alt.Chart(trend).mark_line(point=True).encode(
        x='month', 
        y='velocity', 
        color='city',
        tooltip=['city', 'month', 'velocity']
    ).interactive()
    st.altair_chart(line, use_container_width=True)
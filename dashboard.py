import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import time

st.set_page_config(layout="wide", page_title="Vectis Command Console")

# --- STYLING ---
st.markdown("""
    <style>
    .stApp { background-color: #F8F9FA; }
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border-left: 5px solid #C87F42;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    h1, h2, h3 { font-family: 'Arial', sans-serif; color: #1C2B39; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # --- PAGINATION LOOP: THE "1000 LIMIT" FIX ---
        all_records = []
        chunk_size = 1000 
        offset = 0
        
        # Placeholder to show loading progress
        progress_text = "Fetching complete dataset..."
        my_bar = st.progress(0, text=progress_text)

        while True:
            # Fetch a chunk of 1000
            response = supabase.table('permits')\
                .select("*")\
                .order('issued_date', desc=True)\
                .range(offset, offset + chunk_size - 1)\
                .execute()
            
            data = response.data
            all_records.extend(data)
            
            # Update progress bar (visual feedback)
            my_bar.progress(min(len(all_records) / 12000, 1.0), text=f"Fetched {len(all_records)} records...")
            
            # If we got less than the chunk size, we reached the end
            if len(data) < chunk_size:
                break
                
            offset += chunk_size
            time.sleep(0.1) # Be nice to the API

        my_bar.empty() # Clear progress bar
            
        df = pd.DataFrame(all_records)
        
        if not df.empty:
            df['issue_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
            df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
            
            # 1. TIMEZONE CLEANUP
            if df['issue_date'].dt.tz is not None:
                df['issue_date'] = df['issue_date'].dt.tz_localize(None)
            
            # 2. TIME GUARD (Filter out the Fort Worth "Future" dates)
            # We filter locally, but since we fetched EVERYTHING, San Antonio is safely included
            now = pd.Timestamp.now() + pd.Timedelta(days=1)
            df = df[df['issue_date'] <= now]

            # 3. VELOCITY CALCULATION
            df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days
            
        return df
    except Exception as e:
        st.error(f"Data Load Error: {e}")
        return pd.DataFrame()

st.sidebar.title("Vectis Command")
if st.sidebar.button("🔄 Force Refresh"):
    st.cache_data.clear()
    st.rerun()

df_raw = load_data()

# --- TRUTH TABLE (VERIFICATION) ---
if not df_raw.empty:
    with st.expander("🔎 Database Content Verification (Click to Expand)", expanded=True):
        counts = df_raw['city'].value_counts().reset_index()
        counts.columns = ['City', 'Record Count']
        st.dataframe(counts, use_container_width=True, hide_index=True)

# --- FILTERS ---
min_val = st.sidebar.slider("Valuation Floor ($)", 0, 1000000, 0, step=10000)
all_tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
selected_tiers = st.sidebar.multiselect("Complexity Tiers", all_tiers, default=all_tiers)
cities = sorted(df_raw['city'].unique().tolist()) if not df_raw.empty else []
selected_cities = st.sidebar.multiselect("Jurisdictions", cities, default=cities)

if not df_raw.empty:
    df = df_raw[
        (df_raw['valuation'] >= min_val) & 
        (df_raw['complexity_tier'].isin(selected_tiers)) &
        (df_raw['city'].isin(selected_cities))
    ].copy()
else:
    df = pd.DataFrame()

st.title("🏛️ National Regulatory Friction Index")

if df.empty:
    st.warning("No records found. Check filters or database connection.")
    st.stop()

# --- METRICS ---
real_projects = df[df['velocity'] >= 0]
median_vel = real_projects['velocity'].median() if not real_projects.empty else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Volume", len(df))
c2.metric("Median Lead Time", f"{median_vel:.0f} Days")
c3.metric("Pipeline Value", f"${df['valuation'].sum()/1e6:.1f}M")
c4.metric("High Friction (>180d)", len(df[df['velocity'] > 180]))

st.divider()

# --- CHARTS ---
st.caption("💡 *Tip: Click and drag charts to pan. Use mouse wheel to zoom.*")
col_vol, col_vel = st.columns(2)

with col_vol:
    st.subheader("📊 Weekly Volume")
    if not df.empty:
        df['week'] = df['issue_date'].dt.to_period('W').apply(lambda r: r.start_time)
        line_vol = alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('week:T', title='Week Of', axis=alt.Axis(format='%b %d')),
            y=alt.Y('count():Q', title='Permits Issued'),
            color='city:N',
            tooltip=['city', 'week', 'count()']
        ).properties(height=300).interactive(bind_y=False)
        st.altair_chart(line_vol, use_container_width=True)

with col_vel:
    st.subheader("🐢 Weekly Velocity (Speed)")
    chart_df = df.dropna(subset=['issue_date', 'velocity'])
    chart_df = chart_df[chart_df['velocity'] >= 0]
    
    if not chart_df.empty:
        chart_df['week'] = chart_df['issue_date'].dt.to_period('W').apply(lambda r: r.start_time)
        line_vel = alt.Chart(chart_df).mark_line(point=True).encode(
            x=alt.X('week:T', title='Week Of', axis=alt.Axis(format='%b %d')),
            y=alt.Y('median(velocity):Q', title='Median Days'),
            color='city:N',
            tooltip=['city', 'week', 'median(velocity)']
        ).properties(height=300).interactive(bind_y=False)
        st.altair_chart(line_vel, use_container_width=True)
    else:
        st.info("No velocity data yet (Missing 'Applied Date').")

st.divider()

# --- PIE CHART & TABLE ---
c_pie, c_table = st.columns([1, 2])

with c_pie:
    st.subheader("🏷️ Permit Mix")
    base = alt.Chart(df).encode(theta=alt.Theta("count():Q", stack=True))
    pie = base.mark_arc(outerRadius=120, innerRadius=50).encode(
        color=alt.Color("complexity_tier:N"),
        order=alt.Order("complexity_tier", sort="ascending"),
        tooltip=["complexity_tier", "count()"]
    )
    st.altair_chart(pie, use_container_width=True)

with c_table:
    st.subheader("📋 Recent Permit Manifest")
    st.dataframe(
        df[['city', 'complexity_tier', 'valuation', 'velocity', 'description', 'issue_date']]
        .sort_values('issue_date', ascending=False)
        .head(100),
        use_container_width=True,
        height=300
    )
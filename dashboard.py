import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

st.set_page_config(layout="wide", page_title="Vectis Command Console")
st.markdown("""<style>.metric-card { background-color: #F0F4F8; border-left: 5px solid #C87F42; padding: 15px; }</style>""", unsafe_allow_html=True)

# --- PERMISSIVE DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 1. Fetch ALL Data
        response = supabase.table('permits').select("*").execute()
        if not response.data: return pd.DataFrame()
        df = pd.DataFrame(response.data)

        # 2. Rename & Type Convert
        if 'issued_date' in df.columns: df = df.rename(columns={'issued_date': 'issue_date'})
        
        if 'issue_date' in df.columns: df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        if 'applied_date' in df.columns: df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')

        # 3. Velocity Logic
        if 'processing_days' in df.columns:
            df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
            df['velocity'] = None

        # Fallback calc
        mask = df['velocity'].isna() & df['issue_date'].notna() & df['applied_date'].notna()
        df.loc[mask, 'velocity'] = (df.loc[mask, 'issue_date'] - df.loc[mask, 'applied_date']).dt.days

        # 4. Cleanup
        if 'complexity_tier' not in df.columns: df['complexity_tier'] = 'Unknown'
        df['complexity_tier'] = df['complexity_tier'].fillna('Unknown')
        
        if 'valuation' in df.columns:
            df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        else:
            df['valuation'] = 0

        return df

    except Exception as e:
        st.error(f"Data Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.caption("v5.1 | FIX: ALTAIR SYNTAX")

if df.empty:
    st.error("Database returned 0 records.")
    st.stop()

# Filter Controls
all_tiers = sorted(df['complexity_tier'].unique().tolist())
selected_tiers = st.sidebar.multiselect("Permit Class", all_tiers, default=all_tiers)
show_bad_data = st.sidebar.checkbox("Show Records with Missing Dates?", value=True)

if show_bad_data:
    df_filtered = df[df['complexity_tier'].isin(selected_tiers)]
else:
    df_filtered = df[df['complexity_tier'].isin(selected_tiers)]
    df_filtered = df_filtered.dropna(subset=['velocity'])
    df_filtered = df_filtered[df_filtered['velocity'] > 0]

# --- CHARTS (Fixed Schema) ---
st.markdown("### 📉 Velocity Trends")

if 'issue_date' in df_filtered.columns:
    chart_df = df_filtered.dropna(subset=['issue_date', 'velocity'])
    chart_df = chart_df[chart_df['velocity'] > 0] 
    
    if not chart_df.empty:
        # Create explicit Month column
        chart_df['month'] = chart_df['issue_date'].dt.to_period('M').apply(lambda r: r.start_time)
        trend = chart_df.groupby(['city', 'month'])['velocity'].median().reset_index()
        
        # FIX: Strict Schema Compliance for Altair
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('month:T', title='Month', axis=alt.Axis(format='%b %Y')), # Explicit Type :T and correct Axis param
            y=alt.Y('velocity:Q', title='Median Days'), # Explicit Type :Q
            color=alt.Color('city:N', legend=alt.Legend(title="Jurisdiction")), # Explicit Type :N
            tooltip=['city', 'month', 'velocity']
        ).interactive()
        
        st.altair_chart(line, use_container_width=True)
    else:
        st.warning("Filters active, but no records have valid Dates + Velocity to plot.")

# --- DATA TABLE ---
st.markdown("### 📋 Data Inspection")
st.dataframe(
    df_filtered[['city', 'complexity_tier', 'issue_date', 'velocity']].sort_values('issue_date', ascending=False).head(50),
    use_container_width=True
)

# --- FOOTER ---
st.divider()
c1, c2, c3 = st.columns(3)
with c1: st.metric("Total Records", len(df_filtered))
with c2: st.metric("Records with Velocity", len(df_filtered.dropna(subset=['velocity'])))
with c3: st.metric("Missing/Bad Data", len(df_filtered) - len(df_filtered.dropna(subset=['velocity'])))
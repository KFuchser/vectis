import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Vectis Command Console",
    layout="wide",
    initial_sidebar_state="expanded"
)

# The "Vectis Tax" Basis ($500/day)
DAILY_CARRY_COST = 500 

# --- CSS INJECTION ---
st.markdown("""
    <style>
    .metric-card {
        background-color: #F0F4F8; 
        border-left: 5px solid #C87F42;
        padding: 15px;
        border-radius: 5px;
    }
    .big-stat {
        font-family: 'Inter Tight', sans-serif; 
        font-size: 26px; 
        font-weight: 700; 
        color: #1C2B39;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATA FACTORY ---
@st.cache_data
def load_data():
    # Mock data for MVP stability
    data = [
        {'city': 'Fort Worth', 'permit_id': 'FW-001', 'velocity_days': 14, 'complexity_tier': 'Standard', 'valuation': 120000, 'issue_date': '2025-12-01'},
        {'city': 'Fort Worth', 'permit_id': 'FW-002', 'velocity_days': 12, 'complexity_tier': 'Strategic', 'valuation': 5000000, 'issue_date': '2025-12-05'},
        {'city': 'Fort Worth', 'permit_id': 'FW-003', 'velocity_days': 45, 'complexity_tier': 'Commodity', 'valuation': 5000, 'issue_date': '2025-11-20'},
        {'city': 'Fort Worth', 'permit_id': 'FW-004', 'velocity_days': 18, 'complexity_tier': 'Standard', 'valuation': 85000, 'issue_date': '2025-12-10'},
        
        {'city': 'San Antonio', 'permit_id': 'SA-101', 'velocity_days': 110, 'complexity_tier': 'Standard', 'valuation': 150000, 'issue_date': '2025-10-15'},
        {'city': 'San Antonio', 'permit_id': 'SA-102', 'velocity_days': 180, 'complexity_tier': 'Strategic', 'valuation': 4500000, 'issue_date': '2025-09-01'},
        {'city': 'San Antonio', 'permit_id': 'SA-103', 'velocity_days': 95, 'complexity_tier': 'Standard', 'valuation': 75000, 'issue_date': '2025-11-05'},
        {'city': 'San Antonio', 'permit_id': 'SA-104', 'velocity_days': 60, 'complexity_tier': 'Commodity', 'valuation': 4000, 'issue_date': '2025-12-12'},

        {'city': 'Austin', 'permit_id': 'AU-555', 'velocity_days': 45, 'complexity_tier': 'Standard', 'valuation': 200000, 'issue_date': '2025-11-25'},
        {'city': 'Austin', 'permit_id': 'AU-556', 'velocity_days': 65, 'complexity_tier': 'Strategic', 'valuation': 8000000, 'issue_date': '2025-11-10'},
        {'city': 'Austin', 'permit_id': 'AU-557', 'velocity_days': 30, 'complexity_tier': 'Standard', 'valuation': 50000, 'issue_date': '2025-12-02'},
    ]
    df = pd.DataFrame(data)
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    return df

df = load_data()

# --- SIDEBAR ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.markdown("`v1.0.5 | STATUS: LIVE`")
st.sidebar.divider()

st.sidebar.header("Signal Filters")

tier_options = df['complexity_tier'].unique().tolist()
# Filter out Commodity by default to remove noise
default_tiers = [t for t in tier_options if t != 'Commodity']

selected_tiers = st.sidebar.multiselect(
    "Complexity Tranche",
    options=tier_options,
    default=default_tiers
)

df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

# --- HERO SECTION: BUREAUCRACY LEADERBOARD ---
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("#### *The Cost of Delay (Market Velocity)*")

# Calculation Logic
leaderboard = df_filtered.groupby('city')['velocity_days'].median().reset_index()
leaderboard.columns = ['Jurisdiction', 'Median Days']

benchmark_speed = leaderboard['Median Days'].min()
leaderboard['Benchmark Delta'] = leaderboard['Median Days'] - benchmark_speed
leaderboard['Implied Tax'] = leaderboard['Benchmark Delta'] * DAILY_CARRY_COST
leaderboard = leaderboard.sort_values('Median Days')

st.dataframe(
    leaderboard,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Jurisdiction": st.column_config.TextColumn("Jurisdiction"),
        "Median Days": st.column_config.NumberColumn("Median Velocity", format="%d days"),
        "Benchmark Delta": st.column_config.NumberColumn("Delay vs Best", format="+%d days"),
        "Implied Tax": st.column_config.ProgressColumn(
            "The Cost of Delay ($)",
            format="$%d",
            min_value=0,
            max_value=int(leaderboard['Implied Tax'].max() * 1.25),
        ),
    }
)

st.caption(f"Benchmark Basis: Fastest jurisdiction. Cost Basis: ${DAILY_CARRY_COST}/day.")
st.divider()

# --- CHARTS SECTION ---
c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("### 📉 Velocity Trends")
    chart_data = df_filtered.groupby(['city', 'issue_date'])['velocity_days'].mean().reset_index()
    
    chart = alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X('issue_date', title='Issuance Week', axis=alt.Axis(format='%b %d')),
        y=alt.Y('velocity_days', title='Days to Issue'),
        color=alt.Color('city', scale={'range': ['#1C2B39', '#C87F42', '#A3A3A3']}, legend=alt.Legend(title="Jurisdiction")),
        tooltip=['city', 'velocity_days', 'issue_date']
    ).properties(height=350)
    
    st.altair_chart(chart, use_container_width=True)

with c2:
    st.markdown("### 📊 Market Mix")
    mix_data = df_filtered['complexity_tier'].value_counts().reset_index()
    mix_data.columns = ['Tier', 'Count']
    
    pie = alt.Chart(mix_data).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="Count", type="quantitative"),
        color=alt.Color(field="Tier", type="nominal", scale={'range': ['#C87F42', '#1C2B39', '#8899A6']}),
        tooltip=['Tier', 'Count']
    ).properties(height=350)
    st.altair_chart(pie, use_container_width=True)

# --- FOOTER METRICS ---
st.divider()
st.markdown("### 📡 Live Feed Status")

m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Active Permits", f"{len(df_filtered)}")
with m2:
    val_millions = df_filtered['valuation'].sum() / 1_000_000
    st.metric("Pipeline Value", f"${val_millions:.1f}M")
with m3:
    risk_std = df_filtered['velocity_days'].std()
    st.metric("Risk Index (σ)", f"±{risk_std:.0f} Days")
with m4:
    st.metric("Data Freshness", "Live Stream")
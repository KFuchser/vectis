with col_r:
    st.subheader("📊 Complexity Tier")
    
    # Filtering out "Unknown" to focus on classified data
    valid_tiers = filtered[filtered['complexity_tier'] != "Unknown"]
    
    if not valid_tiers.empty:
        tier_counts = valid_tiers['complexity_tier'].value_counts().reset_index()
        tier_counts.columns = ['tier', 'count']
        
        # DEFINING THE STRATEGIC COLOR SCALE
        # This aligns the dashboard with Architectural Zoning Standards
        color_scale = alt.Scale(
            domain=['Strategic', 'Residential', 'Standard', 'Commodity'],
            range=[VECTIS_BRONZE, '#F2C94C', VECTIS_BLUE, '#A0A0A0']
        )
        
        pie = alt.Chart(tier_counts).mark_arc(outerRadius=100, innerRadius=50).encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(
                field="tier", 
                type="nominal", 
                scale=color_scale,
                legend=alt.Legend(title="Tier Type", orient="bottom")
            ),
            tooltip=['tier', 'count']
        ).properties(height=350)
        
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("Pending AI Classification...")
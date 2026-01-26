"""
Ingestion spoke for Austin, TX.
Connects to the City of Austin's Socrata open data portal to fetch recently applied-for or issued building permits.
"""
import os
import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord, ComplexityTier, ProjectCategory

# --- CONFIG ---
AUSTIN_API_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"

def get_austin_data(app_token, cutoff_date):
    """
    Fetches Austin permits via Socrata API.
    CRITICAL FIX: Filters by 'applieddate' instead of 'issue_date' to capture active pipeline.
    """
    print(f"🤠 Fetching Austin data since {cutoff_date}...")
    
    # SOCRATA QUERY (SoQL)
    # 1. Filter by applieddate >= cutoff (New applications)
    # 2. OR filter by issue_date >= cutoff (Recently issued legacy apps)
    # 3. Limit 5000 to prevent timeouts (Process in chunks if needed)
    params = {
        "$where": f"applieddate >= '{cutoff_date}T00:00:00' OR issue_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "applieddate DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(AUSTIN_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print("⚠️ No Austin data returned from API.")
            return []
            
        print(f"✅ Retrieved {len(data)} raw records from Austin.")
        
        records = []
        for item in data:
            # MAPPING LOGIC
            # Socrata Field -> Vectis Schema
            # applieddate -> applied_date
            # issue_date -> issued_date
            # permit_number -> permit_id
            
            # 1. Parse Dates (Handle Socrata's Floating Timestamp)
            applied = item.get("applieddate", "").split("T")[0] if item.get("applieddate") else None
            issued = item.get("issue_date", "").split("T")[0] if item.get("issue_date") else None
            
            # 2. Construct Record
            r = PermitRecord(
                city="Austin",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=item.get("description") or item.get("work_class") or "No Description",
                # Austin Valuation is often missing/zero, default to 0.0
                valuation=float(item.get("valuation", 0.0) or 0.0),
                status=item.get("status_current", "Unknown")
            )
            records.append(r)
            
        return records

    except Exception as e:
        print(f"❌ Austin API Error: {e}")
        return []
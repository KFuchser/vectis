"""
Ingestion spoke for Fort Worth, TX.
Connects to the City of Fort Worth's ArcGIS REST API to fetch recent development permits.
"""
"""
Ingestion spoke for Fort Worth, TX.
Connects to the City of Fort Worth's ArcGIS REST API to fetch recent development permits.
"""
import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_fort_worth_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches Fort Worth permits via ArcGIS REST API.
    Now fetches 'Applied_Date' to ensure Velocity (Lead Time) can be calculated.
    """
    print(f"🤠 Starting Fort Worth Sync (Cutoff: {cutoff_date})...")
    
    # 1. Setup Endpoint
    url = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"
    
    # 2. Build Query Parameters
    # ADDED 'Applied_Date' to outFields
    params = {
        "where": f"Status_Date >= '{cutoff_date} 00:00:00'",
        "outFields": "Permit_No,Status_Date,Applied_Date,Current_Status,B1_WORK_DESC,JobValue,Permit_Type,Addr_No,Street_Name,Zip_Code",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": 2000, 
        "orderByFields": "Status_Date DESC"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "features" not in data or not data["features"]:
            print(f"⚠️ Fort Worth: No records found since {cutoff_date}.")
            return []

        # 3. Parse Raw Data
        raw_records = [f["attributes"] for f in data["features"]]
        
        # 4. Convert to PermitRecord Objects
        mapped_records = []
        
        for r in raw_records:
            # Helper to convert Epoch MS to ISO Date
            def parse_arcgis_date(ms):
                try:
                    if ms:
                        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
                except:
                    pass
                return None

            # Handle Dates
            issued_iso = parse_arcgis_date(r.get('Status_Date'))
            applied_iso = parse_arcgis_date(r.get('Applied_Date')) # Now capturing start time

            # Handle Description
            desc = r.get('B1_WORK_DESC', '')
            if not desc:
                desc = r.get('Permit_Type', 'Unspecified Permit')

            # Create the PermitRecord
            record = PermitRecord(
                permit_id=str(r.get('Permit_No')),
                city="Fort Worth",
                status=r.get('Current_Status', 'Unknown'),
                applied_date=applied_iso, # Critical for Velocity Calculation
                issued_date=issued_iso,
                description=desc,
                valuation=float(r.get('JobValue') or 0.0),
                # Note: 'job_class' isn't in your main PermitRecord model, using project_category downstream
                contractor="Unknown", 
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ Fort Worth: Retrieved & Converted {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ Fort Worth API Error: {e}")
        return []
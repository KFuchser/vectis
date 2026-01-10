import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord, ComplexityTier # Links to your shared models

def get_fort_worth_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches Fort Worth permits via ArcGIS REST API.
    Returns: List[PermitRecord] to match Vectis pipeline standards.
    """
    print(f"🤠 Starting Fort Worth Sync (Cutoff: {cutoff_date})...")
    
    # 1. Setup Endpoint
    url = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"
    
    # 2. Build Query Parameters
    # Convert 'YYYY-MM-DD' to standard SQL timestamp for ArcGIS
    # Note: ArcGIS REST is picky about date formats in queries
    params = {
        "where": f"Status_Date >= '{cutoff_date} 00:00:00'",
        "outFields": "Permit_No,Status_Date,Current_Status,B1_WORK_DESC,JobValue,Permit_Type,Addr_No,Street_Name,Zip_Code",
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
            # Handle Dates (Epoch MS to YYYY-MM-DD)
            try:
                # ArcGIS returns Epoch Milliseconds
                dt = datetime.fromtimestamp(r.get('Status_Date', 0) / 1000.0)
                iso_date = dt.strftime('%Y-%m-%d')
            except:
                iso_date = cutoff_date # Fallback

            # Handle Address Construction
            addr = f"{r.get('Addr_No', '')} {r.get('Street_Name', '')}".strip()
            
            # Handle Description (Crucial for AI)
            desc = r.get('B1_WORK_DESC', '')
            if not desc:
                desc = r.get('Permit_Type', 'Unspecified Permit')

            # Create the PermitRecord
            # We map ArcGIS columns to your Pydantic model
            record = PermitRecord(
                permit_id=str(r.get('Permit_No')),
                city="Fort Worth",
                status=r.get('Current_Status', 'Unknown'),
                issued_date=iso_date,
                description=desc,
                valuation=float(r.get('JobValue') or 0.0),
                job_class=r.get('Permit_Type', 'Unknown'),
                contractor="Unknown", # ArcGIS view doesn't expose contractor easily
                latitude=0.0, # Not strictly needed for MVP
                longitude=0.0,
                complexity_tier=ComplexityTier.UNKNOWN # AI will fill this later
            )
            mapped_records.append(record)
        
        print(f"✅ Fort Worth: Retrieved & Converted {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ Fort Worth API Error: {e}")
        return []
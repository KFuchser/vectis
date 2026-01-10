import requests
import pandas as pd
from datetime import datetime
import logging
from service_models import PermitRecord, ComplexityTier

# Setup Module Logging
logger = logging.getLogger(__name__)

# The "Golden Source" URL (Verified)
# Note: We use the '/query' endpoint directly
BASE_URL = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"

def get_fort_worth_data(cutoff_date_str: str) -> list[PermitRecord]:
    """
    Fetches permits from Fort Worth ArcGIS (CIVIC Layer).
    Args:
        cutoff_date_str: Date string in 'YYYY-MM-DD' format.
    Returns:
        List of PermitRecord objects.
    """
    logger.info(f"🤠 Fetching Fort Worth data (Threshold: {cutoff_date_str})...")
    
    # 1. Convert Cutoff String to Unix Timestamp (ArcGIS expects milliseconds)
    try:
        dt_obj = datetime.strptime(cutoff_date_str, "%Y-%m-%d")
        cutoff_ms = int(dt_obj.timestamp() * 1000)
    except ValueError:
        # Fallback if format is weird
        cutoff_ms = 0
    
    all_features = []
    offset = 0
    batch_size = 1000 # ArcGIS Max Limit
    
    while True:
        # ArcGIS SQL Query
        params = {
            "where": f"File_Date >= {cutoff_ms}", 
            "outFields": "Permit_No,B1_WORK_DESC,File_Date,Current_Status,Permit_Type,JobValue,Address,Status_Date",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "orderByFields": "File_Date DESC"
        }

        try:
            r = requests.get(BASE_URL, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error(f"💥 FW Connection Failed: {e}")
            break

        features = data.get("features", [])
        if not features:
            break
            
        all_features.extend([f["attributes"] for f in features])
        print(f"   -> Fetched batch: {len(features)} records...")
        
        if len(features) < batch_size:
            break
        
        offset += batch_size

    if not all_features:
        return []

    # 2. Process to Pandas for cleanup
    df = pd.DataFrame(all_features)
    
    # 3. Clean & Map Columns
    valid_records = []
    
    for _, row in df.iterrows():
        # A. Map Fields
        pid = row.get("Permit_No")
        desc = row.get("B1_WORK_DESC")
        status = row.get("Current_Status")
        val = row.get("JobValue")
        
        # B. Handle Dates (Unix MS -> Date Object)
        try:
            file_date_ms = row.get("File_Date")
            filing_date = datetime.fromtimestamp(file_date_ms / 1000).date() if file_date_ms else None
            
            # Logic Fix: Negative Duration / "Time Travel"
            # If Status Date (Completion) is before File Date, swap them?
            # For this simple ingestion, we just ensure filing_date is valid.
        except:
            filing_date = None

        if not pid or not filing_date or not desc:
            continue

        # C. Create PermitRecord Object
        # Note: We default complexity to UNKNOWN; the Manager script handles the AI batching.
        record = PermitRecord(
            permit_id=str(pid),
            city="Fort Worth",
            description=str(desc),
            filing_date=filing_date,
            status=str(status) if status else "Unknown",
            valuation=float(val) if val else 0.0,
            complexity_tier=ComplexityTier.UNKNOWN,
            ai_rationale=""
        )
        valid_records.append(record)

    return valid_records
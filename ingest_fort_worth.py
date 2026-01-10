import requests
import pandas as pd
import logging
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - FW_WORKER - %(message)s')
logger = logging.getLogger(__name__)

# 🚨 VERIFIED ENDPOINT (Fort Worth Open Data - ArcGIS Online)
# Confirmed live data as of Jan 2026
BASE_URL = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"

def get_fort_worth_data(cutoff_date_str: str) -> list[PermitRecord]:
    logger.info(f"🤠 Fetching Fort Worth data (Official) since {cutoff_date_str}...")
    
    # 1. Date Conversion: String -> Unix Milliseconds
    try:
        dt_obj = datetime.strptime(cutoff_date_str, "%Y-%m-%d")
        cutoff_ms = int(dt_obj.timestamp() * 1000)
    except ValueError:
        logger.error(f"❌ Invalid date format: {cutoff_date_str}")
        return []

    all_features = []
    offset = 0
    batch_size = 1000 
    
    while True:
        # 2. Query Parameters
        # We use outFields='*' to grab everything available (Status, Valuation, etc.)
        # without guessing specific field names that might crash the query.
        params = {
            "where": f"File_Date >= {cutoff_ms}",
            "outFields": "*", 
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "orderByFields": "File_Date DESC"
        }

        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            if "error" in data:
                logger.error(f"❌ API Error: {data['error']}")
                break
        except Exception as e:
            logger.error(f"💥 Connection Failed: {e}")
            break

        features = data.get("features", [])
        if not features:
            break
            
        all_features.extend([f["attributes"] for f in features])
        logger.info(f"   -> Fetched batch: {len(features)} records...")
        
        if len(features) < batch_size:
            break
        
        offset += batch_size

    if not all_features:
        logger.warning("⚠️ No records found.")
        return []

    # 3. Process Data
    df = pd.DataFrame(all_features)
    valid_records = []
    
    for _, row in df.iterrows():
        try:
            # Map Verified Columns (from your test)
            pid = row.get("Permit_No")
            desc = row.get("B1_WORK_DESC")
            date_ms = row.get("File_Date")
            
            # Try to grab Status/Valuation if they exist in the '*' return
            # Common ArcGIS keys for these fields:
            status = row.get("Status") or row.get("STATUS") or "Unknown"
            val = row.get("JobValue") or row.get("JOB_VALUE") or row.get("Estimated_Cost") or 0.0

            if pid and date_ms:
                filing_date = datetime.fromtimestamp(date_ms / 1000).date()
                
                # Cleanup Description
                # (Your test showed the desc sometimes equals the column name, we keep it as is for now)
                final_desc = str(desc) if desc else "No Description Available"
                
                record = PermitRecord(
                    permit_id=str(pid),
                    city="Fort Worth",
                    description=final_desc,
                    filing_date=filing_date,
                    status=str(status),
                    valuation=float(val),
                    complexity_tier=ComplexityTier.UNKNOWN,
                    ai_rationale=""
                )
                valid_records.append(record)
                
        except Exception:
            continue

    logger.info(f"✅ Parsed {len(valid_records)} valid Fort Worth permits.")
    return valid_records

# --- 🧪 LOCAL TEST HARNESS ---
if __name__ == "__main__":
    print("\n--- 🧪 STARTING FINAL INTEGRATION TEST ---")
    # Test with a known recent date (Jan 1, 2026)
    results = get_fort_worth_data("2026-01-01")
    
    if results:
        print(f"✅ Success! Found {len(results)} records.")
        print(f"Sample ID: {results[0].permit_id}")
        print(f"Sample Date: {results[0].filing_date}")
        print(f"Sample Desc: {results[0].description}")
    else:
        print("No results found.")
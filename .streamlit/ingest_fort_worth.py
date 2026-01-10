import requests
import pandas as pd
import logging
from datetime import datetime, timedelta

# Import the Intelligence Layer we built earlier
from service_models import VectisClassifier

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FortWorthPipeline:
    """
    Production Pipeline for Fort Worth (ArcGIS Source).
    Bypasses Socrata. Targets CIVIC/Permits/MapServer/0.
    """
    BASE_URL = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    
    def __init__(self):
        self.classifier = VectisClassifier()

    def fetch_latest_permits(self, days_back: int = 30) -> pd.DataFrame:
        """
        Extracts raw data using the verified schema.
        """
        all_features = []
        offset = 0
        batch_size = 1000  # ArcGIS Hard Limit
        
        # Query Logic: Get permits filed in the last X days
        # Note: We query the DB, not the API, for date math if possible, 
        # but ArcGIS REST often requires specific syntax.
        # We will fetch roughly by date if the server supports it, or filter in pandas.
        # Given the "dirty data" risk, we'll fetch wider and filter in Python.
        
        logger.info(f"🛰️ Connecting to Fort Worth CIVIC Layer (Last {days_back} days)...")

        while True:
            params = {
                "where": "1=1", # Grab all, we filter by date in Python to be safe against timezone bugs
                "outFields": "Permit_No,B1_WORK_DESC,File_Date,Status_Date,Current_Status,Permit_Type,JobValue,Address", 
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
                "orderByFields": "File_Date DESC"
            }
            
            try:
                r = requests.get(self.BASE_URL, params=params, timeout=20)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                logger.error(f"💥 Connection Failed: {e}")
                break

            features = data.get("features", [])
            if not features:
                break
            
            # Extract attributes
            batch_df = pd.DataFrame([f["attributes"] for f in features])
            
            # --- DATE FILTERING (Do it here to stop pagination early) ---
            # Convert File_Date to datetime to check age
            if "File_Date" in batch_df.columns:
                batch_df["temp_date"] = pd.to_datetime(batch_df["File_Date"], unit="ms", errors="coerce")
                cutoff_date = datetime.now() - timedelta(days=days_back)
                
                # If the *newest* record in this batch is older than our cutoff, we stop.
                if batch_df["temp_date"].max() < cutoff_date:
                    logger.info("   Reached time horizon. Stopping fetch.")
                    all_features.extend(batch_df[batch_df["temp_date"] >= cutoff_date].to_dict('records'))
                    break
            
            all_features.extend(batch_df.to_dict('records'))
            logger.info(f"   Fetched batch: {len(batch_df)} records...")
            
            if len(features) < batch_size:
                break
            
            offset += batch_size

        df = pd.DataFrame(all_features)
        return self._clean_and_normalize(df)

    def _clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The "Quality Lock": Maps Schema, Fixes Dates, Filters Junk.
        """
        if df.empty:
            return df

        # 1. Schema Mapping (Fort Worth -> Vectis)
        df.rename(columns={
            "Permit_No": "permit_id",
            "B1_WORK_DESC": "raw_description",
            "File_Date": "created_at",
            "Status_Date": "completed_at", # We treat status date as completion for now
            "Current_Status": "status",
            "Permit_Type": "permit_type",
            "JobValue": "valuation",
            "Address": "address"
        }, inplace=True)

        # 2. Fix Timestamps (Unix ms -> DateTime)
        for col in ["created_at", "completed_at"]:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

        # 3. LOGIC FIX: "Time Travel" (Negative Durations)
        # If Completed < Created, swap them.
        dirty_mask = (df["completed_at"].notna()) & (df["completed_at"] < df["created_at"])
        if dirty_mask.any():
            logger.warning(f"⚠️ Found {dirty_mask.sum()} records with Negative Duration. Swapping dates.")
            df.loc[dirty_mask, ["created_at", "completed_at"]] = df.loc[dirty_mask, ["completed_at", "created_at"]].values

        # 4. Calculate Duration (Cycle Time)
        df["duration_days"] = (df["completed_at"] - df["created_at"]).dt.days

        # 5. Cost Saving Filter (Regex)
        # Drop rows where description is empty OR it's a routine trade permit (Roof, Water Heater)
        initial_len = len(df)
        df = df[df["raw_description"].notna()] # Drop nulls
        
        # Regex for "Commodity" permits (Don't waste AI tokens on these)
        junk_regex = "ROOF|FENCE|IRRIGATION|WATER HEATER|SIDING|WINDOW|DRIVEWAY"
        df = df[~df["raw_description"].str.contains(junk_regex, case=False, regex=True)]
        
        logger.info(f"📉 Filtered garbage. Kept {len(df)}/{initial_len} rows for AI processing.")

        return df

    def enrich_with_ai(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sends the Clean Data to Gemini for Classification.
        """
        logger.info(f"🧠 Processing {len(df)} permits through Intelligence Layer...")
        
        results = []
        for idx, row in df.iterrows():
            # HEURISTIC OPTIMIZATION:
            # If Valuation > $1M, force "Strategic" flag (Save AI reasoning effort)
            is_whale = (row["valuation"] or 0) > 1_000_000
            
            # Call AI
            ai_output = self.classifier.classify_permit(row["raw_description"])
            
            results.append({
                "permit_id": row["permit_id"],
                "vectis_class": ai_output["complexity_tier"],
                # If it's a whale, override the AI's strategic flag to True
                "is_strategic": True if is_whale else ai_output["is_strategic"],
                "risk_flags": ai_output["risk_flags"],
                "clean_desc": ai_output["standardized_description"]
            })

        # Merge AI results back to main Dataframe
        ai_df = pd.DataFrame(results)
        return pd.merge(df, ai_df, on="permit_id", how="left")

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    pipeline = FortWorthPipeline()
    
    # 1. Fetch (Last 7 Days)
    raw_df = pipeline.fetch_latest_permits(days_back=7)
    
    if not raw_df.empty:
        # 2. Enrich (Process top 5 for demo)
        print(f"--- FETCH SUCCESS: {len(raw_df)} Clean Records ---")
        enriched_df = pipeline.enrich_with_ai(raw_df.head(5))
        
        # 3. Display
        display_cols = ["permit_id", "vectis_class", "is_strategic", "duration_days", "clean_desc"]
        print(enriched_df[display_cols].to_markdown(index=False))
    else:
        print("No records found.")
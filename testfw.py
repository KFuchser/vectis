import requests
import pandas as pd
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - DEBUG - %(message)s')
logger = logging.getLogger(__name__)

class FortWorthDebug:
    # 1. THE FIX: Explicitly defined string, simplified structure
    BASE_URL = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    
    def run_diagnostics(self):
        logger.info("🔍 DIAGNOSTIC: Checking URL String...")
        
        # 2. THE CHECK: Print the raw representation to see hidden \n or \r
        logger.info(f"   Raw URL: {repr(self.BASE_URL)}")
        
        if "\n" in self.BASE_URL or " " in self.BASE_URL:
            logger.error("❌ FOUND HIDDEN 'GREMLIN' CHARACTERS IN URL!")
            self.BASE_URL = self.BASE_URL.strip()
            logger.info("   ✅ Cleaned URL.")
            
        self.fetch_sample()

    def fetch_sample(self):
        logger.info("🚀 Attempting connection...")
        
        params = {
            "where": "1=1",
            "outFields": "Permit_No,B1_WORK_DESC,File_Date",
            "resultRecordCount": 3,
            "f": "json"
        }
        
        try:
            r = requests.get(self.BASE_URL, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            
            if "error" in data:
                logger.error(f"❌ API Error: {data['error']}")
            else:
                features = data.get("features", [])
                logger.info(f"✅ SUCCESS! Connection verified. Fetched {len(features)} records.")
                if features:
                    print("\n--- SAMPLE DATA ---")
                    print(features[0]['attributes'])
                    
        except requests.exceptions.InvalidURL:
            logger.error("❌ CRITICAL: 'Invalid URL' Exception still active. Check your Python environment.")
        except Exception as e:
            logger.error(f"💥 Connection Failed: {e}")

if __name__ == "__main__":
    debugger = FortWorthDebug()
    debugger.run_diagnostics()
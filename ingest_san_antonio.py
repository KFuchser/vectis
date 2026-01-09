import requests
import io
import pandas as pd
from service_models import PermitRecord

def get_san_antonio_data(threshold):
    print("\n--- 🛰️ SAN ANTONIO SPOKE (CSV) ---")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        # Fetch Metadata to get the latest CSV URL
        meta_resp = requests.get("https://data.sanantonio.gov/api/3/action/resource_show?id=c21106f9-3ef5-4f3a-8604-f992b4db7512", headers=headers, timeout=30)
        csv_url = meta_resp.json()['result']['url']
        file_resp = requests.get(csv_url, headers=headers, timeout=120)
        df = pd.read_csv(io.BytesIO(file_resp.content))
        
        # Standardize Columns
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Filter by Date using Pandas first for speed
        df['date_issued'] = pd.to_datetime(df['date_issued'], errors='coerce')
        df = df[df['date_issued'] >= pd.Timestamp(threshold)].copy()

        cleaned_records = []
        for _, row in df.iterrows():
            try:
                # Map to Pydantic
                permit = PermitRecord(
                    city="San Antonio",
                    permit_id=str(row.get('permit_number') or row.get('permit_num')),
                    applied_date=row.get('date_submitted'),
                    issued_date=row.get('date_issued'),
                    description=str(row.get('permit_description', '')),
                    valuation=float(row.get('declared_valuation', 0)),
                    status="Issued"
                )
                cleaned_records.append(permit)
            except: continue
        return cleaned_records
    except Exception as e:
        print(f"!! San Antonio Spoke Failed: {e}")
        return []
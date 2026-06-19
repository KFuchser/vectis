"""
Ingestion spoke for Los Angeles, CA (Socrata API).

This module handles fetching and normalizing building permit data from the City of Los Angeles.
Endpoint: https://data.lacity.org/resource/pi9x-tg5x.json

Key Logic:
- Timeout: Set to 60 seconds because the LA Socrata endpoint is historically slow.
- Schema Limitation: The source does not reliably publish application dates, so `applied_date` is set to None.
  This means LA data contributes to Volume metrics but not Velocity (Lead Time) metrics.
"""
import requests
from service_models import PermitRecord, ComplexityTier


def _parse_date(d):
    return d.split("T")[0] if d else None


def get_la_data(cutoff_date, socrata_token=None):
    """
    Fetches and normalizes building permit data from the City of Los Angeles' Socrata API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.
        socrata_token: The Socrata application token for API authentication.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Fetching LA data (Volume Only)...")
    
    LA_ENDPOINT = "https://data.lacity.org/resource/pi9x-tg5x.json"
    PAGE_SIZE = 1000

    headers = {}
    if socrata_token:
        headers["X-App-Token"] = socrata_token

    all_items = []
    offset = 0

    try:
        while True:
            params = {
                "$select": "permit_nbr,issue_date,valuation,work_desc,status_desc",
                "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "issue_date DESC",
            }
            # LA Socrata is notoriously slow — 60s timeout required.
            resp = requests.get(LA_ENDPOINT, params=params, headers=headers, timeout=60)

            if resp.status_code != 200:
                print(f"LA API Error on page {offset // PAGE_SIZE + 1}: {resp.status_code}")
                break

            page = resp.json()
            if not page:
                break

            all_items.extend(page)

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            print(f"   Los Angeles: fetched {len(all_items)} records so far...")

        if not all_items:
            print("No Los Angeles data returned.")
            return []

        records = []
        for r in all_items:
            issued = _parse_date(r.get("issue_date"))
            val_raw = r.get("valuation", "0")
            try:
                val = float(val_raw)
            except (TypeError, ValueError):
                val = 0.0

            records.append(PermitRecord(
                permit_id=r.get("permit_nbr"),
                city="Los Angeles",
                applied_date=None,  # LA does not publish application dates
                issued_date=issued,
                valuation=val,
                description=r.get("work_desc", "No Description"),
                status=r.get("status_desc", "Issued"),
                complexity_tier=ComplexityTier.UNKNOWN
            ))

        print(f"Los Angeles: Retrieved {len(records)} records (Volume Only).")
        return records

    except Exception as e:
        print(f"LA API Error: {e}")
        return []
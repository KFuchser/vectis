"""
Ingestion spoke for Chicago, IL (Socrata API).

This module handles fetching and normalizing building permit data from the City of Chicago.
Endpoint: https://data.cityofchicago.org/resource/ydr8-5enu.json

Key Logic:
- Sorts by `issue_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_chicago_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of Chicago's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"Chicago: Fetching permits since {cutoff_date}...")

    CHICAGO_API_URL = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
    PAGE_SIZE = 1000

    def parse_date(d):
        return d.split("T")[0] if d else None

    all_items = []
    offset = 0

    try:
        while True:
            params = {
                "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "issue_date DESC",
                "$$app_token": app_token
            }
            response = requests.get(CHICAGO_API_URL, params=params, timeout=30)

            if response.status_code != 200:
                print(f"Chicago API Error on page {offset // PAGE_SIZE + 1}: {response.status_code}")
                break

            page = response.json()
            if not page:
                break

            all_items.extend(page)

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            print(f"   Chicago: fetched {len(all_items)} records so far...")

        if not all_items:
            print("No Chicago data returned.")
            return []

        records = []
        for item in all_items:
            applied = parse_date(item.get("application_start_date"))
            issued = parse_date(item.get("issue_date"))
            desc = item.get("work_description") or "Unspecified"
            try:
                val = float(item.get("estimated_cost", 0.0) or 0.0)
            except (TypeError, ValueError):
                val = 0.0

            r = PermitRecord(
                city="Chicago",
                permit_id=item.get("permit_", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("permit_status", "Issued")
            )
            records.append(r)

        print(f"Chicago: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"Chicago Integration Error: {e}")
        return []

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("SOCRATA_APP_TOKEN")
    cutoff = "2026-03-01"
    results = get_chicago_data(token, cutoff)
    for r in results[:5]:
        print(r)

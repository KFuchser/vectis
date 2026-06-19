"""
Ingestion spoke for Austin, TX (Socrata API).

This module handles fetching and normalizing building permit data from the City of Austin.
Endpoint: https://data.austintexas.gov/resource/3syk-w9eu.json

Key Logic:
- Sorts by `issue_date` DESC. Sorting by `applieddate` was found to hide recent data 
  because application dates can be significantly older than issue dates or null.
- Maps `permit_number` to `permit_id`.
"""
import requests
from service_models import PermitRecord, ComplexityTier


def _parse_date(d):
    return d.split("T")[0] if d else None


def get_austin_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of Austin's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Fetching Austin data since {cutoff_date}...")
    
    AUSTIN_API_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"
    PAGE_SIZE = 1000

    all_items = []
    offset = 0

    try:
        while True:
            # Sort by issue_date DESC — applieddate is often null or years stale.
            params = {
                "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "issue_date DESC",
                "$$app_token": app_token
            }
            response = requests.get(AUSTIN_API_URL, params=params, timeout=30)

            if response.status_code != 200:
                print(f"Austin API Error on page {offset // PAGE_SIZE + 1}: {response.status_code}")
                break

            page = response.json()
            if not page:
                break

            all_items.extend(page)

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            print(f"   Austin: fetched {len(all_items)} records so far...")

        if not all_items:
            print("No Austin data returned.")
            return []

        records = []
        for item in all_items:
            applied = _parse_date(item.get("applieddate"))
            issued = _parse_date(item.get("issue_date"))
            desc = item.get("description") or item.get("work_class") or "Unspecified"
            try:
                val = float(item.get("valuation", 0.0) or 0.0)
            except (TypeError, ValueError):
                val = 0.0

            r = PermitRecord(
                city="Austin",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("status_current", "Issued")
            )
            records.append(r)

        print(f"Austin: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"Austin Integration Error: {e}")
        return []
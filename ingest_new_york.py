"""
Ingestion spoke for New York, NY (Socrata API) using sodapy.

This module handles fetching and normalizing building permit data from the City of New York.
Endpoint: data.cityofnewyork.us — dataset rbx6-tga4 (DOB NOW active permits)

Key Logic:
- Uses sodapy for Socrata API interaction.
- 24-month hard lookback limit to prevent massive historical pulls.
- Paginated at 1,000 records/page via offset loop.
- Maps `approved_date` to `applied_date` (closest available approximation).
"""
from sodapy import Socrata
from service_models import PermitRecord, ComplexityTier
from datetime import datetime, timedelta

def get_new_york_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of New York's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    # Hard limit: no further back than 24 months to avoid massive historical pulls.
    max_history = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    final_cutoff = max(cutoff_date, max_history)

    print(f"New York: Fetching permits since {final_cutoff} (requested: {cutoff_date})...")

    # Dataset ID: rbx6-tga4 (DOB NOW active permits)
    client = Socrata("data.cityofnewyork.us", app_token=app_token)
    where_clause = f"issued_date >= '{final_cutoff}' AND approved_date >= '{final_cutoff}'"
    PAGE_SIZE = 1000

    def parse_date(d):
        return d.split("T")[0] if d else None

    all_items = []
    offset = 0

    try:
        while True:
            page = client.get(
                "rbx6-tga4",
                where=where_clause,
                limit=PAGE_SIZE,
                offset=offset,
                order="issued_date DESC",
            )

            if not page:
                break

            all_items.extend(page)

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            print(f"   New York: fetched {len(all_items)} records so far...")

        if not all_items:
            print("No New York data returned.")
            return []

        records = []
        for item in all_items:
            applied = parse_date(item.get("approved_date"))
            issued = parse_date(item.get("issued_date"))
            desc = item.get("job_description") or "Unspecified"
            try:
                val = float(item.get("estimated_job_costs", 0.0) or 0.0)
            except (TypeError, ValueError):
                val = 0.0

            r = PermitRecord(
                city="New York",
                permit_id=item.get("job_filing_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("permit_status", "Issued")
            )
            records.append(r)

        print(f"New York: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"New York Integration Error: {e}")
        return []

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("SOCRATA_APP_TOKEN")
    # Test with a very old date to verify the 24-month hard limit
    cutoff = "2000-01-01"
    results = get_new_york_data(token, cutoff)
    if results:
        print(f"Earliest record in batch: {results[-1].issued_date}")
        for r in results[:5]:
            print(r)
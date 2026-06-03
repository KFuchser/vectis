"""
Ingestion spoke for San Antonio, TX (CKAN API).

This module handles fetching and normalizing building permit data from the City of San Antonio.
Endpoint: https://data.sanantonio.gov/api/3/action/datastore_search

Key Logic:
- Composite ID: Combines `PERMIT #` and internal `_id` (e.g., "12345_99") to guarantee uniqueness.
  The raw API often returns duplicate permit numbers for sub-tasks, causing database collisions.
- Filters out records with missing issue dates.
- Pagination: Fetches all records in batches to avoid the hard per-request limit.
"""
import requests
from service_models import PermitRecord, ComplexityTier


def parse_date(d_str):
    """Converts an ISO datetime string or plain date string to YYYY-MM-DD, or returns None."""
    return str(d_str).split("T")[0] if d_str else None


def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches and normalizes building permit data from the City of San Antonio's CKAN API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Starting San Antonio Sync (Paginated + Composite ID Mode)...")

    # RESOURCE ID: Permits Issued 2020-Present
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    BATCH_SIZE = 1000
    offset = 0
    mapped_records = []

    while True:
        params = {
            "resource_id": "c21106f9-3ef5-4f3a-8604-f992b4db7512",
            "limit": BATCH_SIZE,
            "offset": offset,
            "sort": "_id desc",
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            if not data.get("success"):
                print(f"❌ San Antonio: API returned success=false at offset {offset}")
                break

            raw_records = data["result"]["records"]

            if not raw_records:
                break  # No more pages

            batch_mapped = 0
            reached_cutoff = False

            for r in raw_records:
                # DATES
                issued_iso = parse_date(r.get("DATE ISSUED"))
                applied_iso = parse_date(r.get("DATE SUBMITTED"))

                # Skip records with no issue date
                if not issued_iso:
                    continue

                # Records are sorted newest-first. Once we pass the cutoff we can stop.
                if issued_iso < cutoff_date:
                    reached_cutoff = True
                    break

                # CRITICAL FIX: The API often returns duplicate permit numbers for sub-tasks.
                # A composite ID (Permit # + Internal ID) is required to prevent data loss.
                permit_no = str(r.get("PERMIT #", "UNKNOWN"))
                internal_id = str(r.get("_id"))
                unique_pid = f"{permit_no}_{internal_id}"

                # VALUATION: The API returns valuation as a currency string (e.g., "$5,000.00").
                raw_val = r.get("DECLARED VALUATION")
                try:
                    val = float(str(raw_val).replace("$", "").replace(",", "")) if raw_val else 0.0
                except (ValueError, TypeError):
                    val = 0.0

                record = PermitRecord(
                    permit_id=unique_pid,
                    city="San Antonio",
                    status="Issued",
                    applied_date=applied_iso,
                    issued_date=issued_iso,
                    description=r.get("PROJECT NAME") or r.get("WORK TYPE") or "Unspecified",
                    valuation=val,
                    complexity_tier=ComplexityTier.UNKNOWN,
                )
                mapped_records.append(record)
                batch_mapped += 1

            print(f"   ...fetched batch at offset {offset}: {batch_mapped} records added "
                  f"(total so far: {len(mapped_records)})")

            # Stop paginating if we've passed the cutoff date or got a partial page
            if reached_cutoff or len(raw_records) < BATCH_SIZE:
                break

            offset += BATCH_SIZE

        except Exception as e:
            print(f"❌ San Antonio Error at offset {offset}: {e}")
            break

    print(f"✅ San Antonio: Processed {len(mapped_records)} unique records.")
    return mapped_records

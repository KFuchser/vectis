# Vectis System Architecture (v1.0)

## Status
- **Status:** PRODUCTION STABLE
- **Date:** 2026-01-28
- **Verified Cities:** Austin, Fort Worth, Los Angeles, San Antonio
- **Total Volume:** ~43,000+ Records

## 1. The "Secret Sauce" (Critical Logic Locks)

If you change these specific lines, the system will break.

### 🛡️ Ingestion Orchestrator (`ingest_velocity_50.py`)

-   **The Chunking Fix:** Supabase silently rejects large JSON payloads. We solved this by implementing `batch_upsert` with a strict limit.
-   **Configuration:** `batch_size = 200` (Do not increase this above 500).
-   **Why:** Ensures San Antonio and LA data actually lands in the DB instead of timing out.

### 🤠 San Antonio Spoke (`ingest_san_antonio.py`)

-   **The Collision Fix:** The API returns duplicate `PERMIT #` values for different sub-permits.
-   **Configuration:** Composite ID logic is mandatory.
-   **Code:** `unique_pid = f"{permit_no}_{internal_id}"`
-   **Why:** Without this, you lose ~23% of data to invisible overwrites.

### 🎸 Austin Spoke (`ingest_austin.py`)

-   **The Sorting Fix:** Austin's `applieddate` is often null/stale.
-   **Configuration:** `$order=issue_date DESC` (NOT `applieddate`).
-   **Why:** Sorting by application date hides the last 2 years of data.

### 🌴 Los Angeles Spoke (`ingest_la.py`)

-   **The Timeout Fix:** Socrata is slow.
-   **Configuration:** `timeout=60` (Seconds).
-   **Schema:** `applied_date` is hard-coded to `None` (Source does not publish it).

### 📊 Dashboard (`dashboard.py`)

-   **The Pagination Fix:** Supabase has a hard default limit of 1,000 rows per fetch.
-   **Configuration:** `while True` loop with `.range(offset, offset + chunk_size)`.
-   **Why:** Without this loop, the dashboard will only show the "newest" 1,000 records (often dominated by Fort Worth's future dates), making other cities invisible.
-   **The Time Guard:**
    -   **Logic:** `df = df[df['issue_date'] <= now]`
    -   **Why:** Fort Worth publishes expiration dates (e.g., March 2026) in the "Issued" field. This filter prevents the timeline from stretching into the future.

## 2. Verified Data Schema

Every record in the Supabase `permits` table currently adheres to this structure:

| Field             | Type   | Notes                                                  |
| ----------------- | ------ | ------------------------------------------------------ |
| `city`            | String | "Austin", "San Antonio", "Fort Worth", "Los Angeles"   |
| `permit_id`       | String | Critical: Unique identifier for the permit record.     |
| `description`     | String | Raw text describing the work.                          |
| `issue_date`      | Date   | The date the permit was issued.                        |
| `status`          | String | Permit status (e.g., "Active", "Expired").             |
| `complexity_tier` | String | AI-classified tier: "Residential" or "Commercial".     |

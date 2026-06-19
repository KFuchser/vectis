# Vectis System Architecture (v2.0)

## Status

| Field | Value |
| --- | --- |
| **Status** | PRODUCTION STABLE |
| **Last Updated** | 2026-06-19 |
| **Active Spokes** | Austin, San Antonio, Fort Worth, Los Angeles, Chicago, New York, San Francisco |
| **Orchestrator** | `ingest_velocity_50.py` (GitHub Actions, daily 6:00 AM UTC) |
| **Storage** | Supabase (PostgreSQL) |
| **Presentation** | Streamlit Community Cloud |
| **AI Classifier** | Gemini 2.0 Flash (`google.genai` SDK) |

---

## 1. Critical Logic Locks

Changes to the following patterns will break the pipeline.

### Ingestion Orchestrator (`ingest_velocity_50.py`)

- **Batch Upsert:** Supabase silently rejects large JSON payloads. Upserts are chunked at `batch_size = 200` (hard maximum: 500). Do not raise this limit.
- **Env Guard:** `SUPABASE_URL`, `SUPABASE_KEY`, and `GOOGLE_API_KEY` are validated immediately after `load_dotenv()`. A missing variable raises `EnvironmentError` before any client is instantiated.
- **Time-Travel Guard:** After all spokes complete, records where `issued_date < applied_date` (both present) are dropped before classification. Do not remove this filter.
- **Retry Wrapper:** Each spoke call retries up to 3 times with 5s backoff. The `ingest()` helper must `return` on success to stop the retry loop.
- **Deduplication Key:** `f"{city}_{permit_id}"` — built before upsert to handle cross-spoke duplicates in memory.

### San Antonio Spoke (`ingest_san_antonio.py`)

- **Composite ID:** The CKAN API returns duplicate `PERMIT #` values for sub-tasks. The composite `unique_pid = f"{permit_no}_{internal_id}"` is mandatory. Removing it causes ~23% silent data loss.
- **Cutoff break:** Records are sorted `_id DESC` (newest first). Pagination stops when `issued_date < cutoff_date` is encountered, not on empty page.

### Austin Spoke (`ingest_austin.py`)

- **Sort order:** Must sort by `$order=issue_date DESC`, never `applieddate`. The `applieddate` field is frequently null or years in the past, which hides recent records.

### Los Angeles Spoke (`ingest_la.py`)

- **Timeout:** `timeout=60` seconds. The LA Socrata endpoint is historically slow; the default 30s trips frequently.
- **`applied_date`:** Hard-coded to `None`. The source does not publish application dates. LA records contribute to Volume metrics only, not Velocity.

### New York Spoke (`ingest_new_york.py`)

- **24-Month Hard Limit:** Enforced via `max_history = datetime.now() - timedelta(days=730)`. The NYC dataset is large; an uncapped lookback causes memory and timeout failures.
- **Dataset ID:** `rbx6-tga4` (DOB NOW). The original dataset `ipu4-2q9a` is defunct.

### Dashboard (`dashboard.py`)

- **Pagination Loop:** Supabase returns a maximum of 1,000 rows per request. The `while True` offset loop is required to load the full dataset. Removing it limits the dashboard to the 1,000 most recent records.
- **Time Guard:** `df = df[df['issue_date'] <= now]` — Fort Worth publishes permit expiration dates in the `issued_date` field, which are future-dated. This filter prevents chart distortion.

---

## 2. Classification Architecture

Permits are classified at two points in the pipeline:

### 2a. Orchestrator Classification (at ingest, `ingest_velocity_50.py`)

Uses a 4-tier taxonomy: `Commercial`, `Residential`, `Commodity`, `Unknown`.

| Level | Trigger | Action |
| --- | --- | --- |
| Pre-filter | `valuation < $5k` OR description contains commodity keywords | → `Commodity / Residential - Alteration` |
| Res-filter | Description contains residential keywords (sfh, duplex, adu, etc.) | → `Residential / Residential - New` |
| AI (Gemini) | Remaining permits with `valuation >= $25k` | Batched in chunks of 30; classified into Commercial, Residential, or Commodity |
| Fallback | All others | → `Unknown` |

The `project_category` field is populated from the AI's `category` response via `_map_project_category()`.

### 2b. Post-Ingest Engine (standalone, `classify_engine.py`)

Targets `Unknown` records remaining in Supabase after orchestrator ingestion. Uses a 3-tier taxonomy: `Strategic`, `Commodity`, `Ambiguous`. Runs on-demand (not part of the daily GitHub Actions workflow).

> **Note:** The two engines use different tier names. Records classified by the orchestrator will not be re-processed by `classify_engine.py` because their `complexity_tier` is no longer `Unknown`.

---

## 3. Verified Data Schema

Every record in the Supabase `permits` table adheres to this structure:

| Field              | Type    | Notes |
| ------------------ | ------- | ----- |
| `city`             | String  | One of: Austin, San Antonio, Fort Worth, Los Angeles, Chicago, New York, San Francisco |
| `permit_id`        | String  | Source-specific unique ID. San Antonio uses composite `permit_no_internal_id`. |
| `description`      | String  | Raw permit description text. Default: `"Unspecified"`. |
| `applied_date`     | Date    | ISO `YYYY-MM-DD`. Null for LA (source does not publish it). |
| `issued_date`      | Date    | ISO `YYYY-MM-DD`. |
| `valuation`        | Float   | Declared project value in USD. Default: `0.0`. |
| `status`           | String  | Source permit status. Default: `"Issued"`. |
| `complexity_tier`  | String  | `Commercial`, `Residential`, `Commodity`, `Unknown`, `Strategic`, or `Ambiguous`. |
| `project_category` | String  | Granular category from AI (e.g., `"Commercial - Tenant Improvement"`). Default: `"Unknown"`. |
| `ai_rationale`     | String  | Free-text explanation from the AI classifier. |

> `latitude` and `longitude` are defined in `PermitRecord` but excluded from all Supabase upserts. They are reserved for a future map layer.

---

## 4. Error Triage Ledger

| Spoke / Component | Error Signature | Status | Resolution |
| --- | --- | --- | --- |
| New York | `No service found for this URL` | Resolved | Original endpoint defunct. Migrated to dataset `rbx6-tga4`. |
| New York | `no such column: issued_date` | Resolved | Client-side SODA query error. Fixed field mapping in `ingest_new_york.py`. Added 24-month hard limit. |
| San Antonio | Duplicate key / DB collisions | Resolved | Composite ID `permit_no_internal_id` prevents ~23% data loss. |
| Austin | Stale / missing recent records | Resolved | Changed sort to `$order=issue_date DESC`. |
| Los Angeles | Network timeouts | Resolved | Increased HTTP timeout to 60 seconds. |
| Fort Worth | Future dates in `issued_date` | Resolved | Dashboard Time Guard filters `issue_date > now`. |
| Dallas | 90-day cutoff failures | Retired | Source only contained historical data. Script decommissioned. |
| Supabase | Silent transaction failures on large payloads | Resolved | Upsert batch size capped at 200 with 0.2s pause between batches. |
| `classify_engine.py` | Keyword filter never fired | Resolved | Was querying `complexity_tier = 'Standard'`; corrected to `'Unknown'`. |
| `classify_engine.py` | Cross-city record corruption | Resolved | All updates now scoped to `(city, permit_id)`, not `permit_id` alone. |

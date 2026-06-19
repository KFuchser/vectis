# Vectis Operational Playbook (v2.0)

Last updated: 2026-06-19

This document is the first reference for diagnosing pipeline and dashboard issues.

---

## 1. Health Checks

### Check 1: Record Count

**Purpose:** Confirm data is landing in Supabase.

```sql
select city, count(*) from permits group by city order by city;
```

**Expected:** All 7 cities present with record counts proportional to their permit volume. If a city shows 0, run its spoke directly:

```bash
.venv/Scripts/python ingest_austin.py
```

**Threshold:** A city returning 0 records for 3+ consecutive days indicates a spoke failure or source API change.

---

### Check 2: Date Range (Staleness)

**Purpose:** Identify if a city's data is stuck in the past.

```bash
.venv/Scripts/python vc.py
```

Look for cities where `max_date` is more than 5 days behind today. This usually means:
- The API sort order changed (Austin: must sort by `issue_date`, not `applieddate`)
- The source endpoint moved (New York: current dataset is `rbx6-tga4`)
- The 90-day cutoff is filtering everything out due to a date field anomaly

---

### Check 3: Classification Rate

**Purpose:** Confirm the AI classifier is functioning and `Unknown` permits are being resolved.

```sql
select complexity_tier, count(*) from permits group by complexity_tier order by complexity_tier;
```

**Expected:** `Unknown` should be below 10% of total records after a successful orchestrator + classify_engine run. A high `Unknown` count indicates:
- `GOOGLE_API_KEY` is missing or revoked
- The Gemini API returned errors during the last batch run
- `classify_engine.py` has not been run recently (it is standalone, not in the daily workflow)

---

### Check 4: Time-Travel Records

**Purpose:** Identify records where `issued_date < applied_date` (data quality).

```sql
select city, count(*) from permits
where issued_date < applied_date
  and issued_date is not null
  and applied_date is not null
group by city;
```

**Expected:** Zero rows. The orchestrator drops these at ingest. If rows appear, they were either inserted before the guard was implemented or arrived via a direct DB operation.

---

## 2. Known Quirks by City

| City | Quirk | Status |
| --- | --- | --- |
| **Austin** | `applieddate` is often null/stale — sort by `issue_date` | Active guard in spoke |
| **San Antonio** | Duplicate permit numbers for sub-tasks — composite ID required | Active guard in spoke |
| **Los Angeles** | No `applied_date` published — contributes to Volume only, not Velocity | By design |
| **Fort Worth** | `issued_date` often contains expiration dates set years in the future | Dashboard Time Guard filters `issue_date > now` |
| **New York** | Large dataset — 24-month hard lookback limit enforced | Active guard in spoke |
| **Chicago** | High volume — paginated at 1,000 records/page | Active pagination in spoke |
| **San Francisco** | No known active quirks | — |

---

## 3. Running the Post-Ingest Classifier

`classify_engine.py` is a standalone script — it is **not** part of the daily GitHub Actions workflow. Run it manually when a large batch of `Unknown` records needs resolution:

```bash
.venv/Scripts/python classify_engine.py
```

It will:
1. Fetch up to 1,000 `Unknown` records and keyword-classify obvious Commodity items.
2. Fetch up to 500 remaining `Unknown` records, collapse by unique description, and send batches to Gemini 2.0 Flash.
3. Write results back via batched upsert scoped to `(city, permit_id)`.

> **Cost note:** Each run of the AI processor sends up to `ceil(500 / 15) = 34` Gemini API calls. Run it judiciously during high-volume ingestion periods.

---

## 4. Roadmap (Open Items)

| Priority | Item | Notes |
| --- | --- | --- |
| P3 | YoY comparison chart in dashboard | Overlay current 90-day volume vs same window -1 year |
| P3 | Map visualization layer | `lat/lng` fields exist in `PermitRecord` but are excluded from upserts; wire in at ingest and add `pydeck` layer to dashboard |
| P3 | Expand spoke coverage | Phoenix and Houston are on Socrata (compatible pattern). Seattle uses CKAN (same pattern as San Antonio). ~2 days per spoke. |
| P3 | Dallas revival | Retired due to historical-only source data. Worth a fresh endpoint audit — city may have published a current dataset. |
| P3 | Structured logging | Replace `print()` with Python `logging` module for log-level filtering in GitHub Actions output |

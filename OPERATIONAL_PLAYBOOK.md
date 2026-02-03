# Vectis Operational Playbook (v1.0)

This document provides a guide for maintaining and troubleshooting the Vectis Data Pipeline.

## 1. Health Checks

Use these checks to diagnose and resolve common data ingestion and presentation issues.

### 1. The "Record Count" Check

-   **Purpose:** Verify that data is being ingested into the Supabase database.
-   **Method:** Run the following SQL query in the Supabase SQL Editor:
    ```sql
    select count(*) from permits;
    ```
-   **Verification:**
    -   If the count is less than 1,000, you have an ingestion problem.
    -   If the count is greater than 1,000 but the dashboard shows 1,000, you have a dashboard pagination problem.

### 2. The "Date Range" Check

-   **Purpose:** Identify if any city's data is "stuck" in the past.
-   **Method:** Run the `vc.py` script:
    ```bash
    pipenv run python vc.py
    ```
-   **Verification:** Look for cities with a `max_date` that is significantly in the past (e.g., 2023). This usually means the API sort order is wrong.

## 2. Next Steps

Now that the Data Factory is stable, you can safely focus on:

-   **AI Classification:** Refine the `complexity_tier` logic (Residential vs Commercial) in the orchestrator.
-   **UI Polish:** Add year-over-year comparison charts or map visualizations.

You have built a robust pipeline. Do not delete the `systemstate.md` file. It is your safety net.

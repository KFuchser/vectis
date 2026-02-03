# Project Overview

This project is a data pipeline that ingests building permit data from various cities, processes it, and provides a web-based dashboard for visualization and control.

## Components

*   **Data Ingestion:** It uses the Socrata API (via the `sodapy` library) to fetch data from cities like Austin, San Antonio, Fort Worth, and Los Angeles.
*   **AI Processing:** It leverages a Google generative AI model (`google-genai`) to classify and process the raw permit descriptions.
*   **Data Storage:** The processed data is stored in a Supabase database.
*   **Web Interface:** A Streamlit application (`dashboard.py`) serves as a control panel and dashboard for running the data pipeline and visualizing the results.
*   **Data Validation:** Pydantic is used to ensure data integrity and structure.

## Key Documentation
- **[System Architecture](ARCHITECTURE.md)**: The primary technical reference, including "Secret Sauce" logic and schema.
- **[Operational Playbook](OPERATIONAL_PLAYBOOK.md)**: A guide for maintaining and troubleshooting the pipeline.
- **[Connection Report](connection_report.md)**: Automated verification of data source endpoints and status.

In short, it's a complete system for collecting, cleaning, analyzing, and viewing public permit data.

**Note:** Do not delete `ARCHITECTURE.md` or `OPERATIONAL_PLAYBOOK.md`. They are critical for understanding and maintaining the system.
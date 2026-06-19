# Vectis Data Pipeline

## Overview

This project is a data pipeline that ingests building permit data from various cities, processes it, and provides a web-based dashboard for visualization and control. It is designed to be a robust and scalable solution for collecting, cleaning, and analyzing public permit data.

The pipeline currently ingests data from the following cities:
- Austin, TX
- San Antonio, TX
- Fort Worth, TX
- Los Angeles, CA
- Chicago, IL
- New York, NY
- San Francisco, CA

## Getting Started

### Prerequisites

- Python 3.8+
- Pipenv

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/vectis-data-pipeline.git
   ```
2. Install the dependencies:
   ```bash
   pipenv install
   ```
3. Set up your environment variables:
   - Create a `.env` file in the root directory.
   - Add the following variables:
     ```
     SUPABASE_URL="your_supabase_url"
     SUPABASE_KEY="your_supabase_key"
     GOOGLE_API_KEY="your_gemini_api_key"
     SOCRATA_APP_TOKEN="your_socrata_token"
     ```
   - `GOOGLE_API_KEY` and `SOCRATA_APP_TOKEN` are required. The orchestrator will raise `EnvironmentError` at startup if any of the first three are missing.

## Project Structure

```
├── .github/                  # GitHub Actions workflows (daily 6 AM UTC)
├── .streamlit/               # Streamlit secrets config (production)
├── .venv/                    # Virtual environment
├── .gitignore
├── ARCHITECTURE.md           # System architecture and critical logic locks
├── OPERATIONAL_PLAYBOOK.md   # Health checks and troubleshooting
├── README.md                 # This file
├── classify_engine.py        # Post-ingest re-classification engine (standalone)
├── dashboard.py              # Streamlit dashboard (Vectis Command Console)
├── health_check.py           # Connection and schema validation utility
├── ingest_austin.py          # Austin, TX spoke (Socrata)
├── ingest_chicago.py         # Chicago, IL spoke (Socrata, paginated)
├── ingest_fort_worth.py      # Fort Worth, TX spoke (ArcGIS)
├── ingest_la.py              # Los Angeles, CA spoke (Socrata, volume-only)
├── ingest_new_york.py        # New York, NY spoke (sodapy, paginated, 24-month limit)
├── ingest_san_antonio.py     # San Antonio, TX spoke (CKAN, composite ID)
├── ingest_san_francisco.py   # San Francisco, CA spoke (Socrata)
├── ingest_velocity_50.py     # Main orchestrator — fetch → classify → upsert
├── requirements.txt
├── service_models.py         # Pydantic models: PermitRecord, ComplexityTier, ProjectCategory
└── vc.py                     # CLI health check: shows max issued_date per city
```

## Running the Pipeline

The main ingestion script is `ingest_velocity_50.py`. To run the pipeline, execute the following command:

```bash
pipenv run python ingest_velocity_50.py
```

This will ingest the latest permit data from all configured cities.

## Dashboards

The project includes a Streamlit dashboard for visualizing the ingested data. To run the dashboard, execute the following command:

```bash
pipenv run streamlit run dashboard.py
```

## Contributing

Please read `ARCHITECTURE.md` and `OPERATIONAL_PLAYBOOK.md` before contributing. Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

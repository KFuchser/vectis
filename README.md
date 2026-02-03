# Vectis Data Pipeline

## Overview

This project is a data pipeline that ingests building permit data from various cities, processes it, and provides a web-based dashboard for visualization and control. It is designed to be a robust and scalable solution for collecting, cleaning, and analyzing public permit data.

The pipeline currently ingests data from the following cities:
- Austin, TX
- San Antonio, TX
- Fort Worth, TX
- Los Angeles, CA

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
     ```

## Project Structure

```
├── .github/              # GitHub Actions workflows
├── .streamlit/           # Streamlit configuration
├── venv/                 # Virtual environment
├── .gitignore
├── ARCHITECTURE.md       # System architecture and design
├── OPERATIONAL_PLAYBOOK.md # Maintenance and troubleshooting
├── README.md             # This file
├── agent_main.py
├── ai_classifier.py
├── app.py
├── check_models.py
├── classify_engine.py
├── connection_report.md
├── dashboard.py          # Main Streamlit dashboard
├── gemini.md
├── health_check.py
├── ingest_austin.py      # Ingestion script for Austin
├── ingest_fort_worth.py  # Ingestion script for Fort Worth
├── ingest_la.py          # Ingestion script for Los Angeles
├── ingest_san_antonio.py # Ingestion script for San Antonio
├── ingest_velocity_50.py # Main ingestion orchestrator
├── inspect_schema.py
├── keyword_classifier.py
├── requirements.txt
├── runback.py
├── satest.py
├── scrub_history.py
├── service_models.py
├── systemstate.md        # Legacy system state
├── test_logic.py
├── vc.py
└── verify_gemini.py
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

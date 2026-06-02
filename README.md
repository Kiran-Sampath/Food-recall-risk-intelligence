# Food Recall Risk Intelligence Pipeline

Compact end-to-end pipeline to ingest openFDA food enforcement data, transform and clean it with PySpark, run data-quality checks, calculate recall risk scores, build analytics tables, and visualize results with a Streamlit dashboard.

Enhanced outputs include product categories, distribution scope, region, open-recall aging, repeated-company scoring, risk tiers, company watchlists, and dashboard-wide search.

Quickstart

1. Create a Python environment and install dependencies:

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

2. Run the ingestion (example):

```bash
python src/ingestion/extract_openfda.py --limit 100
```

For a larger historical pull, use pagination and a report-date window:

```bash
python src/ingestion/extract_openfda.py --limit 5000 --page-size 100 --start-date 2020-01-01 --end-date 2026-05-29
```

3. Run the PySpark transform locally (requires PySpark):

```bash
python src/transformations/bronze_to_silver.py
python src/scoring/calculate_risk_score.py
python src/transformations/build_gold_tables.py
```

4. Launch the dashboard:

```bash
streamlit run dashboard/app.py
```

Project layout

- `data/` — Bronze/Silver/Gold artifacts (not checked into git)
- `src/` — ingestion, transformations, quality, scoring code
- `dashboard/` — Streamlit app
- `dags/` — Airflow DAGs
- `tests/` — unit tests

See `README` sections in each folder for details.

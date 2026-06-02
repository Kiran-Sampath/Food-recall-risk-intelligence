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

Deployment

The dashboard is designed to deploy on Streamlit Community Cloud with Supabase as the persistent database.

1. Push this repo to GitHub.
2. In Streamlit Community Cloud, create a new app from `dashboard/app.py`.
3. Add these app secrets:

```toml
SUPABASE_URL = "https://your-project-ref.supabase.co"
SUPABASE_ANON_KEY = "your-anon-key"
```

Do not add `SUPABASE_SERVICE_ROLE_KEY` to the public dashboard deployment. Use the service role key only in local/Docker/GitHub Actions pipeline jobs that load data.

To refresh Supabase locally with the last-five-years dataset:

```bash
docker run --rm --env-file .env -v "${PWD}:/app" -w /app food-recall-risk-intelligence-dashboard:latest python scripts/run_pipeline.py --complete-download --filter-report-start 2021-06-01 --filter-report-end 2026-06-01 --load-supabase --replace-supabase
```

Scheduled refresh

The GitHub Actions workflow at `.github/workflows/refresh-supabase.yml` runs daily at 08:30 UTC. By default, it fetches records for yesterday's FDA `report_date`, scores them, and upserts them into the `recalls` table. It can also be run manually from the GitHub Actions tab with a specific `report_date` for backfills.

Add these repository secrets in GitHub before running it:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
```

The daily job is incremental and does not replace the database. Full historical rebuilds should be run deliberately with `--complete-download`, a report-date window, and `--replace-supabase`.

Project layout

- `data/` — Bronze/Silver/Gold artifacts (not checked into git)
- `src/` — ingestion, transformations, quality, scoring code
- `dashboard/` — Streamlit app
- `dags/` — Airflow DAGs
- `tests/` — unit tests

See `README` sections in each folder for details.

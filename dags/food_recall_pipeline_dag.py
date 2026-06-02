from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="food_recall_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id="extract_openfda",
        bash_command="python src/ingestion/extract_openfda.py --limit 1000 --page-size 100",
    )

    # This example assumes the bronze file is the most recent JSON in data/bronze
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python -u src/transformations/bronze_to_silver.py data/bronze/food_recalls_$(date +%F).json",
    )

    dq = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="python src/quality/data_quality_checks.py data/silver/food_recalls data/gold/data_quality_report",
    )

    score = BashOperator(
        task_id="calculate_risk_scores",
        bash_command="python src/scoring/calculate_risk_score.py data/silver/food_recalls data/gold/food_recall_risk_scores",
    )

    build_gold = BashOperator(
        task_id="build_gold_tables",
        bash_command="python src/transformations/build_gold_tables.py data/gold/food_recall_risk_scores data/gold",
    )

    extract >> bronze_to_silver >> dq >> score >> build_gold

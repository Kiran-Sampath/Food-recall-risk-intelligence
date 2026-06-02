import argparse
import json
import shutil
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.extract_openfda import fetch_and_save, fetch_complete_download
from src.quality.data_quality_checks import run_data_quality
from src.scoring.calculate_risk_score import calculate_risk
from src.transformations.bronze_to_silver import bronze_to_silver
from src.transformations.build_gold_tables import build_gold
from src.load.load_to_supabase import load_pipeline_run, load_tables


def filter_parquet_by_report_date(input_path, output_path, start_date=None, end_date=None):
    if not start_date and not end_date:
        return

    source = Path(input_path)
    temp_output = Path(f"{output_path}_filtered_tmp")
    if temp_output.exists():
        shutil.rmtree(temp_output)

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, to_date

        spark = SparkSession.builder.appName("FilterRecallsByReportDate").getOrCreate()
        df = spark.read.parquet(str(source))
        filtered = df
        if start_date:
            filtered = filtered.filter(col("report_date") >= to_date(lit(start_date)))
        if end_date:
            filtered = filtered.filter(col("report_date") <= to_date(lit(end_date)))
        filtered.write.mode("overwrite").parquet(str(temp_output))
    except Exception as err:
        print(f"Spark date filter failed, falling back to pandas: {err}")
        df = pd.read_parquet(source)
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
        filtered = df
        if start_date:
            filtered = filtered[filtered["report_date"] >= pd.to_datetime(start_date)]
        if end_date:
            filtered = filtered[filtered["report_date"] <= pd.to_datetime(end_date)]
        temp_output.mkdir(parents=True, exist_ok=True)
        filtered.to_parquet(temp_output / "part-00000.parquet", index=False)

    target = Path(output_path)
    if target.exists():
        shutil.rmtree(target)
    temp_output.replace(target)


def run_pipeline(
    limit,
    start_date=None,
    end_date=None,
    bronze_path=None,
    load_supabase=False,
    complete_download=False,
    filter_report_start=None,
    filter_report_end=None,
    replace_supabase=False,
    supabase_tables=None,
):
    if bronze_path:
        input_path = bronze_path
    elif complete_download:
        input_path = fetch_complete_download()
    else:
        input_path = fetch_and_save(limit=limit, start_date=start_date, end_date=end_date)

    meta = {}
    input_file = Path(input_path)
    if input_file.exists():
        with input_file.open("r", encoding="utf-8") as fh:
            meta = json.load(fh).get("meta", {})

    if meta.get("records_fetched") == 0:
        if load_supabase:
            load_pipeline_run(input_path, {}, status="success")
        print(
            {
                "status": "success",
                "bronze_path": input_path,
                "records_fetched": 0,
                "supabase_loaded": {},
                "warning": "No records found for requested date window.",
            }
        )
        return

    bronze_to_silver(input_path, "data/silver/food_recalls")
    filter_parquet_by_report_date(
        "data/silver/food_recalls",
        "data/silver/food_recalls",
        start_date=filter_report_start,
        end_date=filter_report_end,
    )
    run_data_quality("data/silver/food_recalls", "data/gold/data_quality_report")
    calculate_risk("data/silver/food_recalls", "data/gold/food_recall_risk_scores")
    build_gold("data/gold/food_recall_risk_scores", "data/gold")
    loaded_counts = {}
    if load_supabase:
        loaded_counts = load_tables(table_names=supabase_tables, replace=replace_supabase)
        load_pipeline_run(input_path, loaded_counts)

    print(
        {
            "status": "success",
            "bronze_path": input_path,
            "records_fetched": meta.get("records_fetched"),
            "supabase_loaded": loaded_counts,
            "warning": meta.get("warning"),
        }
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--bronze-path", type=str, default=None)
    parser.add_argument("--load-supabase", action="store_true")
    parser.add_argument("--complete-download", action="store_true")
    parser.add_argument("--filter-report-start", type=str, default=None)
    parser.add_argument("--filter-report-end", type=str, default=None)
    parser.add_argument("--replace-supabase", action="store_true")
    parser.add_argument("--supabase-tables", nargs="*", default=None)
    args = parser.parse_args()

    run_pipeline(
        limit=args.limit,
        start_date=args.start_date,
        end_date=args.end_date,
        bronze_path=args.bronze_path,
        load_supabase=args.load_supabase,
        complete_download=args.complete_download,
        filter_report_start=args.filter_report_start,
        filter_report_end=args.filter_report_end,
        replace_supabase=args.replace_supabase,
        supabase_tables=args.supabase_tables,
    )


if __name__ == "__main__":
    main()

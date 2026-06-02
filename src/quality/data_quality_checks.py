import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import count, when, col
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


def _resolve_parquet_path(path):
    p = Path(path)
    if p.is_dir():
        candidates = list(p.glob("*.parquet"))
        if not candidates:
            raise FileNotFoundError(f"No parquet file found in directory {path}")
        return candidates[0]
    return p


def run_data_quality(silver_path: str, output_path: str = "data/gold/data_quality_report"):
    if HAS_PYSPARK:
        try:
            spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()
            df = spark.read.parquet(silver_path)
            dq_df = df.select(
                count("*").alias("records_checked"),
                count(when(col("recall_number").isNull(), True)).alias("missing_recall_number"),
                count(when(col("classification").isNull(), True)).alias("missing_classification"),
                count(when(col("reason_for_recall").isNull(), True)).alias("missing_reason"),
                count(when(col("recalling_firm").isNull(), True)).alias("missing_recalling_firm")
            )
            dq_df.write.mode("overwrite").parquet(output_path)
            print(f"Wrote data quality report to {output_path} using PySpark")
            return
        except Exception as err:
            print(f"PySpark failed, falling back to pandas: {err}")

    import pandas as pd
    parquet_file = _resolve_parquet_path(silver_path)
    df = pd.read_parquet(parquet_file)
    report = {
        "records_checked": len(df),
        "missing_recall_number": int(df["recall_number"].isna().sum()),
        "missing_classification": int(df["classification"].isna().sum()),
        "missing_reason": int(df["reason_for_recall"].isna().sum()),
        "missing_recalling_firm": int(df["recalling_firm"].isna().sum()),
    }
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([report]).to_parquet(output_dir / "part-00000.parquet", index=False)
    print(f"Wrote data quality report to {output_dir / 'part-00000.parquet'} using pandas fallback")


if __name__ == "__main__":
    # simple CLI
    import sys
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/food_recalls"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/gold/data_quality_report"
    run_data_quality(in_path, out_path)

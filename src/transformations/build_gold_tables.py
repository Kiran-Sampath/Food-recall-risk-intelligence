import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg as spark_avg, count, date_format, max as spark_max, sum as spark_sum, when, col
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


def build_gold(risk_path: str, out_dir: str = "data/gold"):
    if HAS_PYSPARK:
        try:
            spark = SparkSession.builder.appName("BuildGoldTables").getOrCreate()
            df = spark.read.parquet(risk_path)
            overview_df = df.groupBy("classification").count()
            overview_df.write.mode("overwrite").parquet(f"{out_dir}/recall_overview")
            reason_summary_df = df.groupBy("recall_reason_category").count()
            reason_summary_df.write.mode("overwrite").parquet(f"{out_dir}/recall_reason_summary")
            company_risk_df = df.groupBy("recalling_firm_clean").agg(
                count("*").alias("total_recalls"),
                spark_avg("risk_score").alias("avg_risk_score"),
                spark_max("recall_initiation_date").alias("latest_recall_date"),
                spark_sum(when(col("classification") == "Class I", 1).otherwise(0)).alias("class_i_recalls"),
                spark_sum(when(col("risk_tier").isin("Critical", "High"), 1).otherwise(0)).alias("high_risk_recalls"),
                spark_sum(when(col("status").rlike("(?i)ongoing"), 1).otherwise(0)).alias("ongoing_recalls")
            )
            company_risk_df.write.mode("overwrite").parquet(f"{out_dir}/company_risk")
            company_risk_df.orderBy(col("high_risk_recalls").desc(), col("avg_risk_score").desc()).write.mode("overwrite").parquet(f"{out_dir}/company_watchlist")
            monthly_df = df.withColumn("recall_month", date_format(col("recall_initiation_date"), "yyyy-MM")).groupBy("recall_month").count()
            monthly_df.write.mode("overwrite").parquet(f"{out_dir}/monthly_recall_trends")
            geo_df = df.groupBy("state", "country").agg(
                count("*").alias("total_recalls"),
                spark_avg("risk_score").alias("avg_risk_score")
            )
            geo_df.write.mode("overwrite").parquet(f"{out_dir}/geographic_recall_summary")
            df.groupBy("risk_tier").agg(count("*").alias("count"), spark_avg("risk_score").alias("avg_risk_score")).write.mode("overwrite").parquet(f"{out_dir}/risk_tier_summary")
            df.groupBy("product_category").agg(count("*").alias("count"), spark_avg("risk_score").alias("avg_risk_score")).write.mode("overwrite").parquet(f"{out_dir}/product_category_summary")
            df.filter(col("status").rlike("(?i)ongoing")).select(
                "recall_number",
                "recalling_firm_clean",
                "classification",
                "recall_reason_category",
                "recall_initiation_date",
                "recall_duration_days",
                "risk_score",
                "risk_tier",
            ).write.mode("overwrite").parquet(f"{out_dir}/open_recall_aging")
            print(f"Wrote gold tables to {out_dir} using PySpark")
            return
        except Exception as err:
            print(f"PySpark failed, falling back to pandas: {err}")

    import pandas as pd
    parquet_file = _resolve_parquet_path(risk_path)
    df = pd.read_parquet(parquet_file)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    overview_df = df.groupby("classification").size().reset_index(name="count")
    overview_df.to_parquet(out_dir / "recall_overview.parquet", index=False)

    reason_summary_df = df.groupby("recall_reason_category").size().reset_index(name="count")
    reason_summary_df.to_parquet(out_dir / "recall_reason_summary.parquet", index=False)

    company_risk_df = (
        df.groupby("recalling_firm_clean")["risk_score"]
        .agg(["count", "mean"])
        .reset_index()
        .rename(columns={"count": "total_recalls", "mean": "avg_risk_score"})
    )
    latest_dates = (
        df.groupby("recalling_firm_clean")["recall_initiation_date"]
        .max()
        .reset_index(name="latest_recall_date")
    )
    company_risk_df = company_risk_df.merge(latest_dates, on="recalling_firm_clean", how="left")
    company_flags = df.groupby("recalling_firm_clean").agg(
        class_i_recalls=("classification", lambda s: int((s == "Class I").sum())),
        high_risk_recalls=("risk_tier", lambda s: int(s.isin(["Critical", "High"]).sum())),
        ongoing_recalls=("status", lambda s: int(s.fillna("").astype(str).str.contains("ongoing", case=False).sum())),
    ).reset_index()
    company_risk_df = company_risk_df.merge(company_flags, on="recalling_firm_clean", how="left")
    company_risk_df.to_parquet(out_dir / "company_risk.parquet", index=False)
    company_risk_df.sort_values(["high_risk_recalls", "avg_risk_score"], ascending=[False, False]).to_parquet(out_dir / "company_watchlist.parquet", index=False)

    monthly_df = (
        df.assign(recall_month=df["recall_initiation_date"].dt.strftime("%Y-%m"))
        .groupby("recall_month")
        .size()
        .reset_index(name="count")
    )
    monthly_df.to_parquet(out_dir / "monthly_recall_trends.parquet", index=False)

    geo_df = (
        df.groupby(["state", "country"]).agg(
            total_recalls=("recall_number", "count"),
            avg_risk_score=("risk_score", "mean"),
        )
        .reset_index()
    )
    geo_df.to_parquet(out_dir / "geographic_recall_summary.parquet", index=False)

    risk_tier_df = df.groupby("risk_tier").agg(
        count=("recall_number", "count"),
        avg_risk_score=("risk_score", "mean"),
    ).reset_index()
    risk_tier_df.to_parquet(out_dir / "risk_tier_summary.parquet", index=False)

    product_category_df = df.groupby("product_category").agg(
        count=("recall_number", "count"),
        avg_risk_score=("risk_score", "mean"),
    ).reset_index()
    product_category_df.to_parquet(out_dir / "product_category_summary.parquet", index=False)

    open_recall_cols = [
        "recall_number",
        "recalling_firm_clean",
        "classification",
        "recall_reason_category",
        "recall_initiation_date",
        "recall_duration_days",
        "risk_score",
        "risk_tier",
    ]
    open_recalls = df[df["status"].fillna("").astype(str).str.contains("ongoing", case=False)]
    open_recalls[[c for c in open_recall_cols if c in open_recalls.columns]].to_parquet(out_dir / "open_recall_aging.parquet", index=False)
    print(f"Wrote gold tables to {out_dir} using pandas fallback")


if __name__ == "__main__":
    import sys
    risk_path = sys.argv[1] if len(sys.argv) > 1 else "data/gold/food_recall_risk_scores"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "data/gold"
    build_gold(risk_path, out_dir)

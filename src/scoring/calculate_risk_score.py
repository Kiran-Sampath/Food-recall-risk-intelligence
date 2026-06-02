import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Window
    from pyspark.sql.functions import concat_ws, count, lit, when, col
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


def calculate_risk(silver_path: str, output_path: str = "data/gold/food_recall_risk_scores"):
    if HAS_PYSPARK:
        try:
            spark = SparkSession.builder.appName("CalculateRiskScores").getOrCreate()
            df = spark.read.parquet(silver_path)
            company_window = Window.partitionBy("recalling_firm_clean")
            risk_df = df.withColumn(
                "repeated_company_count",
                count("*").over(company_window) - lit(1)
            ).withColumn(
                "classification_score",
                when(col("classification") == "Class I", 50)
                .when(col("classification") == "Class II", 30)
                .when(col("classification") == "Class III", 10)
                .otherwise(0)
            ).withColumn(
                "status_score",
                when(col("status").rlike("(?i)ongoing"), 20).otherwise(0)
            ).withColumn(
                "distribution_score",
                when(col("distribution_pattern").rlike("(?i)nationwide"), 20).otherwise(0)
            ).withColumn(
                "reason_score",
                when(col("recall_reason_category") == "Bacterial Contamination", 25)
                .when(col("recall_reason_category") == "Allergen Issue", 20)
                .when(col("recall_reason_category") == "Labeling Issue", 10)
                .otherwise(5)
            ).withColumn(
                "repeated_company_score",
                when(col("repeated_company_count") * 2 > 20, 20).otherwise(col("repeated_company_count") * 2)
            ).withColumn(
                "risk_score",
                col("classification_score") + col("status_score") + col("distribution_score") + col("reason_score") + col("repeated_company_score")
            ).withColumn(
                "risk_tier",
                when(col("risk_score") >= 90, "Critical")
                .when(col("risk_score") >= 70, "High")
                .when(col("risk_score") >= 40, "Medium")
                .otherwise("Low")
            ).withColumn(
                "risk_explanation",
                concat_ws(
                    "; ",
                    concat_ws("", lit("Classification +"), col("classification_score")),
                    concat_ws("", lit("Status +"), col("status_score")),
                    concat_ws("", lit("Distribution +"), col("distribution_score")),
                    concat_ws("", lit("Reason +"), col("reason_score")),
                    concat_ws("", lit("Repeat company +"), col("repeated_company_score")),
                )
            )
            risk_df.write.mode("overwrite").parquet(output_path)
            print(f"Wrote risk scores to {output_path} using PySpark")
            return
        except Exception as err:
            print(f"PySpark failed, falling back to pandas: {err}")

    import pandas as pd
    import numpy as np

    parquet_file = _resolve_parquet_path(silver_path)
    df = pd.read_parquet(parquet_file)

    df["repeated_company_count"] = df.groupby("recalling_firm_clean")["recall_number"].transform("count").fillna(1).astype(int) - 1

    classification = df["classification"].fillna("").astype(str).str.lower()
    df["classification_score"] = np.select(
        [classification.str.contains(r"\bclass\s*iii\b"), classification.str.contains(r"\bclass\s*ii\b"), classification.str.contains(r"\bclass\s*i\b")],
        [10, 30, 50],
        default=0,
    )

    status = df["status"].fillna("").astype(str).str.lower()
    df["status_score"] = np.where(status.str.contains("ongoing"), 20, 0)
    distribution = df["distribution_pattern"].fillna("").astype(str).str.lower()
    df["distribution_score"] = np.where(distribution.str.contains("nationwide"), 20, 0)

    reason = df["recall_reason_category"].fillna("").astype(str)
    df["reason_score"] = np.select(
        [reason == "Bacterial Contamination", reason == "Allergen Issue", reason == "Labeling Issue"],
        [25, 20, 10],
        default=5,
    )
    df["repeated_company_score"] = (df["repeated_company_count"] * 2).clip(upper=20)
    df["risk_score"] = df["classification_score"] + df["status_score"] + df["distribution_score"] + df["reason_score"] + df["repeated_company_score"]
    df["risk_tier"] = np.select(
        [df["risk_score"] >= 90, df["risk_score"] >= 70, df["risk_score"] >= 40],
        ["Critical", "High", "Medium"],
        default="Low",
    )
    df["risk_explanation"] = (
        "Classification +" + df["classification_score"].astype(str)
        + "; Status +" + df["status_score"].astype(str)
        + "; Distribution +" + df["distribution_score"].astype(str)
        + "; Reason +" + df["reason_score"].astype(str)
        + "; Repeat company +" + df["repeated_company_score"].astype(str)
    )

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir / "part-00000.parquet", index=False)
    print(f"Wrote risk scores to {output_dir / 'part-00000.parquet'} using pandas fallback")


if __name__ == "__main__":
    import sys
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/food_recalls"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/gold/food_recall_risk_scores"
    calculate_risk(in_path, out_path)

import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import datediff, explode, col, to_date, lower, trim, regexp_replace, when
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


def _clean_recalling_firm(column):
    return trim(lower(regexp_replace(column, "[^a-zA-Z0-9\\s]", "")))


def _standard_classification(column):
    normalized = lower(column)
    return when(normalized.rlike(r"\bclass\s*iii\b"), "Class III") \
        .when(normalized.rlike(r"\bclass\s*ii\b"), "Class II") \
        .when(normalized.rlike(r"\bclass\s*i\b"), "Class I") \
        .otherwise(column)


def _standard_status(column):
    return when(lower(column).rlike("ongoing"), "Ongoing") \
        .when(lower(column).rlike("terminated|completed"), "Terminated") \
        .otherwise(column)


def _reason_category(column):
    lower_col = lower(column)
    return when(lower_col.rlike("listeria|salmonella|e\\. coli|contamination"), "Bacterial Contamination") \
        .when(lower_col.rlike("undeclared allergen|milk|soy|peanut|wheat|tree nut"), "Allergen Issue") \
        .when(lower_col.rlike("label|mislabel"), "Labeling Issue") \
        .when(lower_col.rlike("foreign|plastic|metal|glass"), "Foreign Material") \
        .otherwise("Other")


def _product_category(column):
    lower_col = lower(column)
    return when(lower_col.rlike("cheese|milk|cream|yogurt|butter|dairy"), "Dairy") \
        .when(lower_col.rlike("beef|chicken|pork|turkey|meat|sausage"), "Meat & Poultry") \
        .when(lower_col.rlike("fish|shrimp|crab|salmon|tuna|seafood"), "Seafood") \
        .when(lower_col.rlike("lettuce|spinach|salad|sprout|fruit|vegetable|produce"), "Produce") \
        .when(lower_col.rlike("infant|baby|formula"), "Infant & Baby Food") \
        .when(lower_col.rlike("supplement|vitamin|capsule|tablet"), "Supplements") \
        .when(lower_col.rlike("cookie|cake|bread|bakery|pastry"), "Bakery") \
        .when(lower_col.rlike("juice|drink|beverage|water"), "Beverages") \
        .when(lower_col.rlike("frozen|ice cream"), "Frozen Foods") \
        .otherwise("Other")


def _distribution_scope(column):
    lower_col = lower(column)
    return when(lower_col.rlike("nationwide|nationally|all states"), "Nationwide") \
        .when(lower_col.rlike("international|canada|mexico|export"), "International") \
        .when(lower_col.rlike(","), "Multi-State") \
        .otherwise("Local/Unknown")


def _region(column):
    state = lower(trim(column))
    northeast = ["ct", "me", "ma", "nh", "ri", "vt", "nj", "ny", "pa"]
    midwest = ["il", "in", "mi", "oh", "wi", "ia", "ks", "mn", "mo", "ne", "nd", "sd"]
    south = ["de", "fl", "ga", "md", "nc", "sc", "va", "dc", "wv", "al", "ky", "ms", "tn", "ar", "la", "ok", "tx"]
    west = ["az", "co", "id", "mt", "nv", "nm", "ut", "wy", "ak", "ca", "hi", "or", "wa"]
    return when(state.isin(northeast), "Northeast") \
        .when(state.isin(midwest), "Midwest") \
        .when(state.isin(south), "South") \
        .when(state.isin(west), "West") \
        .otherwise("Unknown")


def gold_output_path(output_path: str) -> Path:
    path = Path(output_path)
    if path.suffix == ".parquet":
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        path.mkdir(parents=True, exist_ok=True)
    return path


def bronze_to_silver(input_path: str, output_path: str = "data/silver/food_recalls"):
    if HAS_PYSPARK:
        try:
            spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
            raw_df = spark.read.option("multiline", "true").json(input_path)
            recalls_df = raw_df.select(explode(col("results")).alias("recall"))

            silver_df = recalls_df.select(
                col("recall.recall_number").alias("recall_number"),
                col("recall.event_id").alias("event_id"),
                _standard_status(col("recall.status")).alias("status"),
                _standard_classification(col("recall.classification")).alias("classification"),
                col("recall.product_type").alias("product_type"),
                col("recall.recalling_firm").alias("recalling_firm"),
                _clean_recalling_firm(col("recall.recalling_firm")).alias("recalling_firm_clean"),
                col("recall.product_description").alias("product_description"),
                col("recall.reason_for_recall").alias("reason_for_recall"),
                _reason_category(col("recall.reason_for_recall")).alias("recall_reason_category"),
                _product_category(col("recall.product_description")).alias("product_category"),
                col("recall.distribution_pattern").alias("distribution_pattern"),
                _distribution_scope(col("recall.distribution_pattern")).alias("distribution_scope"),
                col("recall.state").alias("state"),
                _region(col("recall.state")).alias("region"),
                col("recall.country").alias("country"),
                col("recall.city").alias("city"),
                col("recall.postal_code").alias("postal_code"),
                col("recall.product_quantity").alias("product_quantity"),
                col("recall.voluntary_mandated").alias("voluntary_mandated"),
                col("recall.initial_firm_notification").alias("initial_firm_notification"),
                to_date(col("recall.recall_initiation_date"), "yyyyMMdd").alias("recall_initiation_date"),
                to_date(col("recall.report_date"), "yyyyMMdd").alias("report_date"),
                to_date(col("recall.termination_date"), "yyyyMMdd").alias("termination_date")
            )
            silver_df = silver_df.withColumn(
                "is_open",
                lower(col("status")).rlike("ongoing")
            ).withColumn(
                "recall_duration_days",
                datediff(when(col("termination_date").isNull(), col("report_date")).otherwise(col("termination_date")), col("recall_initiation_date"))
            )

            silver_df.write.mode("overwrite").parquet(output_path)
            print(f"Wrote silver parquet to {output_path} using PySpark")
            return
        except Exception as err:
            print(f"PySpark failed, falling back to pandas: {err}")

    import json
    import pandas as pd
    import numpy as np

    with open(input_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    results = pd.json_normalize(raw.get("results", []))

    if results.empty:
        raise ValueError("No recall records found in bronze JSON.")

    results["recall_initiation_date"] = pd.to_datetime(results.get("recall_initiation_date"), format="%Y%m%d", errors="coerce")
    results["report_date"] = pd.to_datetime(results.get("report_date"), format="%Y%m%d", errors="coerce")
    results["termination_date"] = pd.to_datetime(results.get("termination_date"), format="%Y%m%d", errors="coerce")

    results["recalling_firm_clean"] = (
        results.get("recalling_firm", "").fillna("").astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9\s]", "", regex=True)
        .str.strip()
    )

    classification = results.get("classification", "").fillna("").astype(str).str.lower()
    results["classification"] = np.select(
        [classification.str.contains(r"\bclass\s*iii\b"), classification.str.contains(r"\bclass\s*ii\b"), classification.str.contains(r"\bclass\s*i\b")],
        ["Class III", "Class II", "Class I"],
        default=results.get("classification", "")
    )

    status = results.get("status", "").fillna("").astype(str).str.lower()
    results["status"] = np.select(
        [status.str.contains("ongoing"), status.str.contains("terminated|completed")],
        ["Ongoing", "Terminated"],
        default=results.get("status", "")
    )

    reason = results.get("reason_for_recall", "").fillna("").astype(str).str.lower()
    results["recall_reason_category"] = np.select(
        [reason.str.contains(r"listeria|salmonella|e\. coli|contamination"),
         reason.str.contains(r"undeclared allergen|milk|soy|peanut|wheat|tree nut"),
         reason.str.contains(r"label|mislabel"),
         reason.str.contains(r"foreign|plastic|metal|glass")],
        ["Bacterial Contamination", "Allergen Issue", "Labeling Issue", "Foreign Material"],
        default="Other"
    )

    product_text = results.get("product_description", "").fillna("").astype(str).str.lower()
    results["product_category"] = np.select(
        [
            product_text.str.contains(r"cheese|milk|cream|yogurt|butter|dairy"),
            product_text.str.contains(r"beef|chicken|pork|turkey|meat|sausage"),
            product_text.str.contains(r"fish|shrimp|crab|salmon|tuna|seafood"),
            product_text.str.contains(r"lettuce|spinach|salad|sprout|fruit|vegetable|produce"),
            product_text.str.contains(r"infant|baby|formula"),
            product_text.str.contains(r"supplement|vitamin|capsule|tablet"),
            product_text.str.contains(r"cookie|cake|bread|bakery|pastry"),
            product_text.str.contains(r"juice|drink|beverage|water"),
            product_text.str.contains(r"frozen|ice cream"),
        ],
        ["Dairy", "Meat & Poultry", "Seafood", "Produce", "Infant & Baby Food", "Supplements", "Bakery", "Beverages", "Frozen Foods"],
        default="Other",
    )

    distribution = results.get("distribution_pattern", "").fillna("").astype(str).str.lower()
    results["distribution_scope"] = np.select(
        [distribution.str.contains("nationwide|nationally|all states"), distribution.str.contains("international|canada|mexico|export"), distribution.str.contains(",")],
        ["Nationwide", "International", "Multi-State"],
        default="Local/Unknown",
    )

    state = results.get("state", "").fillna("").astype(str).str.strip().str.lower()
    region_map = {
        **dict.fromkeys(["ct", "me", "ma", "nh", "ri", "vt", "nj", "ny", "pa"], "Northeast"),
        **dict.fromkeys(["il", "in", "mi", "oh", "wi", "ia", "ks", "mn", "mo", "ne", "nd", "sd"], "Midwest"),
        **dict.fromkeys(["de", "fl", "ga", "md", "nc", "sc", "va", "dc", "wv", "al", "ky", "ms", "tn", "ar", "la", "ok", "tx"], "South"),
        **dict.fromkeys(["az", "co", "id", "mt", "nv", "nm", "ut", "wy", "ak", "ca", "hi", "or", "wa"], "West"),
    }
    results["region"] = state.map(region_map).fillna("Unknown")
    results["is_open"] = results["status"].fillna("").astype(str).str.lower().str.contains("ongoing")
    end_dates = results["termination_date"].fillna(results["report_date"])
    results["recall_duration_days"] = (end_dates - results["recall_initiation_date"]).dt.days

    columns = [
        "recall_number",
        "event_id",
        "status",
        "classification",
        "product_type",
        "recalling_firm",
        "recalling_firm_clean",
        "product_description",
        "reason_for_recall",
        "recall_reason_category",
        "product_category",
        "distribution_pattern",
        "distribution_scope",
        "state",
        "region",
        "country",
        "city",
        "postal_code",
        "product_quantity",
        "voluntary_mandated",
        "initial_firm_notification",
        "recall_initiation_date",
        "report_date",
        "termination_date",
        "is_open",
        "recall_duration_days",
    ]
    silver_df = results.reindex(columns=columns)

    output_path_obj = gold_output_path(output_path)
    if output_path_obj.suffix == ".parquet":
        silver_df.to_parquet(output_path_obj, index=False)
        print(f"Wrote silver parquet file to {output_path_obj} using pandas fallback")
    else:
        silver_df.to_parquet(output_path_obj / "part-00000.parquet", index=False)
        print(f"Wrote silver parquet to {output_path_obj}/part-00000.parquet using pandas fallback")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bronze_to_silver.py <input_json_path> [<output_parquet_path>]")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "data/silver/food_recalls"
    bronze_to_silver(input_path, output_path)

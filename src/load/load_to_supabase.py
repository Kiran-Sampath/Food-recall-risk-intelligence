import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests


TABLE_CONFIGS = {
    "recalls": {
        "path": "data/gold/food_recall_risk_scores",
        "conflict": "recall_number",
    },
    "company_risk": {
        "path": "data/gold/company_risk",
        "conflict": "recalling_firm_clean",
    },
    "recall_reason_summary": {
        "path": "data/gold/recall_reason_summary",
        "conflict": "recall_reason_category",
    },
    "monthly_recall_trends": {
        "path": "data/gold/monthly_recall_trends",
        "conflict": "recall_month",
    },
    "geographic_recall_summary": {
        "path": "data/gold/geographic_recall_summary",
        "conflict": "state,country",
    },
    "product_category_summary": {
        "path": "data/gold/product_category_summary",
        "conflict": "product_category",
    },
    "risk_tier_summary": {
        "path": "data/gold/risk_tier_summary",
        "conflict": "risk_tier",
    },
    "open_recall_aging": {
        "path": "data/gold/open_recall_aging",
        "conflict": "recall_number",
    },
}


def get_supabase_config():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
    return url.rstrip("/"), key


def read_parquet(path):
    candidate = Path(path)
    if not candidate.exists():
        candidate_with_ext = candidate.with_suffix(".parquet")
        if candidate_with_ext.exists():
            candidate = candidate_with_ext
        else:
            raise FileNotFoundError(f"Missing parquet input: {path}")
    return pd.read_parquet(candidate)


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def dataframe_to_records(df):
    records = []
    now = datetime.now(timezone.utc).isoformat()
    for row in df.to_dict(orient="records"):
        cleaned = {key: clean_value(value) for key, value in row.items()}
        cleaned["updated_at"] = now
        records.append(cleaned)
    return records


def prepare_conflict_keys(df, conflict):
    for column in conflict.split(","):
        if column in df.columns:
            df[column] = df[column].fillna("Unknown").replace("", "Unknown")
    return df


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def upsert_dataframe(base_url, service_key, table_name, df, conflict, batch_size):
    df = prepare_conflict_keys(df.copy(), conflict)
    records = dataframe_to_records(df)
    if not records:
        return 0

    endpoint = f"{base_url}/rest/v1/{table_name}"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": conflict}

    for batch in chunks(records, batch_size):
        response = requests.post(endpoint, headers=headers, params=params, json=batch, timeout=60)
        response.raise_for_status()
    return len(records)


def clear_table(base_url, service_key, table_name):
    endpoint = f"{base_url}/rest/v1/{table_name}"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Prefer": "return=minimal",
    }
    response = requests.delete(
        endpoint,
        headers=headers,
        params={"updated_at": "not.is.null"},
        timeout=60,
    )
    response.raise_for_status()


def load_tables(table_names=None, batch_size=500, replace=False):
    base_url, service_key = get_supabase_config()
    selected_tables = table_names or list(TABLE_CONFIGS.keys())
    loaded_counts = {}

    for table_name in selected_tables:
        config = TABLE_CONFIGS[table_name]
        df = read_parquet(config["path"])
        if replace:
            clear_table(base_url, service_key, table_name)
        loaded_counts[table_name] = upsert_dataframe(
            base_url=base_url,
            service_key=service_key,
            table_name=table_name,
            df=df,
            conflict=config["conflict"],
            batch_size=batch_size,
        )

    return loaded_counts


def load_pipeline_run(bronze_path, loaded_counts, status="success", error=None):
    base_url, service_key = get_supabase_config()
    meta = {}
    if bronze_path and Path(bronze_path).exists():
        with open(bronze_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh).get("meta", {})

    record = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "source": meta.get("source"),
        "bronze_path": bronze_path,
        "records_fetched": meta.get("records_fetched"),
        "records_loaded": loaded_counts.get("recalls", 0),
        "status": status,
        "warning": meta.get("warning"),
        "error": error,
    }
    endpoint = f"{base_url}/rest/v1/pipeline_runs"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    response = requests.post(endpoint, headers=headers, json=record, timeout=60)
    response.raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--bronze-path", type=str, default=None)
    parser.add_argument("--tables", nargs="*", choices=sorted(TABLE_CONFIGS.keys()))
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    loaded_counts = load_tables(table_names=args.tables, batch_size=args.batch_size, replace=args.replace)
    load_pipeline_run(args.bronze_path, loaded_counts)
    print({"status": "success", "loaded_counts": loaded_counts})


if __name__ == "__main__":
    main()

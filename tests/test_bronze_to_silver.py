import json

import pandas as pd

from src.transformations.bronze_to_silver import bronze_to_silver


def test_classification_product_and_distribution_enrichment(tmp_path, monkeypatch):
    import src.transformations.bronze_to_silver as transform

    monkeypatch.setattr(transform, "HAS_PYSPARK", False)

    bronze_path = tmp_path / "bronze.json"
    bronze_path.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "recall_number": "F-2",
                        "event_id": "100",
                        "status": "Ongoing",
                        "classification": "Class III",
                        "product_type": "Food",
                        "recalling_firm": "Example Dairy, Inc.",
                        "product_description": "Chocolate milk beverage",
                        "reason_for_recall": "Label error",
                        "distribution_pattern": "Nationwide distribution",
                        "state": "AZ",
                        "country": "United States",
                        "product_quantity": "10 cases",
                        "recall_initiation_date": "20260101",
                        "report_date": "20260115",
                        "termination_date": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "silver"
    bronze_to_silver(str(bronze_path), str(out_dir))

    result = pd.read_parquet(out_dir / "part-00000.parquet")
    row = result.iloc[0]
    assert row["classification"] == "Class III"
    assert row["classification"] != "Class I"
    assert row["product_category"] == "Dairy"
    assert row["distribution_scope"] == "Nationwide"
    assert row["region"] == "West"
    assert bool(row["is_open"])

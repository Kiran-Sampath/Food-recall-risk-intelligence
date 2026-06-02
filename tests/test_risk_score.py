from src.scoring.calc_utils import compute_risk_score
from src.scoring.calculate_risk_score import calculate_risk


def test_class_i_bacterial_nationwide():
    score = compute_risk_score("Class I", "Ongoing", "Nationwide distribution", "Bacterial Contamination", repeated_company_count=2)
    # classification 50 + status 20 + distribution 20 + reason 25 + repeated 4 = 119
    assert score == 119


def test_class_iii_labeling_local():
    score = compute_risk_score("Class III", "Terminated", "Local distribution", "Labeling Issue", repeated_company_count=0)
    # 10 + 0 + 0 + 10 = 20
    assert score == 20


def test_class_ii_scoring_does_not_match_class_i(tmp_path, monkeypatch):
    import pandas as pd
    import src.scoring.calculate_risk_score as scoring

    monkeypatch.setattr(scoring, "HAS_PYSPARK", False)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    pd.DataFrame(
        [
            {
                "recall_number": "F-1",
                "classification": "Class II",
                "status": "Terminated",
                "distribution_pattern": "Local distribution",
                "recall_reason_category": "Labeling Issue",
                "recalling_firm_clean": "example foods",
            }
        ]
    ).to_parquet(silver_dir / "part-00000.parquet", index=False)

    out_dir = tmp_path / "risk"
    calculate_risk(str(silver_dir), str(out_dir))

    result = pd.read_parquet(out_dir / "part-00000.parquet")
    assert result.loc[0, "classification_score"] == 30
    assert result.loc[0, "risk_score"] == 40
    assert result.loc[0, "risk_tier"] == "Medium"

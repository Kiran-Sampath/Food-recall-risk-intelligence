def compute_risk_score(classification: str, status: str, distribution_pattern: str, reason_category: str, repeated_company_count: int = 0) -> int:
    classification = (classification or "").strip()
    status = (status or "").strip().lower()
    distribution = (distribution_pattern or "").strip().lower()
    reason = (reason_category or "").strip()

    classification_score = 0
    if classification == "Class I":
        classification_score = 50
    elif classification == "Class II":
        classification_score = 30
    elif classification == "Class III":
        classification_score = 10

    status_score = 20 if status == "ongoing" else 0
    distribution_score = 20 if "nationwide" in distribution else 0

    reason_score = 5
    if reason == "Bacterial Contamination":
        reason_score = 25
    elif reason == "Allergen Issue":
        reason_score = 20
    elif reason == "Labeling Issue":
        reason_score = 10

    repeated_score = min(repeated_company_count * 2, 20)

    return classification_score + status_score + distribution_score + reason_score + repeated_score

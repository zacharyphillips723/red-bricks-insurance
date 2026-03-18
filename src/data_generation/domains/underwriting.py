# Red Bricks Insurance — underwriting domain (risk attributes tied to members for pricing/risk).

import random
from datetime import date
from typing import List, Dict, Any

from ..helpers import random_date_between

# Underwriting attributes that tie to claims/clinical: risk tier, medical history flags,
# lifestyle (smoker, BMI band), occupation class. Use for pricing and risk studies.


def generate_underwriting(
    member_ids: List[str],
    risk_score_by_member: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Generate underwriting records per member. Ties to risk_score (e.g. from enrollment/RAF).
    risk_score_by_member: optional member_id -> risk_score for correlation.
    """
    records = []
    for member_id in member_ids:
        risk_score = risk_score_by_member.get(member_id)
        if risk_score is not None and risk_score > 2.0:
            risk_tier = random.choices(["Standard", "Preferred", "Substandard"], weights=[20, 10, 70], k=1)[0]
        else:
            risk_tier = random.choices(["Standard", "Preferred", "Substandard"], weights=[50, 40, 10], k=1)[0]
        smoker = random.choices(["Y", "N"], weights=[15, 85], k=1)[0]
        bmi_band = random.choices(
            ["underweight", "normal", "overweight", "obese"],
            weights=[5, 35, 35, 25], k=1
        )[0]
        occupation_class = random.choices(
            ["Professional", "Office", "Light", "Heavy", "Hazardous"],
            weights=[25, 40, 20, 10, 5], k=1
        )[0]
        medical_history_flag = random.choices([True, False], weights=[30, 70], k=1)[0]
        effective_date = random_date_between(date(2023, 1, 1), date(2024, 6, 1)).isoformat()
        records.append({
            "member_id": member_id,
            "risk_tier": risk_tier,
            "smoker_indicator": smoker,
            "bmi_band": bmi_band,
            "occupation_class": occupation_class,
            "medical_history_indicator": medical_history_flag,
            "underwriting_effective_date": effective_date,
        })
    return records

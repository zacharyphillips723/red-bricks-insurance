# Red Bricks Insurance — risk adjustment domain (RAF, HCC, model year; member and provider level).

import random
from datetime import date
from typing import List, Dict, Any

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import random_date_between

# Risk scores and HCCs tie to members (and can be aggregated to providers for analysis).


def generate_risk_adjustment_member(
    member_ids: List[str],
    model_year: int = 2024,
) -> List[Dict[str, Any]]:
    """Generate member-level risk adjustment (RAF, HCC codes, model year)."""
    records = []
    hcc_list = reference_data.HCC_CODES
    for member_id in member_ids:
        num_hcc = random.choices([0, 1, 2, 3], weights=[50, 30, 15, 5], k=1)[0]
        hccs = random.sample(hcc_list, min(num_hcc, len(hcc_list)))
        # RAF ≈ 1.0 + sum of HCC factors + age/sex component (simplified)
        base_raf = round(random.uniform(0.6, 1.2), 3)
        hcc_raf = sum(h[2] for h in hccs)
        raf_score = round(base_raf + hcc_raf, 3)
        hcc_codes = ",".join(h[0] for h in hccs) if hccs else None
        measurement_date = random_date_between(date(2023, 7, 1), date(2024, 6, 30)).isoformat()
        records.append({
            "member_id": member_id,
            "model_year": model_year,
            "raf_score": inject_dq_issue(raf_score, "amount"),
            "hcc_codes": hcc_codes,
            "measurement_period_start": "2023-07-01",
            "measurement_period_end": "2024-06-30",
            "measurement_date": measurement_date,
        })
    return records


def generate_risk_adjustment_provider(
    provider_npis: List[str],
    member_raf_records: List[Dict[str, Any]],
    n_assignments: int = 3000,
) -> List[Dict[str, Any]]:
    """
    Assign members to providers and aggregate RAF at provider level (e.g. for MA quality).
    member_raf_records: from generate_risk_adjustment_member (member_id, raf_score).
    """
    member_raf = {r["member_id"]: r["raf_score"] for r in member_raf_records if r.get("raf_score") is not None}
    provider_npis = [n for n in provider_npis if n]
    if not provider_npis:
        return []
    assignments = []
    member_ids = list(member_raf.keys())
    for _ in range(min(n_assignments, len(member_ids) * 2)):
        member_id = random.choice(member_ids)
        npi = random.choice(provider_npis)
        raf = member_raf.get(member_id, 1.0)
        assignments.append({
            "provider_npi": npi,
            "member_id": member_id,
            "raf_score": raf,
            "attribution_date": random_date_between(date(2023, 1, 1), date(2024, 12, 31)).isoformat(),
        })
    return assignments

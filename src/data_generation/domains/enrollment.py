# Red Bricks Insurance — enrollment and plan domain (member_id, plan, LOB, dates).

import random
from datetime import date, timedelta
from typing import Any, Dict, List

from faker import Faker

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import random_date_between, weighted_choice, COUNTIES

fake = Faker("en_US")


def generate_enrollment(member_ids: List[str]) -> List[Dict[str, Any]]:
    """Generate enrollment/plan records for each member (one record per member)."""
    enrollments = []
    lob_names = list(reference_data.LOB_CONFIG.keys())
    lob_weights = [reference_data.LOB_CONFIG[l]["weight"] for l in lob_names]

    for member_id in member_ids:
        lob = weighted_choice(lob_names, lob_weights)
        config = reference_data.LOB_CONFIG.get(lob, reference_data.LOB_CONFIG["Commercial"])
        plan_type = random.choice(config["plan_types"])
        premium = round(
            random.uniform(config["premium_range"][0], config["premium_range"][1]), 2
        )
        elig_start = random_date_between(date(2023, 1, 1), date(2024, 6, 1))
        elig_start = elig_start.replace(day=1)
        if random.random() < 0.75:
            elig_end = date(2026, 12, 31)
        else:
            months_enrolled = random.randint(3, 24)
            elig_end = elig_start + timedelta(days=months_enrolled * 30)
            if elig_end.month == 12:
                elig_end = elig_end.replace(day=31)
            else:
                elig_end = (elig_end.replace(month=elig_end.month + 1, day=1) - timedelta(days=1))
        rating_area = f"NC-{random.randint(1, 8):02d}"
        risk_score = round(random.uniform(0.3, 4.5), 3) if lob == "Medicare Advantage" else round(random.uniform(0.3, 3.0), 3)

        rec = {
            "member_id": member_id,
            "subscriber_id": f"SUB{random.randint(100000, 199999)}",
            "relationship": random.choices(
                ["Self", "Spouse", "Child", "Domestic Partner"],
                weights=[60, 20, 18, 2], k=1
            )[0],
            "line_of_business": lob,
            "plan_type": plan_type,
            "plan_id": f"PLN-{lob[:3].upper()}-{random.randint(1000, 9999)}",
            "group_number": f"GRP{random.randint(10000, 99999)}" if lob == "Commercial" else None,
            "group_name": fake.company() if lob == "Commercial" else None,
            "eligibility_start_date": elig_start.isoformat(),
            "eligibility_end_date": inject_dq_issue(elig_end.isoformat(), "date"),
            "monthly_premium": inject_dq_issue(premium, "amount"),
            "rating_area": rating_area,
            "risk_score": risk_score,
            "metal_level": plan_type if lob == "ACA Marketplace" else None,
        }
        enrollments.append(rec)
    return enrollments

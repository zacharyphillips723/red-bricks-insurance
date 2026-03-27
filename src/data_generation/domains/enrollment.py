# Red Bricks Insurance — enrollment and plan domain (member_id, plan, LOB, dates).

import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from faker import Faker

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import random_date_between, weighted_choice, COUNTIES

fake = Faker("en_US")


def generate_enrollment(
    member_ids: List[str],
    member_lob_map: Optional[Dict[str, str]] = None,
    group_data: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Generate enrollment/plan records for each member (one record per member).

    When member_lob_map is provided, uses the pre-assigned LOB for each member
    (ensuring age-consistent assignment from Synthea demographics).
    When group_data is provided, assigns Commercial members to employer groups
    based on group capacity instead of random group_number/group_name.
    """
    enrollments = []
    lob_names = list(reference_data.LOB_CONFIG.keys())
    lob_weights = [reference_data.LOB_CONFIG[l]["weight"] for l in lob_names]

    # Build group assignment pool: repeat each group_id proportional to group_size
    group_pool: List[Dict[str, Any]] = []
    if group_data:
        for g in group_data:
            group_pool.extend([g] * g["group_size"])
        random.shuffle(group_pool)
    group_idx = 0

    for member_id in member_ids:
        if member_lob_map and member_id in member_lob_map:
            lob = member_lob_map[member_id]
        else:
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

        # Assign group for Commercial members
        if lob == "Commercial" and group_pool:
            grp = group_pool[group_idx % len(group_pool)]
            group_idx += 1
            group_number = grp["group_id"]
            group_name = grp["group_name"]
        elif lob == "Commercial":
            group_number = f"GRP{random.randint(10000, 99999)}"
            group_name = fake.company()
        else:
            group_number = None
            group_name = None

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
            "group_number": group_number,
            "group_name": group_name,
            "eligibility_start_date": elig_start.isoformat(),
            "eligibility_end_date": inject_dq_issue(elig_end.isoformat(), "date"),
            "monthly_premium": inject_dq_issue(premium, "amount"),
            "rating_area": rating_area,
            "risk_score": risk_score,
            "metal_level": plan_type if lob == "ACA Marketplace" else None,
        }
        enrollments.append(rec)
    return enrollments

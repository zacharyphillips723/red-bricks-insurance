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
    using a two-pass approach: first guarantees every group gets at least
    MIN_PER_GROUP members, then distributes remaining members proportionally.
    """
    enrollments = []
    lob_names = list(reference_data.LOB_CONFIG.keys())
    lob_weights = [reference_data.LOB_CONFIG[l]["weight"] for l in lob_names]

    # Pre-determine LOB for all members so we know who is Commercial up front
    member_lob_cache: Dict[str, str] = {}
    for mid in member_ids:
        if member_lob_map and mid in member_lob_map:
            member_lob_cache[mid] = member_lob_map[mid]
        else:
            member_lob_cache[mid] = weighted_choice(lob_names, lob_weights)

    # Build group assignment: two-pass approach ensures every group gets members.
    # Pass 1: guarantee each group gets at least min(group_size, MIN_PER_GROUP).
    # Pass 2: distribute remaining members proportionally by group_size.
    MIN_PER_GROUP = 5
    group_assignments: Dict[str, Dict[str, Any]] = {}  # member_id -> group dict
    if group_data:
        actual_commercial = [
            mid for mid in member_ids if member_lob_cache[mid] == "Commercial"
        ]
        random.shuffle(actual_commercial)
        idx = 0

        # Pass 1: guarantee minimum members per group
        for g in group_data:
            guaranteed = min(g["group_size"], MIN_PER_GROUP)
            for _ in range(guaranteed):
                if idx < len(actual_commercial):
                    group_assignments[actual_commercial[idx]] = g
                    idx += 1

        # Pass 2: distribute remaining members proportionally by group_size
        remaining_members = actual_commercial[idx:]
        if remaining_members:
            total_remaining_capacity = sum(
                max(g["group_size"] - MIN_PER_GROUP, 0) for g in group_data
            )
            if total_remaining_capacity > 0:
                proportional_pool: List[Dict[str, Any]] = []
                for g in group_data:
                    extra = max(g["group_size"] - MIN_PER_GROUP, 0)
                    proportional_pool.extend([g] * extra)
                random.shuffle(proportional_pool)
                for i, mid in enumerate(remaining_members):
                    group_assignments[mid] = proportional_pool[i % len(proportional_pool)]

    for member_id in member_ids:
        lob = member_lob_cache[member_id]
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
        if lob == "Commercial" and member_id in group_assignments:
            grp = group_assignments[member_id]
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

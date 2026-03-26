# Red Bricks Insurance — member domain (demographics only).

import random
from datetime import date, timedelta
from typing import List, Dict, Any, Optional

from faker import Faker

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import random_date_between, weighted_choice, COUNTIES

fake = Faker("en_US")


def generate_members(
    n: int = 5000,
    seed: int = 42,
    synthea_demographics: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Generate member demographic records (member_id, name, DOB, gender, address).

    When synthea_demographics is provided, uses Synthea-generated names, DOBs, and
    addresses instead of Faker. The n parameter is ignored in that case.
    """
    random.seed(seed)
    Faker.seed(seed)
    members = []

    if synthea_demographics:
        # Use Synthea as the demographic source
        for rec in synthea_demographics:
            member = {
                "member_id": rec["member_id"],
                "last_name": inject_dq_issue(rec["last_name"] or fake.last_name(), "code"),
                "first_name": rec["first_name"] or fake.first_name(),
                "date_of_birth": inject_dq_issue(rec["date_of_birth"] or date(1980, 1, 1).isoformat(), "date"),
                "gender": rec.get("gender", random.choice(["M", "F"])),
                "ssn_last_4": f"{random.randint(1000, 9999)}",
                "address_line_1": rec.get("address_line_1") or fake.street_address(),
                "city": rec.get("city") or fake.city(),
                "state": rec.get("state", "NC") or "NC",
                "zip_code": rec.get("zip_code") or fake.zipcode_in_state("NC"),
                "county": random.choice(COUNTIES),
                "phone": fake.phone_number(),
                "email": fake.email(),
            }
            members.append(member)
    else:
        # Fallback: original Faker-based generation
        lob_names = list(reference_data.LOB_CONFIG.keys())
        lob_weights = [reference_data.LOB_CONFIG[l]["weight"] for l in lob_names]

        for i in range(n):
            member_id = f"MBR{100000 + i}"
            lob = weighted_choice(lob_names, lob_weights)
            config = reference_data.LOB_CONFIG[lob]
            age = random.randint(config["age_range"][0], config["age_range"][1])
            dob = date(2025, 1, 1) - timedelta(days=age * 365 + random.randint(0, 364))
            gender = random.choice(["M", "F"])
            county = random.choice(COUNTIES)

            member = {
                "member_id": member_id,
                "last_name": inject_dq_issue(fake.last_name(), "code"),
                "first_name": fake.first_name_male() if gender == "M" else fake.first_name_female(),
                "date_of_birth": inject_dq_issue(dob.isoformat(), "date"),
                "gender": gender,
                "ssn_last_4": f"{random.randint(1000, 9999)}",
                "address_line_1": fake.street_address(),
                "city": fake.city(),
                "state": "NC",
                "zip_code": fake.zipcode_in_state("NC"),
                "county": county,
                "phone": fake.phone_number(),
                "email": fake.email(),
            }
            members.append(member)
    return members

# Red Bricks Insurance — member domain (demographics only).

import random
from datetime import date, timedelta
from typing import List, Dict, Any

from faker import Faker

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import random_date_between, weighted_choice, COUNTIES

fake = Faker("en_US")


def generate_members(n: int = 5000, seed: int = 42) -> List[Dict[str, Any]]:
    """Generate member demographic records (member_id, name, DOB, gender, address)."""
    random.seed(seed)
    Faker.seed(seed)
    members = []
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

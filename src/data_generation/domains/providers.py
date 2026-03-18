# Red Bricks Insurance — provider domain (names, addresses, specialties, NPI).

import random
from datetime import date, timedelta
from typing import List, Dict, Any

from faker import Faker

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import generate_npi, random_date_between, weighted_choice, COUNTIES

fake = Faker("en_US")


def generate_providers(n: int = 500) -> List[Dict[str, Any]]:
    """Generate provider directory records (NPI, name, specialty, address, network, etc.)."""
    providers = []
    specialty_names = [s[0] for s in reference_data.SPECIALTIES]
    specialty_weights = [s[1] for s in reference_data.SPECIALTIES]

    for i in range(n):
        npi = generate_npi()
        specialty = weighted_choice(specialty_names, specialty_weights)
        first_name = fake.first_name()
        last_name = fake.last_name()
        network_status = random.choices(
            ["In-Network", "Out-of-Network"], weights=[85, 15], k=1
        )[0]
        eff_date = random_date_between(date(2018, 1, 1), date(2023, 6, 30))
        term_date = None
        if random.random() < 0.08:
            term_date = random_date_between(
                eff_date + timedelta(days=365), date(2025, 12, 31)
            )
        county = random.choice(COUNTIES)
        zip_code = fake.zipcode_in_state("NC")
        tax_id = f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}"
        group_name = random.choice([
            f"{county} Medical Group",
            f"Carolina {specialty} Partners",
            fake.company() + " Medical",
        ])

        provider = {
            "npi": inject_dq_issue(npi, "code"),
            "provider_first_name": first_name,
            "provider_last_name": last_name,
            "provider_name": f"{last_name}, {first_name}",
            "credential": random.choice(["MD", "DO", "MD", "NP", "PA"]),
            "specialty": specialty,
            "taxonomy_code": f"{random.randint(100, 399)}X00000X",
            "tax_id": tax_id,
            "group_name": group_name,
            "network_status": network_status,
            "effective_date": inject_dq_issue(eff_date.isoformat(), "date"),
            "termination_date": term_date.isoformat() if term_date else None,
            "address_line_1": fake.street_address(),
            "city": fake.city(),
            "state": "NC",
            "zip_code": zip_code,
            "county": county,
            "phone": fake.phone_number(),
        }
        providers.append(provider)
    return providers

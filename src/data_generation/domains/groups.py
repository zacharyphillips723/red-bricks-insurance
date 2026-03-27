# Red Bricks Insurance — employer group domain (group contracts, stop-loss, funding).

import random
from datetime import date, timedelta
from typing import List, Dict, Any

from faker import Faker

from ..helpers import random_date_between

fake = Faker("en_US")

# Industry reference data (SIC code, industry name, typical group size range)
INDUSTRIES = [
    ("2011", "Manufacturing - Food Processing", (100, 3000)),
    ("2711", "Manufacturing - Publishing", (50, 500)),
    ("3559", "Manufacturing - Industrial Machinery", (200, 2000)),
    ("3674", "Manufacturing - Semiconductors", (500, 5000)),
    ("4011", "Transportation - Railroads", (1000, 8000)),
    ("4512", "Transportation - Air Transportation", (500, 5000)),
    ("4813", "Telecommunications", (200, 3000)),
    ("5065", "Wholesale - Electronic Parts", (50, 400)),
    ("5411", "Retail - Grocery Stores", (100, 2000)),
    ("5812", "Retail - Restaurants", (50, 1000)),
    ("5912", "Retail - Drug Stores", (200, 1500)),
    ("6021", "Finance - National Banks", (500, 10000)),
    ("6141", "Finance - Personal Credit", (100, 800)),
    ("6311", "Insurance - Life Insurance", (200, 3000)),
    ("6411", "Insurance - Agents & Brokers", (50, 300)),
    ("6512", "Real Estate - Operators", (100, 500)),
    ("7011", "Hospitality - Hotels & Motels", (100, 2000)),
    ("7372", "Technology - Software", (50, 5000)),
    ("7374", "Technology - Data Processing", (100, 1500)),
    ("7389", "Services - Business Services", (50, 800)),
    ("8011", "Healthcare - Physicians", (50, 500)),
    ("8051", "Healthcare - Skilled Nursing", (100, 1000)),
    ("8062", "Healthcare - Hospitals", (500, 5000)),
    ("8071", "Healthcare - Medical Labs", (50, 400)),
    ("8211", "Education - Elementary & Secondary", (200, 3000)),
    ("8221", "Education - Colleges & Universities", (500, 8000)),
    ("8711", "Engineering Services", (100, 1000)),
    ("8742", "Management Consulting", (50, 500)),
    ("9111", "Government - Executive Offices", (200, 5000)),
    ("9199", "Government - General", (500, 10000)),
]

# Funding type distribution varies by group size
FUNDING_BY_SIZE = {
    "Small (1-50)":      {"Fully-Insured": 70, "Level-Funded": 25, "Self-Funded": 5},
    "Medium (51-250)":   {"Fully-Insured": 40, "Level-Funded": 30, "Self-Funded": 30},
    "Large (251-1000)":  {"Fully-Insured": 15, "Level-Funded": 15, "Self-Funded": 70},
    "Jumbo (1000+)":     {"Fully-Insured": 5,  "Level-Funded": 5,  "Self-Funded": 90},
}

# Stop-loss attachment points vary by funding type and group size
SPECIFIC_STOP_LOSS = {
    "Small (1-50)":      (75_000, 150_000),
    "Medium (51-250)":   (100_000, 250_000),
    "Large (251-1000)":  (150_000, 500_000),
    "Jumbo (1000+)":     (250_000, 1_000_000),
}

STATES = ["NC", "SC", "VA", "GA", "TN", "FL", "TX", "NY", "CA", "IL", "PA", "OH", "MI", "NJ", "MA"]


def _size_tier(n: int) -> str:
    if n <= 50:
        return "Small (1-50)"
    elif n <= 250:
        return "Medium (51-250)"
    elif n <= 1000:
        return "Large (251-1000)"
    return "Jumbo (1000+)"


def generate_groups(n: int = 200, seed: int = 42) -> List[Dict[str, Any]]:
    """Generate employer group contracts with stop-loss, funding type, and admin fees."""
    random.seed(seed)
    Faker.seed(seed)
    groups = []

    for i in range(n):
        industry = random.choice(INDUSTRIES)
        sic_code, industry_name, (size_low, size_high) = industry

        # Group size — skewed toward smaller groups (realistic distribution)
        r = random.random()
        if r < 0.35:
            group_size = random.randint(10, 50)
        elif r < 0.65:
            group_size = random.randint(51, 250)
        elif r < 0.85:
            group_size = random.randint(251, 1000)
        else:
            group_size = random.randint(1001, max(1001, min(size_high, 5000)))

        size_tier = _size_tier(group_size)

        # Funding type based on group size
        funding_dist = FUNDING_BY_SIZE[size_tier]
        funding_type = random.choices(
            list(funding_dist.keys()),
            weights=list(funding_dist.values()),
            k=1,
        )[0]

        # Stop-loss (only for Self-Funded and Level-Funded)
        has_stop_loss = funding_type in ("Self-Funded", "Level-Funded")
        sl_range = SPECIFIC_STOP_LOSS[size_tier]
        specific_sl = float(round(random.randint(sl_range[0], sl_range[1]) / 5000) * 5000) if has_stop_loss else None
        aggregate_sl_pct = round(random.uniform(1.15, 1.40), 2) if has_stop_loss else None

        # Expected annual claims (PMPM * 12 * group_size)
        base_pmpm = random.uniform(350, 650)
        expected_annual_claims = round(base_pmpm * 12 * group_size, 2)

        # Admin fee (PMPM) — Self-Funded pays explicit admin fees
        if funding_type == "Self-Funded":
            admin_fee_pmpm = round(random.uniform(25, 75), 2)
        elif funding_type == "Level-Funded":
            admin_fee_pmpm = round(random.uniform(30, 60), 2)
        else:
            admin_fee_pmpm = round(random.uniform(0, 15), 2)  # embedded in premium

        # Stop-loss premium (PMPM) — cost of reinsurance
        if has_stop_loss:
            stop_loss_premium_pmpm = round(random.uniform(15, 85), 2)
        else:
            stop_loss_premium_pmpm = None

        # Contract dates
        effective_date = date(random.choice([2023, 2024, 2025]), random.choice([1, 4, 7, 10]), 1)
        renewal_date = date(effective_date.year + 1, effective_date.month, effective_date.day)

        group_id = f"GRP-{i + 1:04d}"
        group_name = fake.company()
        state = random.choice(STATES)

        groups.append({
            "group_id": group_id,
            "group_name": group_name,
            "sic_code": sic_code,
            "industry": industry_name,
            "state": state,
            "group_size": group_size,
            "group_size_tier": size_tier,
            "funding_type": funding_type,
            "specific_stop_loss_attachment": specific_sl,
            "aggregate_stop_loss_attachment_pct": aggregate_sl_pct,
            "expected_annual_claims": expected_annual_claims,
            "admin_fee_pmpm": admin_fee_pmpm,
            "stop_loss_premium_pmpm": stop_loss_premium_pmpm,
            "effective_date": effective_date.isoformat(),
            "renewal_date": renewal_date.isoformat(),
        })

    return groups

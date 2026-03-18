# Red Bricks Insurance — shared generation helpers.

import random
from datetime import date, timedelta
from typing import List, Sequence, Tuple, TypeVar

T = TypeVar("T")


def generate_npi() -> str:
    """10-digit NPI (1 + 9 digits)."""
    return f"1{random.randint(100000000, 999999999)}"


def generate_claim_id(prefix: str = "MC") -> str:
    """Claim ID with optional prefix (MC, PH, RX)."""
    return f"{prefix}{random.randint(100000000, 999999999)}"


def weighted_choice(items: Sequence[T], weights: Sequence[float]) -> T:
    """Select one item by weight."""
    return random.choices(items, weights=weights, k=1)[0]


def random_date_between(start: date, end: date) -> date:
    """Random date in [start, end] inclusive."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(0, delta)))


def apply_payment_lag(service_date: date) -> date:
    """Realistic IBNR-style payment lag (mix of 7–365 days)."""
    r = random.random()
    if r < 0.40:
        lag = random.randint(7, 14)
    elif r < 0.65:
        lag = random.randint(15, 30)
    elif r < 0.80:
        lag = random.randint(31, 60)
    elif r < 0.90:
        lag = random.randint(61, 90)
    elif r < 0.97:
        lag = random.randint(91, 180)
    else:
        lag = random.randint(181, 365)
    return service_date + timedelta(days=lag)


# Counties for geography (NC-style)
COUNTIES = [
    "Wake", "Mecklenburg", "Guilford", "Forsyth", "Durham",
    "Cumberland", "Buncombe", "New Hanover", "Gaston", "Cabarrus",
]

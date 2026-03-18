# Red Bricks Insurance — data quality injection for testing expectations in SDP.
"""Inject a small rate of bad data (nulls, out-of-range, invalid codes) for DQ pipelines."""

import random
from datetime import date
from typing import Any, Optional

# Default rate ~2%; can be overridden per call
DEFAULT_DQ_RATE = 0.02


def inject_dq_issue(
    value: Any,
    field_type: str,
    rate: float = DEFAULT_DQ_RATE,
) -> Optional[Any]:
    """
    With probability `rate`, return a defective value; otherwise return `value`.
    field_type: "date" | "amount" | "code" | "id"
    """
    if random.random() > rate:
        return value

    issue_type = random.choice(["null", "out_of_range", "invalid"])

    if issue_type == "null":
        return None

    if field_type == "date":
        return date(random.choice([1900, 2099]), 1, 1).isoformat()
    if field_type == "amount":
        return -abs(float(value)) if value is not None else -999.99
    if field_type == "code":
        return "INVALID"
    if field_type == "id":
        return ""

    return None

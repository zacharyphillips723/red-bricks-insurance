"""
Genie Space Setup — Group Reporting Portal

Run this script after the gold analytics pipeline has materialized the group tables.
Creates a Genie Space configured with group-level analytics tables for
account executives and sales reps.
"""

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
import os, sys
CATALOG = "red_bricks_insurance"

TABLES = [
    f"{CATALOG}.analytics.gold_group_report_card",
    f"{CATALOG}.analytics.gold_group_experience",
    f"{CATALOG}.analytics.gold_group_renewal",
    f"{CATALOG}.analytics.gold_group_stop_loss",
    f"{CATALOG}.analytics.gold_tcoc_summary",
]

# ---------------------------------------------------------------------------
# Sample questions
# ---------------------------------------------------------------------------
SAMPLE_QUESTIONS = [
    # Group Overview
    "Which groups have a loss ratio above 100%?",
    "Show me all groups that need a rate increase this year",
    "What are the top 10 groups by total claims paid?",
    "Which groups have the highest health scores?",

    # Financial
    "What is the average claims PMPM by industry?",
    "Show groups with the highest pharmacy PMPM",
    "Which funding type has the best loss ratios?",
    "Compare medical vs pharmacy spend across group size tiers",

    # Renewal
    "Which groups have renewal dates in the next 90 days?",
    "Show projected renewal PMPM by group size tier",
    "What is the average actual-to-expected ratio by industry?",
    "Which groups have the lowest credibility factors?",

    # Stop-Loss
    "Which groups have members exceeding the specific stop-loss attachment?",
    "Show groups where the aggregate attachment ratio exceeds 0.9",
    "What is the total specific stop-loss excess across all groups?",

    # Cost of Care
    "Which LOB has the highest average TCOC?",
    "Show the cost tier distribution by line of business",
    "What percentage of total spend comes from the top 5% of members?",

    # Peer Comparison
    "Which groups perform better than their industry peers on loss ratio?",
    "Show me groups in the bottom quartile for ER visits per 1000",
]

INSTRUCTIONS = """You are an analytics assistant for Red Bricks Insurance, focused on employer group reporting and renewal analytics.

Key concepts:
- **PMPM** = Per Member Per Month (total cost / member months)
- **Loss Ratio** = Claims Paid / Premium Revenue (>1.0 means claims exceed premiums)
- **TCI (Total Cost Index)** = Member actual cost / LOB average cost (>1.0 = above average)
- **TCOC (Total Cost of Care)** = Risk-adjusted cost per member month
- **Specific Stop-Loss** = Per-claimant threshold; excess above this is reinsured
- **Aggregate Stop-Loss** = Total group threshold; ratio > 1.0 means group exceeded limit
- **Health Score** = Composite 1-100 score (higher is better); based on loss ratio, ER utilization, and TCI

When answering:
- Always format currency values with $ and appropriate decimal places
- Express loss ratios and percentiles as percentages
- When comparing groups, include both the metric value and peer context
- Flag groups that need attention (high loss ratio, approaching stop-loss limits, etc.)
"""


# ---------------------------------------------------------------------------
# Create the Genie Space
# ---------------------------------------------------------------------------

def create_genie_space() -> str:
    """Create the Group Reporting Genie Space and return its ID."""
    w = WorkspaceClient()

    space = w.genie.create_space(
        title="Red Bricks Insurance — Group Analytics",
        description=(
            "Natural language analytics for employer group reporting: "
            "experience metrics, renewal projections, stop-loss exposure, "
            "cost tier distributions, and peer benchmarks."
        ),
        table_identifiers=TABLES,
    )

    space_id = space.space_id
    print(f"Genie Space created: {space_id}")
    print(f"URL: {w.config.host}/genie/rooms/{space_id}")
    print("\nNext steps:")
    print("  1. Open the Genie Space URL above")
    print("  2. Click the gear icon -> 'General Instructions'")
    print("  3. Paste the instructions from the INSTRUCTIONS variable above")
    print("  4. Add sample questions from the SAMPLE_QUESTIONS list")
    print(f"  5. Update GENIE_SPACE_ID in app-group-reporting/app.yml to: {space_id}")

    return space_id


if __name__ == "__main__":
    create_genie_space()

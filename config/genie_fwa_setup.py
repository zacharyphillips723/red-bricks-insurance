"""Genie Space setup for FWA Investigation Portal.

Creates a Genie space configured with FWA gold tables and sample questions
for natural language exploration of fraud, waste, and abuse data.
"""

from databricks.sdk import WorkspaceClient

import os, sys
CATALOG = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("UC_CATALOG", "red_bricks_insurance")

# Tables to register with the Genie space
FWA_TABLES = [
    f"{CATALOG}.fwa.gold_fwa_provider_risk",
    f"{CATALOG}.fwa.gold_fwa_claim_flags",
    f"{CATALOG}.fwa.gold_fwa_summary",
    f"{CATALOG}.fwa.silver_fwa_signals",
    f"{CATALOG}.fwa.silver_fwa_investigation_cases",
    f"{CATALOG}.analytics.gold_fwa_member_risk",
    f"{CATALOG}.analytics.gold_fwa_network_analysis",
    f"{CATALOG}.analytics.fwa_model_inference",
    f"{CATALOG}.claims.silver_claims_medical",
    f"{CATALOG}.members.silver_enrollment",
    f"{CATALOG}.providers.silver_providers",
]

SAMPLE_QUESTIONS = [
    "Which providers have the highest composite risk score?",
    "Show me the top 10 providers by estimated overpayment",
    "How many flagged claims are there by fraud type?",
    "What is the total estimated overpayment by line of business?",
    "Which investigations are Critical severity and still Open?",
    "Compare average ML fraud probability by provider specialty",
    "Show members with the highest doctor shopping scores",
    "What fraud types are most common in the Medicare line of business?",
    "Which providers have both high rules-based flags and high ML model scores?",
    "Show the distribution of investigation statuses",
    "What is the average fraud score by detection method?",
    "List providers in the highest risk tier with more than 20 FWA signals",
    "Show monthly trends of estimated overpayment from the summary table",
    "Which provider referral pairs have the most shared members?",
]

DESCRIPTION = """FWA (Fraud, Waste & Abuse) Investigation Data Explorer

This Genie space provides natural language access to Red Bricks Insurance's
FWA detection and investigation data, including:

- Provider risk scorecards with composite risk scores
- Flagged claims with rules-based and ML-based fraud signals
- ML model fraud probability predictions per claim
- Member-level fraud risk indicators (doctor shopping, pharmacy abuse)
- Provider referral network analysis
- Investigation case records and status tracking
- Aggregate FWA metrics by fraud type, severity, and line of business

Data flows through a medallion architecture (bronze → silver → gold) with
data quality expectations at each layer. ML model predictions are generated
by an AutoML-trained fraud scoring model registered in Unity Catalog."""


def create_genie_space(
    warehouse_id: str = "",
    space_name: str = "Red Bricks FWA Investigation",
):
    """Create or update the FWA Genie space."""
    w = WorkspaceClient()

    print(f"Creating Genie space: {space_name}")
    print(f"  Warehouse: {warehouse_id}")
    print(f"  Tables: {len(FWA_TABLES)}")
    print(f"  Sample questions: {len(SAMPLE_QUESTIONS)}")

    # Note: Genie space creation via SDK may require specific API version.
    # This provides the configuration for manual or API-based creation.
    config = {
        "space_name": space_name,
        "description": DESCRIPTION,
        "warehouse_id": warehouse_id,
        "tables": FWA_TABLES,
        "sample_questions": SAMPLE_QUESTIONS,
    }

    print("\nGenie space configuration:")
    print(f"  Name: {config['space_name']}")
    print(f"  Tables ({len(config['tables'])}):")
    for t in config["tables"]:
        print(f"    - {t}")
    print(f"  Sample Questions ({len(config['sample_questions'])}):")
    for q in config["sample_questions"]:
        print(f"    - {q}")

    return config


if __name__ == "__main__":
    create_genie_space()

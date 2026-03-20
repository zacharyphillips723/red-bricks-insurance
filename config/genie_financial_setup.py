"""
Genie Space Setup — Red Bricks Insurance Financial Analytics

Run this script after the gold analytics pipeline (including actuarial_metrics.sql)
has materialized all tables. Creates a Genie Space focused on financial KPIs,
actuarial metrics, utilization benchmarking, and IBNR reserve analysis.
"""

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = "catalog_insurance_vpx9o6"
SCHEMA = "red_bricks_insurance_dev"

TABLES = [
    # Core financial metrics
    f"{CATALOG}.{SCHEMA}.gold_pmpm",
    f"{CATALOG}.{SCHEMA}.gold_mlr",
    f"{CATALOG}.{SCHEMA}.gold_mlr_ai_insights",

    # Actuarial / utilization
    f"{CATALOG}.{SCHEMA}.gold_utilization_per_1000",
    f"{CATALOG}.{SCHEMA}.gold_ibnr_estimate",
    f"{CATALOG}.{SCHEMA}.gold_ibnr_triangle",
    f"{CATALOG}.{SCHEMA}.gold_ibnr_completion_factors",

    # Denial analysis (financial impact)
    f"{CATALOG}.{SCHEMA}.gold_denial_analysis",
    f"{CATALOG}.{SCHEMA}.gold_claims_summary",

    # Enrollment & exposure
    f"{CATALOG}.{SCHEMA}.gold_enrollment_summary",
    f"{CATALOG}.{SCHEMA}.silver_member_months",
    f"{CATALOG}.{SCHEMA}.silver_enrollment",

    # Supporting drill-down
    f"{CATALOG}.{SCHEMA}.silver_claims_medical",
    f"{CATALOG}.{SCHEMA}.silver_claims_pharmacy",
    f"{CATALOG}.{SCHEMA}.gold_pharmacy_summary",
]

# ---------------------------------------------------------------------------
# Sample questions — financial and actuarial focus
# ---------------------------------------------------------------------------
SAMPLE_QUESTIONS = [
    # PMPM & Cost Trends
    "What is the PMPM trend by line of business over the last 12 months?",
    "Which LOB has the highest PMPM and what is driving it?",
    "How does paid PMPM compare to allowed PMPM by LOB?",

    # Medical Loss Ratio
    "What is our MLR by line of business and are we ACA compliant?",
    "Which LOBs are at rebate risk below the MLR threshold?",
    "Show the AI-recommended actions for each LOB based on MLR performance",
    "What is the admin ratio by line of business?",

    # Utilization Benchmarking
    "What are our claims per 1,000 by service category and LOB?",
    "Show inpatient admits per 1,000 by line of business",
    "Which LOB has the highest ER utilization rate?",
    "What is the average cost per claim by service category?",

    # IBNR & Reserves
    "Show the IBNR payment development triangle for the last 6 service months",
    "What are the completion factors by development month and LOB?",
    "Which service months have the highest reserve exposure?",
    "What is the average payment lag in days by service month?",

    # Denial Financial Impact
    "What is the total denied amount by AI-classified denial category?",
    "Which LOB has the highest denial rate and what drives it?",

    # Enrollment & Exposure
    "How many total member months do we have by LOB?",
    "What is the monthly premium revenue by line of business?",
    "Show enrollment exposure trends by LOB — are we growing or shrinking?",

    # Executive
    "Give me an executive financial summary by LOB",
    "Which line of business is most profitable based on MLR and admin ratio?",
]


# ---------------------------------------------------------------------------
# Create the Genie Space
# ---------------------------------------------------------------------------

def create_genie_space() -> str:
    """Create the Red Bricks Insurance Financial Analytics Genie Space."""
    w = WorkspaceClient()

    space = w.genie.create_space(
        title="Red Bricks Insurance — Financial Analytics",
        description=(
            "Financial and actuarial analytics for Red Bricks Insurance: PMPM trends, "
            "MLR compliance with AI-generated action recommendations, utilization per 1,000 "
            "benchmarking, IBNR reserve estimation via chain-ladder triangles, denial impact "
            "analysis, and enrollment exposure metrics across all lines of business."
        ),
        table_identifiers=TABLES,
    )

    space_id = space.space_id
    print(f"Genie Space created: {space_id}")
    print(f"URL: {w.config.host}/genie/rooms/{space_id}")
    print("\nNext steps:")
    print("  1. Open the Genie Space URL above")
    print("  2. Click the gear icon -> 'General Instructions'")
    print("  3. Paste the instructions from config/genie_financial_instructions.md")
    print("  4. Add sample questions from the 'Sample Questions' section")

    return space_id


if __name__ == "__main__":
    create_genie_space()

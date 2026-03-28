"""
Genie Space Setup — Red Bricks Insurance Command Center

Run this script after the SDP pipelines have materialized the gold tables.
Creates a Genie Space configured with all relevant insurance analytics tables.
"""

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = "red_bricks_insurance"

TABLES = [
    # Cross-domain gold analytics tables
    f"{CATALOG}.analytics.gold_pmpm",
    f"{CATALOG}.analytics.gold_mlr",
    f"{CATALOG}.analytics.gold_denial_analysis",
    f"{CATALOG}.analytics.gold_denial_classification",
    f"{CATALOG}.analytics.gold_member_risk_narrative",
    f"{CATALOG}.analytics.gold_risk_adjustment_analysis",
    f"{CATALOG}.analytics.gold_hedis_member",
    f"{CATALOG}.analytics.gold_hedis_provider",
    f"{CATALOG}.analytics.gold_stars_provider",
    # Domain-level gold tables
    f"{CATALOG}.claims.gold_claims_summary",
    f"{CATALOG}.claims.gold_pharmacy_summary",
    f"{CATALOG}.members.gold_enrollment_summary",
    f"{CATALOG}.members.gold_member_demographics",
    f"{CATALOG}.providers.gold_provider_directory",
    f"{CATALOG}.underwriting.gold_underwriting_summary",
    # Domain silver tables
    f"{CATALOG}.claims.silver_claims_medical",
    f"{CATALOG}.members.silver_enrollment",
    f"{CATALOG}.members.silver_members",
]

# ---------------------------------------------------------------------------
# Sample questions
# ---------------------------------------------------------------------------
SAMPLE_QUESTIONS = [
    # Claims & Financial
    "What is our total claims paid by line of business?",
    "Show me the PMPM trend over time by line of business",
    "What is our medical loss ratio by LOB and how does it compare to the target?",
    "Which claim types have the highest denial rates?",
    "Show me monthly claims volume trend by claim type",

    # Denial Analysis (AI-powered)
    "What are the top denial categories identified by AI classification?",
    "Show denied amount by denial category and line of business",
    "Which LOB has the highest total denied amount?",

    # Quality & HEDIS
    "What is the overall HEDIS compliance rate by measure?",
    "Show providers with 5-star ratings and their specialties",
    "Which providers have the lowest compliance rates?",
    "How many members are non-compliant on diabetes care?",

    # Risk Adjustment
    "What is the average RAF score by line of business?",
    "Show the percentage of high-risk members by LOB",
    "What is the estimated annual revenue from Medicare Advantage risk adjustment?",

    # Enrollment & Demographics
    "How many active members do we have by line of business?",
    "Show member demographics by age band and LOB",
    "What is the churn rate by plan type?",
    "Which counties have the most enrolled members?",

    # Pharmacy
    "What are the top therapeutic classes by total cost?",
    "Show pharmacy fill trends over time",
    "What is the generic fill rate by formulary tier?",

    # AI Risk Narratives
    "Show the top 10 highest risk members and their AI-generated clinical summaries",
    "What are the most common HCC codes among high-risk members?",
]


# ---------------------------------------------------------------------------
# Create the Genie Space
# ---------------------------------------------------------------------------

def create_genie_space() -> str:
    """Create the Red Bricks Insurance Genie Space and return its ID."""
    w = WorkspaceClient()

    space = w.genie.create_space(
        title="Red Bricks Insurance — Analytics Assistant",
        description=(
            "Natural language analytics for Red Bricks Insurance: explore claims trends, "
            "denial patterns (AI-classified), HEDIS quality measures, risk adjustment scores, "
            "enrollment metrics, and AI-generated member risk narratives across all lines of business."
        ),
        table_identifiers=TABLES,
    )

    space_id = space.space_id
    print(f"Genie Space created: {space_id}")
    print(f"URL: {w.config.host}/genie/rooms/{space_id}")
    print("\nNext steps:")
    print("  1. Open the Genie Space URL above")
    print("  2. Click the gear icon → 'General Instructions'")
    print("  3. Paste the instructions from config/genie_instructions.md")
    print("  4. Add sample questions from the 'Sample Questions' section")

    return space_id


if __name__ == "__main__":
    create_genie_space()

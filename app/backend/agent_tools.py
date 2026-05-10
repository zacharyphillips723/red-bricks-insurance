"""Tool definitions for the Care Intelligence Agent.

Each tool calls a governed Unity Catalog function in the `ai_tools` schema.
This means every tool invocation is auditable, has lineage, and is reusable
by any agent, Genie space, or notebook in the workspace.

The UC functions are registered by src/notebooks/create_uc_tools.py.
"""

import json
import traceback

try:
    import mlflow
    _trace = mlflow.trace
except ImportError:
    def _trace(*args, **kwargs):
        if args and callable(args[0]):
            return args[0]
        def decorator(fn):
            return fn
        return decorator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from langchain_core.tools import tool

from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID

_CAT = f"`{UC_CATALOG}`"


@_trace(name="call_uc_function", span_type="RETRIEVER")
def _call_uc_function(func_name: str, params: dict) -> str:
    """Call a Unity Catalog function and return its result as a string."""
    w = WorkspaceClient()

    # Build the function call SQL: SELECT ai_tools.func(:p1, :p2)
    param_placeholders = ", ".join(f":{k}" for k in params)
    sql = f"SELECT {_CAT}.ai_tools.{func_name}({param_placeholders}) AS result"

    stmt_params = [
        StatementParameterListItem(
            name=k,
            value=str(v),
            type="INT" if isinstance(v, int) else "STRING",
        )
        for k, v in params.items()
    ]

    stmt = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=sql,
        parameters=stmt_params,
        wait_timeout="30s",
    )

    if not stmt.result or not stmt.result.data_array:
        return "null"

    result = stmt.result.data_array[0][0]
    return result if result else "null"


@_trace(name="sdk_request", span_type="RETRIEVER")
def _sdk_request(method: str, path: str, body: dict | None = None) -> dict:
    """Make an authenticated request using the SDK's api_client."""
    w = WorkspaceClient()
    if method == "GET":
        return w.api_client.do(method, path)
    return w.api_client.do(method, path, body=body)


# ═══════════════════════════════════════════════════════════════════════════════
# Clinical Tools
# ═══════════════════════════════════════════════════════════════════════════════

@tool
def get_member_profile(member_id: str) -> str:
    """Get the full Member 360 profile including demographics, enrollment,
    risk scores (RAF, HCC), HEDIS care gap summary, claims totals, top
    diagnoses, and PCP information. Use this as the starting point for
    any member inquiry."""
    try:
        return _call_uc_function("get_member_profile", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching profile: {e}"


@tool
def get_lab_results(member_id: str, max_results: int = 15) -> str:
    """Get recent lab results for a member ordered by collection date.
    Returns lab name, value, unit, reference range, and abnormal flag.
    Useful for tracking HbA1c, eGFR, lipid panels, glucose trends."""
    try:
        return _call_uc_function("get_lab_results", {"member_id": member_id, "max_results": max_results})
    except Exception as e:
        return f"Error fetching labs: {e}"


@tool
def search_case_notes(member_id: str, query: str) -> str:
    """Search case notes, call transcripts, and claims summaries for a member
    using Vector Search. Returns the most relevant document chunks."""
    try:
        vs_index = f"{UC_CATALOG}.documents.case_notes_vs_index"
        data = _sdk_request("POST", f"/api/2.0/vector-search/indexes/{vs_index}/query", {
            "columns": ["chunk_id", "document_type", "title", "created_date", "author", "chunk_text"],
            "query_text": query,
            "filters_json": json.dumps({"member_id": member_id}),
            "num_results": 5,
        })
        rows = data.get("result", {}).get("data_array", [])
        col_names = [c["name"] for c in data.get("manifest", {}).get("columns", [])]
        results = [dict(zip(col_names, row)) for row in rows]
        if results:
            return json.dumps(results, default=str)
        return f"No case notes found for member {member_id} matching '{query}'."
    except Exception as e:
        return f"Vector search error (continuing without case notes): {e}"


@tool
def get_case_assessments(member_id: str) -> str:
    """Get clinical and behavioral health assessments for a member.
    Returns PHQ-9 (depression 0-27), GAD-7 (anxiety 0-21), PRAPARE
    (SDOH screening 0-20), Fall Risk (0-10), and Functional Status
    scores with risk levels and dates."""
    try:
        return _call_uc_function("get_case_assessments", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching assessments: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# Financial Tools
# ═══════════════════════════════════════════════════════════════════════════════

@tool
def get_claims_summary(member_id: str) -> str:
    """Get claims and cost summary: medical claim count, total paid/billed
    YTD, pharmacy claim count and spend, combined total paid, and top
    diagnoses. Use for financial analysis and cost trending."""
    try:
        return _call_uc_function("get_claims_summary", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching claims: {e}"


@tool
def get_denial_history(member_id: str) -> str:
    """Get denied medical claims with claim ID, service date, procedure
    code/description, diagnosis code, billed amount, and denial reason.
    Use to identify denial patterns and prior auth gaps."""
    try:
        return _call_uc_function("get_denial_history", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching denials: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# Care Management Tools
# ═══════════════════════════════════════════════════════════════════════════════

@tool
def get_care_programs(member_id: str) -> str:
    """Get disease management program enrollments: Diabetes Management,
    CHF Care, COPD Wellness, Behavioral Health, Maternal Health, CKD.
    Returns program name, status, enrollment/disenrollment dates, and
    referral source."""
    try:
        return _call_uc_function("get_care_programs", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching programs: {e}"


@tool
def get_sdoh_screening(member_id: str) -> str:
    """Get the most recent SDOH screening results: food insecurity,
    housing instability, transportation barriers, social isolation,
    financial strain flags, composite risk score (0-100), and total
    flag count. Members with 3+ flags are high SDOH risk."""
    try:
        return _call_uc_function("get_sdoh_screening", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching SDOH: {e}"


@tool
def get_care_gaps(member_id: str) -> str:
    """Get HEDIS care gaps with intervention tracking. Returns measure
    name, condition, priority, gap age in days, intervention count,
    last intervention date, and closure date. Open gaps with high
    priority and long age should be escalated."""
    try:
        return _call_uc_function("get_care_gaps", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching care gaps: {e}"


@tool
def get_toc_history(member_id: str) -> str:
    """Get transitions of care history: discharge date/type/facility,
    readmission risk score and tier, follow-up type (48-Hour Call,
    7-Day PCP Visit, Medication Reconciliation), follow-up status,
    and days from due date. Track post-discharge compliance."""
    try:
        return _call_uc_function("get_toc_history", {"member_id": member_id})
    except Exception as e:
        return f"Error fetching TOC: {e}"


@tool
def recommend_intervention(member_id: str) -> str:
    """Aggregate key data for next-best-action recommendations: member
    risk profile (tier, RAF, diagnoses), SDOH screening results (all
    flags and composite score), and all open care gaps with priority
    and aging. Use this for care management decision-making."""
    try:
        return _call_uc_function("recommend_intervention", {"member_id": member_id})
    except Exception as e:
        return f"Error gathering intervention data: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# Tool registries by specialist domain
# ═══════════════════════════════════════════════════════════════════════════════

CLINICAL_TOOLS = [get_member_profile, get_lab_results, search_case_notes, get_case_assessments]
FINANCIAL_TOOLS = [get_claims_summary, get_denial_history]
CARE_MGMT_TOOLS = [get_care_programs, get_sdoh_screening, get_care_gaps, get_toc_history, recommend_intervention]
ALL_TOOLS = CLINICAL_TOOLS + FINANCIAL_TOOLS + CARE_MGMT_TOOLS

# Tool lookup map for agent dispatching
_tool_map = {t.name: t for t in ALL_TOOLS}

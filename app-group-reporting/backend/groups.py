"""SQL queries for group data via Statement Execution API."""

import os
import traceback

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

UC_CATALOG = os.environ.get("UC_CATALOG", "catalog_insurance_vpx9o6")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "red_bricks_insurance_dev")
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")

REPORT_CARD_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_group_report_card"
EXPERIENCE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_group_experience"
STOP_LOSS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_group_stop_loss"
RENEWAL_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_group_renewal"
TCOC_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_member_tcoc"
ENROLLMENT_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.silver_enrollment"
MEMBER_360_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_member_360"
CLAIMS_MEDICAL_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.silver_claims_medical"
CLAIMS_PHARMACY_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.silver_claims_pharmacy"


def _execute_sql(sql: str, params: list | None = None) -> list[dict]:
    """Execute SQL via SDK Statement Execution API."""
    w = WorkspaceClient()
    kwargs = {
        "warehouse_id": SQL_WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "30s",
    }
    if params:
        kwargs["parameters"] = [
            StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
            for p in params
        ]

    stmt = w.statement_execution.execute_statement(**kwargs)

    if not stmt.result or not stmt.result.data_array:
        return []

    col_names = []
    if stmt.manifest and stmt.manifest.schema and stmt.manifest.schema.columns:
        col_names = [c.name for c in stmt.manifest.schema.columns]

    if not col_names:
        return []

    return [dict(zip(col_names, row)) for row in stmt.result.data_array]


def list_groups(
    query: str = "",
    industry: str = "",
    funding_type: str = "",
    renewal_action: str = "",
) -> list[dict]:
    """List/filter groups from the report card."""
    try:
        sql = f"""
            SELECT group_id, group_name, industry, group_size_tier, funding_type,
                   state, total_members, active_members, claims_pmpm, loss_ratio,
                   renewal_action, group_health_score
            FROM {REPORT_CARD_TABLE}
            WHERE 1=1
        """
        params = []

        if query.strip():
            sql += " AND (LOWER(group_name) LIKE CONCAT('%', LOWER(:query), '%') OR group_id = :query)"
            params.append({"name": "query", "value": query})
        if industry.strip():
            sql += " AND industry = :industry"
            params.append({"name": "industry", "value": industry})
        if funding_type.strip():
            sql += " AND funding_type = :funding_type"
            params.append({"name": "funding_type", "value": funding_type})
        if renewal_action.strip():
            sql += " AND renewal_action = :renewal_action"
            params.append({"name": "renewal_action", "value": renewal_action})

        sql += " ORDER BY group_name LIMIT 100"
        return _execute_sql(sql, params if params else None)
    except Exception as e:
        print(f"[Groups] List error: {e}")
        traceback.print_exc()
        return []


def get_report_card(group_id: str) -> dict | None:
    """Get full report card for a group."""
    try:
        rows = _execute_sql(
            f"SELECT * FROM {REPORT_CARD_TABLE} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[Groups] Report card error: {e}")
        traceback.print_exc()
        return None


def get_experience(group_id: str) -> dict | None:
    """Get claims experience detail for a group."""
    try:
        rows = _execute_sql(
            f"SELECT * FROM {EXPERIENCE_TABLE} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[Groups] Experience error: {e}")
        traceback.print_exc()
        return None


def get_stop_loss(group_id: str) -> dict | None:
    """Get stop-loss detail (latest year) for a group."""
    try:
        rows = _execute_sql(
            f"SELECT * FROM {STOP_LOSS_TABLE} WHERE group_id = :gid ORDER BY claim_year DESC LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[Groups] Stop-loss error: {e}")
        traceback.print_exc()
        return None


def get_renewal(group_id: str) -> dict | None:
    """Get renewal detail for a group."""
    try:
        rows = _execute_sql(
            f"SELECT * FROM {RENEWAL_TABLE} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[Groups] Renewal error: {e}")
        traceback.print_exc()
        return None


def get_tcoc(group_id: str) -> list[dict]:
    """Get member cost tier distribution for a group."""
    try:
        return _execute_sql(
            f"""
            SELECT
              t.cost_tier,
              COUNT(DISTINCT t.member_id) AS member_count,
              ROUND(AVG(t.tcoc), 2) AS avg_tcoc,
              ROUND(AVG(t.tci), 3) AS avg_tci,
              ROUND(SUM(t.total_paid), 2) AS total_paid
            FROM {TCOC_TABLE} t
            INNER JOIN {ENROLLMENT_TABLE} e
              ON t.member_id = e.member_id
            WHERE e.group_number = :gid
            GROUP BY t.cost_tier
            ORDER BY avg_tcoc DESC
            """,
            [{"name": "gid", "value": group_id}],
        )
    except Exception as e:
        print(f"[Groups] TCOC error: {e}")
        traceback.print_exc()
        return []


# ===================================================================
# Standard Reports
# ===================================================================

def report_high_cost_members(group_id: str, limit: int = 10) -> list[dict]:
    """Top N costliest members in a group with clinical summary."""
    try:
        return _execute_sql(
            f"""
            SELECT
              m.member_id,
              m.first_name,
              m.last_name,
              m.age,
              m.gender,
              t.cost_tier,
              t.tci,
              t.raf_score,
              ROUND(t.total_paid, 2) AS total_paid,
              ROUND(t.medical_paid, 2) AS medical_paid,
              ROUND(t.pharmacy_paid, 2) AS pharmacy_paid,
              m.top_diagnoses,
              m.risk_tier,
              m.hedis_gap_count,
              m.hedis_gap_measures,
              t.hcc_count,
              t.member_months
            FROM {MEMBER_360_TABLE} m
            INNER JOIN {ENROLLMENT_TABLE} e ON m.member_id = e.member_id
            INNER JOIN {TCOC_TABLE} t ON m.member_id = t.member_id
            WHERE e.group_number = :gid
            ORDER BY t.total_paid DESC
            LIMIT :lim
            """,
            [
                {"name": "gid", "value": group_id},
                {"name": "lim", "value": str(limit), "type": "INT"},
            ],
        )
    except Exception as e:
        print(f"[Reports] High-cost members error: {e}")
        traceback.print_exc()
        return []


def report_claims_trend(group_id: str) -> list[dict]:
    """Monthly claims PMPM trend for a group (last 18 months)."""
    try:
        return _execute_sql(
            f"""
            WITH monthly_claims AS (
              SELECT
                c.service_year_month AS month,
                SUM(c.paid_amount) AS medical_paid,
                COUNT(DISTINCT c.claim_id) AS medical_claims
              FROM {CLAIMS_MEDICAL_TABLE} c
              INNER JOIN {ENROLLMENT_TABLE} e ON c.member_id = e.member_id
              WHERE e.group_number = :gid
              GROUP BY c.service_year_month
            ),
            monthly_rx AS (
              SELECT
                p.fill_year_month AS month,
                SUM(p.plan_paid) AS pharmacy_paid,
                COUNT(DISTINCT p.claim_id) AS pharmacy_claims
              FROM {CLAIMS_PHARMACY_TABLE} p
              INNER JOIN {ENROLLMENT_TABLE} e ON p.member_id = e.member_id
              WHERE e.group_number = :gid
              GROUP BY p.fill_year_month
            ),
            member_count AS (
              SELECT COUNT(DISTINCT member_id) AS members
              FROM {ENROLLMENT_TABLE}
              WHERE group_number = :gid
            )
            SELECT
              COALESCE(mc.month, rx.month) AS month,
              ROUND(COALESCE(mc.medical_paid, 0), 2) AS medical_paid,
              ROUND(COALESCE(rx.pharmacy_paid, 0), 2) AS pharmacy_paid,
              ROUND(COALESCE(mc.medical_paid, 0) + COALESCE(rx.pharmacy_paid, 0), 2) AS total_paid,
              COALESCE(mc.medical_claims, 0) AS medical_claims,
              COALESCE(rx.pharmacy_claims, 0) AS pharmacy_claims,
              mem.members,
              ROUND((COALESCE(mc.medical_paid, 0) + COALESCE(rx.pharmacy_paid, 0))
                / NULLIF(mem.members, 0), 2) AS total_pmpm,
              ROUND(COALESCE(mc.medical_paid, 0) / NULLIF(mem.members, 0), 2) AS medical_pmpm,
              ROUND(COALESCE(rx.pharmacy_paid, 0) / NULLIF(mem.members, 0), 2) AS pharmacy_pmpm
            FROM monthly_claims mc
            FULL OUTER JOIN monthly_rx rx ON mc.month = rx.month
            CROSS JOIN member_count mem
            WHERE COALESCE(mc.month, rx.month) IS NOT NULL
            ORDER BY month
            """,
            [{"name": "gid", "value": group_id}],
        )
    except Exception as e:
        print(f"[Reports] Claims trend error: {e}")
        traceback.print_exc()
        return []


def report_top_drugs(group_id: str, limit: int = 10) -> list[dict]:
    """Top N drugs by plan paid for a group."""
    try:
        return _execute_sql(
            f"""
            SELECT
              p.drug_name,
              p.therapeutic_class,
              MAX(CAST(p.is_specialty AS INT)) AS is_specialty,
              COUNT(DISTINCT p.claim_id) AS fill_count,
              COUNT(DISTINCT p.member_id) AS member_count,
              ROUND(SUM(p.plan_paid), 2) AS total_plan_paid,
              ROUND(SUM(p.total_cost), 2) AS total_cost,
              ROUND(SUM(p.member_copay), 2) AS total_member_copay,
              ROUND(AVG(p.plan_paid), 2) AS avg_cost_per_fill
            FROM {CLAIMS_PHARMACY_TABLE} p
            INNER JOIN {ENROLLMENT_TABLE} e ON p.member_id = e.member_id
            WHERE e.group_number = :gid
            GROUP BY p.drug_name, p.therapeutic_class
            ORDER BY total_plan_paid DESC
            LIMIT :lim
            """,
            [
                {"name": "gid", "value": group_id},
                {"name": "lim", "value": str(limit), "type": "INT"},
            ],
        )
    except Exception as e:
        print(f"[Reports] Top drugs error: {e}")
        traceback.print_exc()
        return []


def report_utilization_summary(group_id: str) -> list[dict]:
    """Utilization breakdown by claim type with top diagnoses."""
    try:
        return _execute_sql(
            f"""
            WITH claim_agg AS (
              SELECT
                c.claim_type,
                COUNT(DISTINCT c.claim_id) AS claim_count,
                COUNT(DISTINCT c.member_id) AS unique_members,
                ROUND(SUM(c.paid_amount), 2) AS total_paid,
                ROUND(AVG(c.paid_amount), 2) AS avg_paid_per_claim,
                ROUND(SUM(c.billed_amount), 2) AS total_billed
              FROM {CLAIMS_MEDICAL_TABLE} c
              INNER JOIN {ENROLLMENT_TABLE} e ON c.member_id = e.member_id
              WHERE e.group_number = :gid
              GROUP BY c.claim_type
            ),
            member_months AS (
              SELECT SUM(coverage_months) AS mm
              FROM {ENROLLMENT_TABLE}
              WHERE group_number = :gid
            ),
            top_dx AS (
              SELECT
                c.claim_type,
                c.primary_diagnosis_code,
                c.primary_diagnosis_desc,
                COUNT(*) AS dx_count,
                ROW_NUMBER() OVER (PARTITION BY c.claim_type ORDER BY COUNT(*) DESC) AS rn
              FROM {CLAIMS_MEDICAL_TABLE} c
              INNER JOIN {ENROLLMENT_TABLE} e ON c.member_id = e.member_id
              WHERE e.group_number = :gid
              GROUP BY c.claim_type, c.primary_diagnosis_code, c.primary_diagnosis_desc
            )
            SELECT
              ca.claim_type,
              ca.claim_count,
              ca.unique_members,
              ca.total_paid,
              ca.avg_paid_per_claim,
              ca.total_billed,
              ROUND(ca.claim_count * 1000.0 / NULLIF(mm.mm, 0), 1) AS per_1000,
              CONCAT_WS(' | ',
                MAX(CASE WHEN td.rn = 1 THEN CONCAT(td.primary_diagnosis_desc, ' (', td.dx_count, ')') END),
                MAX(CASE WHEN td.rn = 2 THEN CONCAT(td.primary_diagnosis_desc, ' (', td.dx_count, ')') END),
                MAX(CASE WHEN td.rn = 3 THEN CONCAT(td.primary_diagnosis_desc, ' (', td.dx_count, ')') END)
              ) AS top_diagnoses
            FROM claim_agg ca
            CROSS JOIN member_months mm
            LEFT JOIN top_dx td ON ca.claim_type = td.claim_type AND td.rn <= 3
            GROUP BY ca.claim_type, ca.claim_count, ca.unique_members, ca.total_paid,
                     ca.avg_paid_per_claim, ca.total_billed, mm.mm
            ORDER BY ca.total_paid DESC
            """,
            [{"name": "gid", "value": group_id}],
        )
    except Exception as e:
        print(f"[Reports] Utilization summary error: {e}")
        traceback.print_exc()
        return []


def report_risk_care_gaps(group_id: str) -> dict:
    """Risk distribution and care gap summary for a group."""
    try:
        # Cost tier distribution
        cost_tiers = _execute_sql(
            f"""
            SELECT
              t.cost_tier,
              COUNT(DISTINCT t.member_id) AS member_count,
              ROUND(AVG(t.tci), 3) AS avg_tci,
              ROUND(SUM(t.total_paid), 2) AS total_paid
            FROM {TCOC_TABLE} t
            INNER JOIN {ENROLLMENT_TABLE} e ON t.member_id = e.member_id
            WHERE e.group_number = :gid
            GROUP BY t.cost_tier
            ORDER BY avg_tci DESC
            """,
            [{"name": "gid", "value": group_id}],
        )

        # Risk tier distribution
        risk_tiers = _execute_sql(
            f"""
            SELECT
              m.risk_tier,
              COUNT(DISTINCT m.member_id) AS member_count,
              ROUND(AVG(CAST(m.raf_score AS DOUBLE)), 3) AS avg_raf
            FROM {MEMBER_360_TABLE} m
            INNER JOIN {ENROLLMENT_TABLE} e ON m.member_id = e.member_id
            WHERE e.group_number = :gid
            GROUP BY m.risk_tier
            ORDER BY avg_raf DESC
            """,
            [{"name": "gid", "value": group_id}],
        )

        # Care gap summary
        care_gaps = _execute_sql(
            f"""
            SELECT
              COUNT(DISTINCT m.member_id) AS total_members,
              COUNT(DISTINCT CASE WHEN CAST(m.hedis_gap_count AS INT) > 0 THEN m.member_id END)
                AS members_with_gaps,
              SUM(CAST(m.hedis_gap_count AS INT)) AS total_gaps,
              ROUND(AVG(CAST(m.raf_score AS DOUBLE)), 3) AS avg_raf_score
            FROM {MEMBER_360_TABLE} m
            INNER JOIN {ENROLLMENT_TABLE} e ON m.member_id = e.member_id
            WHERE e.group_number = :gid
            """,
            [{"name": "gid", "value": group_id}],
        )

        # Rising risk members (TCI 1.5-2.0) — intervention sweet spot
        rising_risk = _execute_sql(
            f"""
            SELECT
              m.member_id,
              m.first_name,
              m.last_name,
              m.age,
              t.tci,
              t.cost_tier,
              ROUND(t.total_paid, 2) AS total_paid,
              m.top_diagnoses,
              m.hedis_gap_count
            FROM {MEMBER_360_TABLE} m
            INNER JOIN {ENROLLMENT_TABLE} e ON m.member_id = e.member_id
            INNER JOIN {TCOC_TABLE} t ON m.member_id = t.member_id
            WHERE e.group_number = :gid
              AND t.tci >= 1.5 AND t.tci < 2.0
            ORDER BY t.tci DESC
            LIMIT 10
            """,
            [{"name": "gid", "value": group_id}],
        )

        return {
            "cost_tiers": cost_tiers,
            "risk_tiers": risk_tiers,
            "summary": care_gaps[0] if care_gaps else {},
            "rising_risk_members": rising_risk,
        }
    except Exception as e:
        print(f"[Reports] Risk/care gaps error: {e}")
        traceback.print_exc()
        return {"cost_tiers": [], "risk_tiers": [], "summary": {}, "rising_risk_members": []}

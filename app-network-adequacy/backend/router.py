"""FastAPI route handlers for the Network Adequacy Portal.

All analytics data is read from Unity Catalog via Statement Execution API.
"""

import asyncio
import traceback
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, Query

from .env_config import SQL_WAREHOUSE_ID, UC_CATALOG
from .genie import ask_genie
from .models import (
    ComplianceRow,
    ComplianceSummary,
    CountyMapMetric,
    DashboardStats,
    GenieQuestionIn,
    GenieResponseOut,
    GhostProviderRow,
    GhostSummary,
    LeakageByCounty,
    LeakageByReason,
    LeakageBySpecialty,
    LeakageSummary,
    NetworkGap,
    RecruitmentTarget,
)

api = APIRouter(prefix="/api")

# SQL-safe catalog quoting (handles hyphens)
_cat = f"`{UC_CATALOG}`" if UC_CATALOG else "`red_bricks_insurance`"


def _execute_sql(query: str) -> list[dict]:
    """Execute a SQL query via Statement Execution API and return rows as dicts."""
    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )
    if not resp.result or not resp.result.data_array:
        return []
    col_names = [
        c.name for c in (resp.manifest.schema.columns or [])
    ] if resp.manifest and resp.manifest.schema else []
    return [dict(zip(col_names, row)) for row in resp.result.data_array]


def _safe_int(v, default=0) -> int:
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _safe_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return bool(v) if v is not None else False


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@api.get("/health")
async def health():
    return {"status": "ok", "catalog": UC_CATALOG, "warehouse": SQL_WAREHOUSE_ID}


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@api.get("/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    try:
        compliance_rows, ghost_rows, leakage_rows, telehealth_rows, recruitment_rows = (
            await asyncio.gather(
                asyncio.to_thread(_execute_sql, f"""
                    SELECT
                        ROUND(SUM(CASE WHEN is_compliant THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS overall_compliance_pct,
                        COUNT(*) AS total_combos,
                        SUM(CASE WHEN NOT is_compliant THEN 1 ELSE 0 END) AS non_compliant_count,
                        SUM(CASE WHEN NOT is_compliant THEN gap_members ELSE 0 END) AS total_gap_members
                    FROM {_cat}.network.gold_network_adequacy_compliance
                """),
                asyncio.to_thread(_execute_sql, f"""
                    SELECT
                        COUNT(*) AS total_flagged,
                        SUM(CASE WHEN ghost_severity = 'High' THEN 1 ELSE 0 END) AS high_sev,
                        SUM(CASE WHEN ghost_severity = 'Medium' THEN 1 ELSE 0 END) AS med_sev,
                        SUM(CASE WHEN ghost_severity = 'Low' THEN 1 ELSE 0 END) AS low_sev,
                        SUM(impact_members) AS total_impact
                    FROM {_cat}.network.gold_ghost_network_flags
                    WHERE is_ghost_flagged = TRUE
                """),
                asyncio.to_thread(_execute_sql, f"""
                    SELECT
                        SUM(total_oon_claims) AS total_claims,
                        ROUND(SUM(total_leakage_cost), 0) AS total_cost,
                        SUM(total_oon_members) AS total_members
                    FROM {_cat}.network.gold_leakage_summary
                """),
                asyncio.to_thread(_execute_sql, f"""
                    SELECT SUM(CASE WHEN telehealth_credit_applied THEN 1 ELSE 0 END) AS cnt
                    FROM {_cat}.network.gold_network_adequacy_compliance
                """),
                asyncio.to_thread(_execute_sql, f"""
                    SELECT rendering_provider_npi, specialty, county_name,
                           total_claims, ROUND(total_paid, 0) AS total_paid,
                           ROUND(potential_savings, 0) AS potential_savings,
                           members_served,
                           ROUND(avg_member_distance_mi, 1) AS avg_member_distance_mi,
                           ROUND(recruitment_priority_score, 0) AS recruitment_priority_score
                    FROM {_cat}.network.gold_provider_recruitment_targets
                    ORDER BY recruitment_priority_score DESC LIMIT 5
                """),
            )
        )

        c = compliance_rows[0] if compliance_rows else {}
        g = ghost_rows[0] if ghost_rows else {}
        l = leakage_rows[0] if leakage_rows else {}
        t = telehealth_rows[0] if telehealth_rows else {}

        return DashboardStats(
            compliance_summary=ComplianceSummary(
                overall_compliance_pct=_safe_float(c.get("overall_compliance_pct")),
                total_county_specialty_combos=_safe_int(c.get("total_combos")),
                non_compliant_count=_safe_int(c.get("non_compliant_count")),
                total_gap_members=_safe_int(c.get("total_gap_members")),
            ),
            ghost_summary=GhostSummary(
                total_flagged=_safe_int(g.get("total_flagged")),
                high_severity=_safe_int(g.get("high_sev")),
                medium_severity=_safe_int(g.get("med_sev")),
                low_severity=_safe_int(g.get("low_sev")),
                total_impact_members=_safe_int(g.get("total_impact")),
            ),
            total_leakage_cost=_safe_float(l.get("total_cost")),
            total_oon_claims=_safe_int(l.get("total_claims")),
            telehealth_credits_applied=_safe_int(t.get("cnt")),
            top_recruitment_targets=[
                RecruitmentTarget(
                    rendering_provider_npi=r.get("rendering_provider_npi") or "",
                    specialty=r.get("specialty"),
                    county_name=r.get("county_name"),
                    total_claims=_safe_int(r.get("total_claims")),
                    total_paid=_safe_float(r.get("total_paid")),
                    potential_savings=_safe_float(r.get("potential_savings")),
                    members_served=_safe_int(r.get("members_served")),
                    avg_member_distance_mi=_safe_float(r.get("avg_member_distance_mi")),
                    recruitment_priority_score=_safe_float(r.get("recruitment_priority_score")),
                )
                for r in recruitment_rows
            ],
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Compliance
# ---------------------------------------------------------------------------

@api.get("/compliance", response_model=list[ComplianceRow])
async def get_compliance(
    county: Optional[str] = Query(None),
    specialty: Optional[str] = Query(None),
    compliant_only: Optional[bool] = Query(None),
):
    filters = []
    if county:
        filters.append(f"county_name = '{county}'")
    if specialty:
        filters.append(f"cms_specialty_type = '{specialty}'")
    if compliant_only is True:
        filters.append("is_compliant = TRUE")
    elif compliant_only is False:
        filters.append("is_compliant = FALSE")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT * FROM {_cat}.network.gold_network_adequacy_compliance
        {where}
        ORDER BY pct_compliant ASC
    """)
    return [
        ComplianceRow(
            county_fips=r.get("county_fips") or "",
            county_name=r.get("county_name") or "",
            county_type=r.get("county_type") or "",
            cms_specialty_type=r.get("cms_specialty_type") or "",
            max_distance_miles=_safe_int(r.get("max_distance_miles")),
            max_time_minutes=_safe_int(r.get("max_time_minutes")),
            total_members=_safe_int(r.get("total_members")),
            compliant_members=_safe_int(r.get("compliant_members")),
            pct_compliant=_safe_float(r.get("pct_compliant")),
            is_compliant=_safe_bool(r.get("is_compliant")),
            gap_members=_safe_int(r.get("gap_members")),
            avg_nearest_distance_mi=_safe_float(r.get("avg_nearest_distance_mi")),
            telehealth_available=_safe_bool(r.get("telehealth_available")),
            telehealth_credit_applied=_safe_bool(r.get("telehealth_credit_applied")),
        )
        for r in rows
    ]


@api.get("/compliance/counties")
async def get_compliance_counties():
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT DISTINCT county_name FROM {_cat}.network.gold_network_adequacy_compliance
        ORDER BY county_name
    """)
    return [r["county_name"] for r in rows]


@api.get("/compliance/specialties")
async def get_compliance_specialties():
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT DISTINCT cms_specialty_type FROM {_cat}.network.gold_network_adequacy_compliance
        ORDER BY cms_specialty_type
    """)
    return [r["cms_specialty_type"] for r in rows]


# ---------------------------------------------------------------------------
# Ghost Network
# ---------------------------------------------------------------------------

@api.get("/ghost-network", response_model=list[GhostProviderRow])
async def get_ghost_providers(
    severity: Optional[str] = Query(None),
    specialty: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    flagged_only: bool = Query(True),
):
    filters = []
    if flagged_only:
        filters.append("is_ghost_flagged = TRUE")
    if severity:
        filters.append(f"ghost_severity = '{severity}'")
    if specialty:
        filters.append(f"specialty = '{specialty}'")
    if county:
        filters.append(f"county = '{county}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT * FROM {_cat}.network.gold_ghost_network_flags
        {where}
        ORDER BY
            CASE ghost_severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
            impact_members DESC
        LIMIT 200
    """)
    return [
        GhostProviderRow(
            npi=r.get("npi") or "",
            provider_name=r.get("provider_name") or "",
            specialty=r.get("specialty") or "",
            cms_specialty_type=r.get("cms_specialty_type"),
            county=r.get("county") or "",
            county_fips=r.get("county_fips"),
            ghost_severity=r.get("ghost_severity", "None"),
            ghost_signal_count=_safe_int(r.get("ghost_signal_count")),
            is_ghost_flagged=_safe_bool(r.get("is_ghost_flagged")),
            impact_members=_safe_int(r.get("impact_members")),
            accepts_new_patients=_safe_bool(r.get("accepts_new_patients")),
            telehealth_capable=_safe_bool(r.get("telehealth_capable")),
            panel_size=_safe_int(r.get("panel_size")),
            panel_capacity=_safe_int(r.get("panel_capacity")),
            appointment_wait_days=_safe_int(r.get("appointment_wait_days")),
            credentialing_status=r.get("credentialing_status"),
            last_claims_date=r.get("last_claims_date"),
            no_claims_12m=_safe_bool(r.get("no_claims_12m")),
            no_claims_6m=_safe_bool(r.get("no_claims_6m")),
            not_accepting=_safe_bool(r.get("not_accepting")),
            extreme_wait=_safe_bool(r.get("extreme_wait")),
            credential_expired=_safe_bool(r.get("credential_expired")),
            panel_full=_safe_bool(r.get("panel_full")),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Leakage
# ---------------------------------------------------------------------------

@api.get("/leakage", response_model=LeakageSummary)
async def get_leakage_summary():
    by_spec, by_county, by_reason = await asyncio.gather(
        asyncio.to_thread(_execute_sql, f"""
            SELECT cms_specialty_type,
                   SUM(total_oon_claims) AS total_claims,
                   ROUND(SUM(total_oon_paid), 0) AS total_paid,
                   ROUND(SUM(total_leakage_cost), 0) AS leakage_cost,
                   SUM(total_oon_members) AS unique_members,
                   SUM(oon_provider_count) AS oon_providers
            FROM {_cat}.network.gold_leakage_summary
            GROUP BY cms_specialty_type ORDER BY leakage_cost DESC
        """),
        asyncio.to_thread(_execute_sql, f"""
            SELECT county_name, county_type,
                   SUM(total_oon_claims) AS total_claims,
                   ROUND(SUM(total_leakage_cost), 0) AS leakage_cost,
                   SUM(total_oon_members) AS unique_members
            FROM {_cat}.network.gold_leakage_summary
            GROUP BY county_name, county_type ORDER BY leakage_cost DESC
        """),
        asyncio.to_thread(_execute_sql, f"""
            SELECT leakage_reason,
                   SUM(claim_count) AS total_claims,
                   ROUND(SUM(total_leakage_cost), 0) AS leakage_cost,
                   SUM(unique_members) AS unique_members
            FROM {_cat}.network.gold_network_leakage
            GROUP BY leakage_reason ORDER BY leakage_cost DESC
        """),
    )

    total_claims = sum(_safe_int(r.get("total_claims")) for r in by_spec)
    total_cost = sum(_safe_float(r.get("leakage_cost")) for r in by_spec)
    total_members = sum(_safe_int(r.get("unique_members")) for r in by_spec)

    return LeakageSummary(
        total_oon_claims=total_claims,
        total_leakage_cost=total_cost,
        total_oon_members=total_members,
        by_specialty=[
            LeakageBySpecialty(
                cms_specialty_type=r.get("cms_specialty_type") or "Unknown",
                total_claims=_safe_int(r.get("total_claims")),
                total_paid=_safe_float(r.get("total_paid")),
                leakage_cost=_safe_float(r.get("leakage_cost")),
                unique_members=_safe_int(r.get("unique_members")),
                oon_providers=_safe_int(r.get("oon_providers")),
            ) for r in by_spec
        ],
        by_county=[
            LeakageByCounty(
                county_name=r.get("county_name") or "Unknown",
                county_type=r.get("county_type") or "Unknown",
                total_claims=_safe_int(r.get("total_claims")),
                leakage_cost=_safe_float(r.get("leakage_cost")),
                unique_members=_safe_int(r.get("unique_members")),
            ) for r in by_county
        ],
        by_reason=[
            LeakageByReason(
                leakage_reason=r.get("leakage_reason") or "Unknown",
                total_claims=_safe_int(r.get("total_claims")),
                leakage_cost=_safe_float(r.get("leakage_cost")),
                unique_members=_safe_int(r.get("unique_members")),
            ) for r in by_reason
        ],
    )


# ---------------------------------------------------------------------------
# Recruitment Targets
# ---------------------------------------------------------------------------

@api.get("/recruitment", response_model=list[RecruitmentTarget])
async def get_recruitment_targets(limit: int = Query(20)):
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT rendering_provider_npi, specialty, county_name,
               total_claims, ROUND(total_paid, 0) AS total_paid,
               ROUND(potential_savings, 0) AS potential_savings,
               members_served,
               ROUND(avg_member_distance_mi, 1) AS avg_member_distance_mi,
               ROUND(recruitment_priority_score, 0) AS recruitment_priority_score
        FROM {_cat}.network.gold_provider_recruitment_targets
        ORDER BY recruitment_priority_score DESC
        LIMIT {min(limit, 100)}
    """)
    return [
        RecruitmentTarget(
            rendering_provider_npi=r.get("rendering_provider_npi", ""),
            specialty=r.get("specialty"),
            county_name=r.get("county_name"),
            total_claims=_safe_int(r.get("total_claims")),
            total_paid=_safe_float(r.get("total_paid")),
            potential_savings=_safe_float(r.get("potential_savings")),
            members_served=_safe_int(r.get("members_served")),
            avg_member_distance_mi=_safe_float(r.get("avg_member_distance_mi")),
            recruitment_priority_score=_safe_float(r.get("recruitment_priority_score")),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Network Gaps
# ---------------------------------------------------------------------------

@api.get("/gaps", response_model=list[NetworkGap])
async def get_network_gaps(max_priority: int = Query(3)):
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT county_name, county_type, cms_specialty_type,
               ROUND(pct_compliant, 1) AS pct_compliant,
               gap_members, gap_status, priority_rank,
               cms_threshold_miles,
               ROUND(avg_nearest_distance_mi, 1) AS avg_nearest_distance_mi,
               telehealth_credit_applied
        FROM {_cat}.network.gold_network_gaps
        WHERE priority_rank <= {min(max_priority, 4)}
        ORDER BY priority_rank, gap_members DESC
    """)
    return [
        NetworkGap(
            county_name=r.get("county_name") or "",
            county_type=r.get("county_type") or "",
            cms_specialty_type=r.get("cms_specialty_type") or "",
            pct_compliant=_safe_float(r.get("pct_compliant")),
            gap_members=_safe_int(r.get("gap_members")),
            gap_status=r.get("gap_status") or "",
            priority_rank=_safe_int(r.get("priority_rank")),
            cms_threshold_miles=_safe_int(r.get("cms_threshold_miles")),
            avg_nearest_distance_mi=_safe_float(r.get("avg_nearest_distance_mi")),
            telehealth_credit_applied=_safe_bool(r.get("telehealth_credit_applied")),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Map / Geographic View
# ---------------------------------------------------------------------------

@api.get("/map/county-metrics", response_model=list[CountyMapMetric])
async def get_county_map_metrics():
    compliance_rows, ghost_rows, leakage_rows, provider_rows = await asyncio.gather(
        asyncio.to_thread(_execute_sql, f"""
            SELECT county_fips, county_name, county_type,
                   AVG(pct_compliant) AS avg_compliance_pct,
                   SUM(CASE WHEN NOT is_compliant THEN 1 ELSE 0 END) AS non_compliant_specialties,
                   COUNT(*) AS total_specialties,
                   SUM(gap_members) AS gap_members
            FROM {_cat}.network.gold_network_adequacy_compliance
            GROUP BY county_fips, county_name, county_type
        """),
        asyncio.to_thread(_execute_sql, f"""
            SELECT county_fips,
                   SUM(CASE WHEN is_ghost_flagged THEN 1 ELSE 0 END) AS ghost_flagged_count,
                   SUM(CASE WHEN ghost_severity = 'High' THEN 1 ELSE 0 END) AS ghost_high_count,
                   SUM(impact_members) AS ghost_impact_members
            FROM {_cat}.network.gold_ghost_network_flags
            GROUP BY county_fips
        """),
        asyncio.to_thread(_execute_sql, f"""
            SELECT county_fips, county_name, county_type,
                   SUM(total_oon_claims) AS oon_claims,
                   ROUND(SUM(total_leakage_cost), 0) AS leakage_cost,
                   SUM(total_oon_members) AS oon_members
            FROM {_cat}.network.gold_leakage_summary
            GROUP BY county_fips, county_name, county_type
        """),
        asyncio.to_thread(_execute_sql, f"""
            SELECT pg.county_fips,
                   COUNT(*) AS total_providers,
                   SUM(CASE WHEN pg.network_status = 'In-Network' THEN 1 ELSE 0 END) AS inn_providers,
                   SUM(CASE WHEN pg.network_status = 'Out-of-Network' THEN 1 ELSE 0 END) AS oon_providers,
                   ROUND(AVG(pg.provider_latitude), 4) AS centroid_lat,
                   ROUND(AVG(pg.provider_longitude), 4) AS centroid_lon
            FROM {_cat}.network.silver_provider_geo pg
            JOIN {_cat}.network.silver_county_classification cc
              ON pg.county_fips = cc.county_fips
            GROUP BY pg.county_fips
        """),
    )

    merged: dict[str, dict] = {}

    for r in compliance_rows:
        fips = r.get("county_fips") or ""
        if not fips:
            continue
        merged[fips] = {
            "county_fips": fips,
            "county_name": r.get("county_name") or "",
            "county_type": r.get("county_type") or "",
            "avg_compliance_pct": _safe_float(r.get("avg_compliance_pct")),
            "non_compliant_specialties": _safe_int(r.get("non_compliant_specialties")),
            "total_specialties": _safe_int(r.get("total_specialties")),
            "gap_members": _safe_int(r.get("gap_members")),
        }

    for r in ghost_rows:
        fips = r.get("county_fips") or ""
        if not fips:
            continue
        entry = merged.setdefault(fips, {"county_fips": fips, "county_name": "", "county_type": ""})
        entry["ghost_flagged_count"] = _safe_int(r.get("ghost_flagged_count"))
        entry["ghost_high_count"] = _safe_int(r.get("ghost_high_count"))
        entry["ghost_impact_members"] = _safe_int(r.get("ghost_impact_members"))

    for r in leakage_rows:
        fips = r.get("county_fips") or ""
        if not fips:
            continue
        entry = merged.setdefault(fips, {"county_fips": fips, "county_name": "", "county_type": ""})
        if not entry.get("county_name"):
            entry["county_name"] = r.get("county_name") or ""
        if not entry.get("county_type"):
            entry["county_type"] = r.get("county_type") or ""
        entry["oon_claims"] = _safe_int(r.get("oon_claims"))
        entry["leakage_cost"] = _safe_float(r.get("leakage_cost"))
        entry["oon_members"] = _safe_int(r.get("oon_members"))

    for r in provider_rows:
        fips = r.get("county_fips") or ""
        if not fips:
            continue
        entry = merged.setdefault(fips, {"county_fips": fips, "county_name": "", "county_type": ""})
        entry["total_providers"] = _safe_int(r.get("total_providers"))
        entry["inn_providers"] = _safe_int(r.get("inn_providers"))
        entry["oon_providers"] = _safe_int(r.get("oon_providers"))
        entry["latitude"] = _safe_float(r.get("centroid_lat"))
        entry["longitude"] = _safe_float(r.get("centroid_lon"))

    return [CountyMapMetric(**data) for data in merged.values()]


# ---------------------------------------------------------------------------
# Genie
# ---------------------------------------------------------------------------

@api.post("/genie/ask", response_model=GenieResponseOut)
async def genie_ask(question_in: GenieQuestionIn):
    return await asyncio.to_thread(ask_genie, question_in)

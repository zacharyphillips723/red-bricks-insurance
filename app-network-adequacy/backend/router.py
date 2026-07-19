"""FastAPI route handlers for the Network Adequacy Portal.

All analytics data is read from Unity Catalog via Statement Execution API.
"""

import asyncio
import traceback
from typing import Optional

import json

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from .env_config import (
    LLM_ENDPOINT, NET_AGENT_ENDPOINT, SQL_WAREHOUSE_ID, UC_CATALOG,
    UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX,
)
from .genie import ask_genie
from .models import (
    ComplianceRow,
    ComplianceSummary,
    CountyComplianceSummary,
    CountyMapMetric,
    DashboardStats,
    GeoMemberCluster,
    GeoProvider,
    GenieQuestionIn,
    GenieResponseOut,
    GhostProviderRow,
    GhostSummary,
    LeakageByCounty,
    LeakageByReason,
    LeakageBySpecialty,
    LeakageSummary,
    NetworkGap,
    OutreachLetterRequest,
    OutreachLetterResponse,
    RecruitmentRecord,
    RecruitmentStatusUpdate,
    RecruitmentTarget,
)

api = APIRouter(prefix="/api")

OBSERVED_MODELS = [NET_AGENT_ENDPOINT, LLM_ENDPOINT]


def _actor(request: Request) -> str:
    """Resolve the acting user from Databricks Apps forwarded identity headers."""
    h = request.headers
    return (h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username")
            or h.get("X-Forwarded-User") or "network_ops")

# SQL-safe catalog quoting (handles hyphens)
_cat = f"`{UC_CATALOG}`" if UC_CATALOG else "`red_bricks_insurance`"


def _execute_sql(query: str, params: list | None = None) -> list[dict]:
    """Execute a SQL query via Statement Execution API and return rows as dicts.

    `params` is a list of {name, value, type?} dicts bound as named SQL
    parameters (:name) — use for any user-supplied value to avoid injection.
    """
    from databricks.sdk.service.sql import StatementParameterListItem

    w = WorkspaceClient()
    kwargs = {"warehouse_id": SQL_WAREHOUSE_ID, "statement": query, "wait_timeout": "30s"}
    if params:
        kwargs["parameters"] = [
            StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
            for p in params
        ]
    resp = w.statement_execution.execute_statement(**kwargs)
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
    params: list[dict] = []
    if county:
        filters.append("county_name = :county")
        params.append({"name": "county", "value": county})
    if specialty:
        filters.append("cms_specialty_type = :specialty")
        params.append({"name": "specialty", "value": specialty})
    if compliant_only is True:
        filters.append("is_compliant = TRUE")
    elif compliant_only is False:
        filters.append("is_compliant = FALSE")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT * FROM {_cat}.network.gold_network_adequacy_compliance
        {where}
        ORDER BY pct_compliant ASC
    """, params)
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
    params: list[dict] = []
    if flagged_only:
        filters.append("is_ghost_flagged = TRUE")
    if severity:
        filters.append("ghost_severity = :severity")
        params.append({"name": "severity", "value": severity})
    if specialty:
        filters.append("specialty = :specialty")
        params.append({"name": "specialty", "value": specialty})
    if county:
        filters.append("county = :county")
        params.append({"name": "county", "value": county})

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT * FROM {_cat}.network.gold_ghost_network_flags
        {where}
        ORDER BY
            CASE ghost_severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
            impact_members DESC
        LIMIT 200
    """, params)
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
# Geographic / Map Detail Endpoints
# ---------------------------------------------------------------------------

@api.get("/geographic/providers", response_model=list[GeoProvider])
async def get_geographic_providers():
    """Provider locations with lat/lon, specialty, name for map markers."""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT
            pg.npi,
            pg.provider_name,
            pg.specialty,
            pg.cms_specialty_type,
            pg.network_status,
            cc.county_name,
            ROUND(pg.provider_latitude, 5) AS latitude,
            ROUND(pg.provider_longitude, 5) AS longitude,
            COALESCE(pg.panel_size, 0) AS panel_size,
            pg.accepts_new_patients,
            pg.telehealth_capable
        FROM {_cat}.network.silver_provider_geo pg
        JOIN {_cat}.network.silver_county_classification cc
          ON pg.county_fips = cc.county_fips
        WHERE pg.is_active = TRUE
        ORDER BY pg.specialty, pg.provider_name
    """)
    return [
        GeoProvider(
            npi=r.get("npi") or "",
            provider_name=r.get("provider_name") or "",
            specialty=r.get("specialty") or "",
            cms_specialty_type=r.get("cms_specialty_type"),
            network_status=r.get("network_status") or "In-Network",
            county_name=r.get("county_name") or "",
            latitude=_safe_float(r.get("latitude")),
            longitude=_safe_float(r.get("longitude")),
            panel_size=_safe_int(r.get("panel_size")),
            accepts_new_patients=_safe_bool(r.get("accepts_new_patients")),
            telehealth_capable=_safe_bool(r.get("telehealth_capable")),
        )
        for r in rows
    ]


@api.get("/geographic/members", response_model=list[GeoMemberCluster])
async def get_geographic_members():
    """Member clusters aggregated by county + zip (~500 points max) for map display."""
    rows = await asyncio.to_thread(_execute_sql, f"""
        WITH member_zip AS (
            SELECT
                mg.county_fips,
                cc.county_name,
                mg.zip_code,
                ROUND(AVG(mg.member_latitude), 5) AS latitude,
                ROUND(AVG(mg.member_longitude), 5) AS longitude,
                COUNT(*) AS member_count
            FROM {_cat}.network.silver_member_geo mg
            JOIN {_cat}.network.silver_county_classification cc
              ON mg.county_fips = cc.county_fips
            GROUP BY mg.county_fips, cc.county_name, mg.zip_code
        ),
        -- Assign risk tiers based on member density & county compliance
        with_compliance AS (
            SELECT
                mz.*,
                COALESCE(comp.avg_compliance, 100) AS avg_compliance
            FROM member_zip mz
            LEFT JOIN (
                SELECT county_fips, AVG(pct_compliant) AS avg_compliance
                FROM {_cat}.network.gold_network_adequacy_compliance
                GROUP BY county_fips
            ) comp ON mz.county_fips = comp.county_fips
        )
        SELECT
            county_fips,
            county_name,
            zip_code,
            latitude,
            longitude,
            member_count,
            CASE
                WHEN avg_compliance < 60 THEN 'Critical'
                WHEN avg_compliance < 75 THEN 'High'
                WHEN avg_compliance < 90 THEN 'Standard'
                ELSE 'Low'
            END AS risk_tier
        FROM with_compliance
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY member_count DESC
        LIMIT 500
    """)
    return [
        GeoMemberCluster(
            county_fips=r.get("county_fips") or "",
            county_name=r.get("county_name") or "",
            zip_code=r.get("zip_code") or "",
            latitude=_safe_float(r.get("latitude")),
            longitude=_safe_float(r.get("longitude")),
            member_count=_safe_int(r.get("member_count")),
            risk_tier=r.get("risk_tier") or "Standard",
        )
        for r in rows
    ]


@api.get("/geographic/compliance", response_model=list[CountyComplianceSummary])
async def get_geographic_compliance():
    """County-level compliance with pass/fail by specialty for map overlays."""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT
            c.county_fips,
            c.county_name,
            c.county_type,
            ROUND(AVG(pg.centroid_lat), 5) AS latitude,
            ROUND(AVG(pg.centroid_lon), 5) AS longitude,
            SUM(CASE WHEN c.is_compliant THEN 1 ELSE 0 END) AS specialties_compliant,
            SUM(CASE WHEN NOT c.is_compliant THEN 1 ELSE 0 END) AS specialties_non_compliant,
            COUNT(*) AS total_specialties,
            SUM(c.gap_members) AS gap_members,
            SUM(c.total_members) AS total_members,
            ROUND(AVG(c.pct_compliant), 1) AS avg_compliance_pct,
            CONCAT_WS(', ', COLLECT_SET(
                CASE WHEN NOT c.is_compliant THEN c.cms_specialty_type END
            )) AS non_compliant_list
        FROM {_cat}.network.gold_network_adequacy_compliance c
        LEFT JOIN (
            SELECT county_fips,
                   ROUND(AVG(provider_latitude), 5) AS centroid_lat,
                   ROUND(AVG(provider_longitude), 5) AS centroid_lon
            FROM {_cat}.network.silver_provider_geo
            GROUP BY county_fips
        ) pg ON c.county_fips = pg.county_fips
        GROUP BY c.county_fips, c.county_name, c.county_type
        ORDER BY avg_compliance_pct ASC
    """)
    return [
        CountyComplianceSummary(
            county_fips=r.get("county_fips") or "",
            county_name=r.get("county_name") or "",
            county_type=r.get("county_type") or "",
            latitude=_safe_float(r.get("latitude")),
            longitude=_safe_float(r.get("longitude")),
            overall_compliant=_safe_int(r.get("specialties_non_compliant")) == 0,
            specialties_compliant=_safe_int(r.get("specialties_compliant")),
            specialties_non_compliant=_safe_int(r.get("specialties_non_compliant")),
            total_specialties=_safe_int(r.get("total_specialties")),
            gap_members=_safe_int(r.get("gap_members")),
            total_members=_safe_int(r.get("total_members")),
            avg_compliance_pct=_safe_float(r.get("avg_compliance_pct")),
            non_compliant_specialties=[
                s.strip() for s in (r.get("non_compliant_list") or "").split(",")
                if s.strip()
            ],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Recruitment Workflow — persisted to a governed UC Delta table (read-only app
# has no Lakebase; recruitment-pipeline state is written back to Unity Catalog
# for traceability and cross-session/replica durability).
# ---------------------------------------------------------------------------

_RECRUIT_STATUS_TABLE = f"{_cat}.network.provider_recruitment_status"
_VALID_STATUSES = {"Identified", "Contacted", "Interested", "Contracted", "Active"}


def _recruitment_record_from_row(r: dict) -> "RecruitmentRecord":
    return RecruitmentRecord(
        npi=r.get("npi") or "",
        specialty=r.get("specialty"),
        county_name=r.get("county_name"),
        status=r.get("status") or "Identified",
        potential_savings=_safe_float(r.get("potential_savings")),
        members_served=_safe_int(r.get("members_served")),
        priority_score=_safe_float(r.get("priority_score")),
        notes=r.get("notes"),
        updated_at=str(r.get("updated_at") or ""),
    )


@api.get("/recruitment/status", response_model=list[RecruitmentRecord])
async def get_recruitment_statuses():
    """Return all tracked recruitment records from the governed Delta table."""
    rows = await asyncio.to_thread(_execute_sql, f"""
        SELECT npi, specialty, county_name, status, potential_savings,
               members_served, priority_score, notes, CAST(updated_at AS STRING) AS updated_at
        FROM {_RECRUIT_STATUS_TABLE}
        ORDER BY updated_at DESC
    """)
    return [_recruitment_record_from_row(r) for r in rows]


def _upsert_recruitment_status(update, actor: str) -> "RecruitmentRecord":
    """Look up provider info + MERGE the recruitment status into the Delta table."""
    from datetime import datetime, timezone

    info_rows = _execute_sql(
        f"""SELECT specialty, county_name,
                   ROUND(potential_savings, 0) AS potential_savings,
                   members_served,
                   ROUND(recruitment_priority_score, 0) AS priority_score
            FROM {_cat}.network.gold_provider_recruitment_targets
            WHERE rendering_provider_npi = :npi LIMIT 1""",
        [{"name": "npi", "value": update.npi}],
    )
    info = info_rows[0] if info_rows else {}
    now = datetime.now(timezone.utc).isoformat()
    params = [
        {"name": "npi", "value": update.npi},
        {"name": "specialty", "value": info.get("specialty") or ""},
        {"name": "county", "value": info.get("county_name") or ""},
        {"name": "status", "value": update.status},
        {"name": "ps", "value": str(_safe_float(info.get("potential_savings"))), "type": "DOUBLE"},
        {"name": "ms", "value": str(_safe_int(info.get("members_served"))), "type": "INT"},
        {"name": "pri", "value": str(_safe_float(info.get("priority_score"))), "type": "DOUBLE"},
        {"name": "notes", "value": update.notes or ""},
        {"name": "actor", "value": actor},
        {"name": "ts", "value": now, "type": "TIMESTAMP"},
    ]
    _execute_sql(
        f"""MERGE INTO {_RECRUIT_STATUS_TABLE} t
            USING (SELECT :npi AS npi) s ON t.npi = s.npi
            WHEN MATCHED THEN UPDATE SET
                status = :status, notes = :notes, updated_by = :actor, updated_at = :ts
            WHEN NOT MATCHED THEN INSERT
                (npi, specialty, county_name, status, potential_savings, members_served,
                 priority_score, notes, updated_by, updated_at)
                VALUES (:npi, :specialty, :county, :status, :ps, :ms, :pri, :notes, :actor, :ts)""",
        params,
    )
    return RecruitmentRecord(
        npi=update.npi, specialty=info.get("specialty"), county_name=info.get("county_name"),
        status=update.status, potential_savings=_safe_float(info.get("potential_savings")),
        members_served=_safe_int(info.get("members_served")),
        priority_score=_safe_float(info.get("priority_score")),
        notes=update.notes, updated_at=now,
    )


@api.post("/recruitment/status", response_model=RecruitmentRecord)
async def update_recruitment_status(update: RecruitmentStatusUpdate, request: Request):
    """Update recruitment status for a provider — persisted to the governed table."""
    if update.status not in _VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {_VALID_STATUSES}")
    return await asyncio.to_thread(_upsert_recruitment_status, update, _actor(request))


@api.post("/recruitment/outreach-letter", response_model=OutreachLetterResponse)
async def generate_outreach_letter(req: OutreachLetterRequest):
    """Generate a recruitment outreach letter using the LLM endpoint."""
    try:
        w = WorkspaceClient()

        prompt = f"""You are a network development specialist at Red Bricks Insurance, a health insurance plan in North Carolina.
Write a professional, warm recruitment outreach letter to the following out-of-network provider, inviting them to join our in-network panel.

Provider Details:
- NPI: {req.npi}
- Name: {req.provider_name or 'Provider'}
- Specialty: {req.specialty or 'Healthcare Provider'}
- County: {req.county_name or 'North Carolina'}
- Estimated Annual Savings if In-Network: ${req.potential_savings:,.0f}
- Members Currently Served (OON): {req.members_served}

Key talking points to include:
1. Growing member demand in their area for their specialty
2. Competitive reimbursement rates and streamlined credentialing
3. The number of members who already seek their care out-of-network
4. Benefits of being part of the Red Bricks Insurance network (referral volume, marketing, support)
5. Next steps to start the credentialing process

Keep the tone professional yet personable. The letter should be about 300 words.
Sign it as "Network Development Team, Red Bricks Insurance"."""

        response = await asyncio.to_thread(
            w.serving_endpoints.query,
            name=LLM_ENDPOINT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
        )

        letter_text = ""
        if response.choices:
            letter_text = response.choices[0].message.content or ""

        if not letter_text:
            letter_text = _fallback_letter(req)

        return OutreachLetterResponse(npi=req.npi, letter=letter_text)

    except Exception as e:
        traceback.print_exc()
        # Fallback to template letter if LLM fails
        return OutreachLetterResponse(
            npi=req.npi,
            letter=_fallback_letter(req),
        )


def _fallback_letter(req: OutreachLetterRequest) -> str:
    """Template-based fallback letter when LLM is unavailable."""
    name = req.provider_name or "Provider"
    specialty = req.specialty or "healthcare services"
    county = req.county_name or "your area"
    savings = f"${req.potential_savings:,.0f}"
    members = req.members_served

    return f"""Dear {name},

I am writing on behalf of Red Bricks Insurance's Network Development Team to invite you to join our growing in-network provider panel in North Carolina.

Our data shows that {members} of our members in {county} currently seek {specialty} services from your practice on an out-of-network basis. This represents significant demand, and we believe a partnership would benefit both your practice and our membership.

By joining the Red Bricks Insurance network, you would gain access to:

- A steady referral stream from our {members}+ members already seeking your care
- Competitive reimbursement rates aligned with market standards
- Streamlined credentialing with a dedicated support team
- Marketing exposure through our provider directory and member tools
- Estimated annual cost reduction of {savings} for your patients

Our credentialing process is straightforward and typically completed within 30-45 days. We would be happy to schedule a brief call to discuss terms, answer questions, and walk through next steps.

Please reply to this email or contact our Network Development Team at network.development@redbricksinsurance.com or (800) 555-0199 to get started.

We look forward to welcoming you to the Red Bricks Insurance network.

Sincerely,
Network Development Team
Red Bricks Insurance"""


# ---------------------------------------------------------------------------
# Genie
# ---------------------------------------------------------------------------

@api.post("/genie/ask", response_model=GenieResponseOut)
async def genie_ask(question_in: GenieQuestionIn):
    return await asyncio.to_thread(ask_genie, question_in)


# ---------------------------------------------------------------------------
# Network Adequacy Agent + What-If Simulation
# ---------------------------------------------------------------------------

@api.post("/agent/chat/stream")
async def agent_chat_stream(payload: dict):
    """SSE tool-calling network agent — streams tool-progress then the answer."""
    from .agent import stream_network_agent
    message = payload.get("message", "")
    history = payload.get("conversation_history") or None

    async def event_source():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for et, pl in stream_network_agent(message, history):
                    loop.call_soon_threadsafe(queue.put_nowait, (et, pl))
            except Exception as e:  # pragma: no cover
                loop.call_soon_threadsafe(queue.put_nowait, ("error", {"message": str(e)}))
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, _SENTINEL)

        producer = loop.run_in_executor(None, _produce)
        try:
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    break
                et, pl = item
                yield f"event: {et}\ndata: {json.dumps(pl, default=str)}\n\n"
        finally:
            await producer

    return StreamingResponse(event_source(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@api.post("/simulate/recruitment")
async def simulate_recruitment_endpoint(payload: dict):
    """What-if network simulation: recompute county+specialty compliance if the
    given OON provider NPIs are recruited in-network (geospatial recompute)."""
    from .agent import simulate_recruitment
    county = payload.get("county", "")
    specialty = payload.get("specialty", "")
    npis = payload.get("npis") or None
    if not county or not specialty:
        raise HTTPException(400, "county and specialty are required")
    return await asyncio.to_thread(simulate_recruitment, county, specialty, npis)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

@api.get("/observability/traces")
async def observability_traces():
    spans_table = f"`{UC_CATALOG}`.`{UC_TRACE_SCHEMA}`.`{UC_TRACE_TABLE_PREFIX}_otel_spans`"
    sql = f"""
        SELECT trace_id, MIN(start_time_unix_nano) AS s, MAX(end_time_unix_nano) AS e,
               COUNT(*) AS span_count,
               CASE WHEN SUM(CASE WHEN status.code='STATUS_CODE_ERROR' THEN 1 ELSE 0 END)>0
                    THEN 'ERROR' ELSE 'OK' END AS trace_status
        FROM {spans_table} GROUP BY trace_id ORDER BY s DESC LIMIT 25
    """
    try:
        rows = await asyncio.to_thread(_execute_sql, sql)
        return {"traces": [{
            "request_id": d.get("trace_id", ""),
            "timestamp_ms": int(d.get("s") or 0) // 1_000_000,
            "execution_time_ms": (int(d.get("e") or 0) - int(d.get("s") or 0)) // 1_000_000,
            "status": d.get("trace_status", "UNKNOWN"),
            "span_count": int(d.get("span_count") or 0),
        } for d in rows]}
    except Exception as e:
        print(f"[observability] trace fetch error: {e}")
        return {"traces": [], "error": str(e)}


@api.get("/observability/costs")
async def observability_costs():
    endpoints = ", ".join(f"'{m}'" for m in OBSERVED_MODELS)
    try:
        try:
            wid = WorkspaceClient().get_workspace_id()
            wf = f"AND eu.workspace_id = '{wid}'" if wid else ""
        except Exception:
            wf = ""
        rows = await asyncio.to_thread(_execute_sql, f"""
            SELECT se.endpoint_name AS endpoint, COUNT(*) AS request_count,
                   COALESCE(SUM(eu.input_token_count),0) AS total_input_tokens,
                   COALESCE(SUM(eu.output_token_count),0) AS total_output_tokens,
                   CASE se.endpoint_name
                     WHEN 'databricks-llama-4-maverick'
                       THEN ROUND(SUM(eu.input_token_count)*0.40/1000000 + SUM(eu.output_token_count)*1.60/1000000, 4)
                     WHEN 'databricks-claude-haiku-4-5'
                       THEN ROUND(SUM(eu.input_token_count)*1.00/1000000 + SUM(eu.output_token_count)*5.00/1000000, 4)
                     ELSE 0 END AS estimated_cost_usd
            FROM system.serving.endpoint_usage eu
            JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
            WHERE se.endpoint_name IN ({endpoints})
              AND eu.request_time >= DATE_SUB(CURRENT_TIMESTAMP(), 30) {wf}
            GROUP BY se.endpoint_name ORDER BY request_count DESC
        """)
        return {"costs": rows}
    except Exception as e:
        print(f"[observability] cost query error: {e}")
        return {"costs": [], "error": str(e)}

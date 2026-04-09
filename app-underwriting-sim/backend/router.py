"""FastAPI routes for the Underwriting Simulation Portal."""

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException

from .agent import query_underwriting_agent
from .data_loader import data_cache
from .database import db
from .genie import ask_genie
from .models import (
    AgentChatIn,
    AgentChatOut,
    BaselineSummaryOut,
    ComparisonIn,
    ComparisonOut,
    GenieQuestionIn,
    GenieResponseOut,
    SimulateIn,
    SimulateOut,
    SimulationDetailOut,
    SimulationListOut,
    SimulationUpdateIn,
    AuditLogEntry,
)
from .scenarios import (
    create_comparison,
    delete_simulation,
    get_audit_log,
    get_comparison,
    get_simulation,
    list_comparisons,
    list_simulations,
    save_simulation,
    update_simulation,
)
from .simulation_engine import run_simulation

api = APIRouter(prefix="/api")


# ===================================================================
# Health
# ===================================================================

@api.get("/health")
async def health():
    import os
    return {
        "status": "ok",
        "db_initialized": db._initialized,
        "lakebase_project": os.environ.get("LAKEBASE_PROJECT_ID", "not set"),
    }


# ===================================================================
# Baseline data
# ===================================================================

@api.get("/baseline", response_model=BaselineSummaryOut)
async def get_baseline(lob: Optional[str] = None):
    """Current book-level financials from cached gold tables."""
    summary = await asyncio.to_thread(data_cache.get_baseline_summary, lob)
    return BaselineSummaryOut(**summary)


@api.post("/baseline/refresh")
async def refresh_baseline():
    """Force refresh cached gold table data."""
    data_cache.invalidate()
    return {"status": "cache_invalidated"}


# ===================================================================
# Simulations — run
# ===================================================================

@api.post("/simulate", response_model=SimulateOut)
async def simulate(body: SimulateIn):
    """Run a what-if simulation and optionally save to Lakebase."""
    result = await asyncio.to_thread(
        run_simulation,
        data_cache,
        body.simulation_type.value,
        body.parameters,
    )

    sim_id = None

    if body.save:
        if not body.name:
            raise HTTPException(400, "name is required when save=True")
        if not db._initialized:
            raise HTTPException(503, "Database not initialized — cannot save simulation")

        async with db.session() as session:
            saved = await save_simulation(
                session,
                simulation_name=body.name,
                simulation_type=body.simulation_type.value,
                parameters=body.parameters,
                results=result,
                baseline_snapshot=result.get("baseline", {}),
                created_by="underwriter",
                scope_lob=body.parameters.get("lob"),
                scope_group_id=body.parameters.get("group_id"),
            )
            sim_id = saved["simulation_id"]

    return SimulateOut(
        simulation_id=sim_id,
        simulation_type=body.simulation_type,
        baseline=result["baseline"],
        projected=result["projected"],
        delta=result["delta"],
        delta_pct=result["delta_pct"],
        narrative=result["narrative"],
        warnings=result.get("warnings", []),
    )


# ===================================================================
# Simulations — CRUD
# ===================================================================

@api.get("/simulations", response_model=list[SimulationListOut])
async def list_sims(
    simulation_type: Optional[str] = None,
    status: Optional[str] = None,
    lob: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_simulations(
            session,
            simulation_type=simulation_type,
            status=status,
            lob=lob,
            limit=limit,
            offset=offset,
        )
        return [SimulationListOut(**r) for r in rows]


@api.get("/simulations/{sim_id}", response_model=SimulationDetailOut)
async def get_sim(sim_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await get_simulation(session, sim_id)
        if not row:
            raise HTTPException(404, "Simulation not found")
        return SimulationDetailOut(**row)


@api.patch("/simulations/{sim_id}", response_model=SimulationDetailOut)
async def update_sim(sim_id: str, body: SimulationUpdateIn):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await update_simulation(
            session,
            sim_id,
            actor="underwriter",
            status=body.status.value if body.status else None,
            notes=body.notes,
        )
        if not row:
            raise HTTPException(404, "Simulation not found")
        return SimulationDetailOut(**row)


@api.delete("/simulations/{sim_id}")
async def delete_sim(sim_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        deleted = await delete_simulation(session, sim_id)
        if not deleted:
            raise HTTPException(404, "Simulation not found")
        return {"status": "deleted"}


@api.get("/simulations/{sim_id}/audit", response_model=list[AuditLogEntry])
async def get_sim_audit(sim_id: str):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await get_audit_log(session, sim_id)
        return [AuditLogEntry(**r) for r in rows]


# ===================================================================
# Comparisons
# ===================================================================

@api.post("/comparisons", response_model=ComparisonOut)
async def create_comp(body: ComparisonIn):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        comp = await create_comparison(
            session,
            comparison_name=body.comparison_name,
            simulation_ids=[str(s) for s in body.simulation_ids],
            created_by="underwriter",
            notes=body.notes,
        )
        return ComparisonOut(**comp)


@api.get("/comparisons", response_model=list[ComparisonOut])
async def list_comps(limit: int = 20, offset: int = 0):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_comparisons(session, limit=limit, offset=offset)
        return [ComparisonOut(**r) for r in rows]


@api.get("/comparisons/{comp_id}", response_model=ComparisonOut)
async def get_comp(comp_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        comp = await get_comparison(session, comp_id)
        if not comp:
            raise HTTPException(404, "Comparison not found")
        return ComparisonOut(**comp)


# ===================================================================
# Agent chat
# ===================================================================

@api.post("/agent/chat", response_model=AgentChatOut)
async def agent_chat(body: AgentChatIn):
    result = await asyncio.to_thread(
        query_underwriting_agent,
        body.message,
        body.conversation_history or None,
    )
    return AgentChatOut(**result)


# ===================================================================
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut)
async def genie_ask(body: GenieQuestionIn):
    result = await asyncio.to_thread(ask_genie, body)
    return result

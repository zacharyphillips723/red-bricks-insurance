"""Statement Execution API data loader with in-memory caching.

Loads baseline data from gold tables for simulation inputs. Small aggregate
tables are pre-loaded at startup and refreshed every 15 minutes. Larger
per-group/per-member queries are fetched on demand.
"""

import time
import traceback
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID

CACHE_TTL = 15 * 60  # 15 minutes


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


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(float(val)) if val is not None else default
    except (ValueError, TypeError):
        return default


class DataCache:
    """In-memory cache for baseline gold table data."""

    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, list[dict]]] = {}  # key -> (timestamp, data)

    def _get_cached(self, key: str) -> Optional[list[dict]]:
        if key in self._cache:
            ts, data = self._cache[key]
            if time.time() - ts < CACHE_TTL:
                return data
        return None

    def _set_cached(self, key: str, data: list[dict]) -> list[dict]:
        self._cache[key] = (time.time(), data)
        return data

    def invalidate(self) -> None:
        self._cache.clear()

    # --- Pre-loaded aggregate tables ---

    def get_pmpm(self) -> list[dict]:
        cached = self._get_cached("pmpm")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_pmpm")
            return self._set_cached("pmpm", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_pmpm: {e}")
            return []

    def get_mlr(self) -> list[dict]:
        cached = self._get_cached("mlr")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_mlr")
            return self._set_cached("mlr", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_mlr: {e}")
            return []

    def get_enrollment_summary(self) -> list[dict]:
        cached = self._get_cached("enrollment_summary")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.members.gold_enrollment_summary")
            return self._set_cached("enrollment_summary", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_enrollment_summary: {e}")
            return []

    def get_utilization(self) -> list[dict]:
        cached = self._get_cached("utilization")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_utilization_per_1000")
            return self._set_cached("utilization", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_utilization_per_1000: {e}")
            return []

    def get_ibnr_completion_factors(self) -> list[dict]:
        cached = self._get_cached("ibnr_cf")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_ibnr_completion_factors")
            return self._set_cached("ibnr_cf", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_ibnr_completion_factors: {e}")
            return []

    def get_ibnr_triangle(self) -> list[dict]:
        cached = self._get_cached("ibnr_triangle")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_ibnr_triangle")
            return self._set_cached("ibnr_triangle", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_ibnr_triangle: {e}")
            return []

    def get_risk_adjustment(self) -> list[dict]:
        cached = self._get_cached("risk_adj")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_risk_adjustment_analysis")
            return self._set_cached("risk_adj", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_risk_adjustment_analysis: {e}")
            return []

    def get_coding_completeness(self) -> list[dict]:
        cached = self._get_cached("coding")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_coding_completeness")
            return self._set_cached("coding", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_coding_completeness: {e}")
            return []

    def get_tcoc_summary(self) -> list[dict]:
        cached = self._get_cached("tcoc_summary")
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(f"SELECT * FROM {UC_CATALOG}.analytics.gold_tcoc_summary")
            return self._set_cached("tcoc_summary", rows)
        except Exception as e:
            print(f"[data_loader] Failed to load gold_tcoc_summary: {e}")
            return []

    # --- On-demand (parameterized) queries ---

    def get_group_experience(self, group_id: str) -> list[dict]:
        key = f"group_exp_{group_id}"
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(
                f"SELECT * FROM {UC_CATALOG}.analytics.gold_group_experience WHERE group_id = :gid",
                params=[{"name": "gid", "value": group_id}],
            )
            return self._set_cached(key, rows)
        except Exception as e:
            print(f"[data_loader] Failed to load group experience for {group_id}: {e}")
            return []

    def get_group_stop_loss(self, group_id: str) -> list[dict]:
        key = f"group_sl_{group_id}"
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(
                f"SELECT * FROM {UC_CATALOG}.analytics.gold_group_stop_loss WHERE group_id = :gid",
                params=[{"name": "gid", "value": group_id}],
            )
            return self._set_cached(key, rows)
        except Exception as e:
            print(f"[data_loader] Failed to load group stop-loss for {group_id}: {e}")
            return []

    def get_group_renewal(self, group_id: str) -> list[dict]:
        key = f"group_ren_{group_id}"
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(
                f"SELECT * FROM {UC_CATALOG}.analytics.gold_group_renewal WHERE group_id = :gid",
                params=[{"name": "gid", "value": group_id}],
            )
            return self._set_cached(key, rows)
        except Exception as e:
            print(f"[data_loader] Failed to load group renewal for {group_id}: {e}")
            return []

    def get_member_tcoc_by_group(self, group_id: str) -> list[dict]:
        key = f"tcoc_grp_{group_id}"
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(
                f"""SELECT * FROM {UC_CATALOG}.analytics.gold_member_tcoc
                    WHERE group_id = :gid ORDER BY actual_cost DESC""",
                params=[{"name": "gid", "value": group_id}],
            )
            return self._set_cached(key, rows)
        except Exception as e:
            print(f"[data_loader] Failed to load member TCOC for {group_id}: {e}")
            return []

    def get_benefits_by_lob(self, lob: str) -> list[dict]:
        key = f"benefits_{lob}"
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        try:
            rows = _execute_sql(
                f"SELECT * FROM {UC_CATALOG}.benefits.silver_benefits WHERE lob = :lob",
                params=[{"name": "lob", "value": lob}],
            )
            return self._set_cached(key, rows)
        except Exception as e:
            print(f"[data_loader] Failed to load benefits for {lob}: {e}")
            return []

    # --- Baseline summary (book-level financials) ---

    def get_baseline_summary(self, lob: Optional[str] = None) -> dict:
        """Aggregate current-state financials from cached data."""
        mlr_data = self.get_mlr()
        pmpm_data = self.get_pmpm()
        enrollment = self.get_enrollment_summary()

        # Column name helper — gold tables use 'line_of_business'
        def _lob(row: dict) -> str:
            return row.get("line_of_business") or row.get("lob") or "Unknown"

        if lob:
            mlr_data = [r for r in mlr_data if _lob(r) == lob]
            pmpm_data = [r for r in pmpm_data if _lob(r) == lob]
            enrollment = [r for r in enrollment if _lob(r) == lob]

        total_premium = sum(_safe_float(r.get("total_premiums")) for r in mlr_data)
        total_claims = sum(_safe_float(r.get("total_claims_paid")) for r in mlr_data)
        total_members = sum(
            _safe_int(r.get("active_member_count") or r.get("member_count") or r.get("total_members"))
            for r in enrollment
        )
        total_mm = sum(
            _safe_int(r.get("member_months") or r.get("total_member_months"))
            for r in pmpm_data
        )
        overall_mlr = (total_claims / total_premium * 100) if total_premium else 0

        pmpm_by_lob = {_lob(r): _safe_float(r.get("pmpm_paid")) for r in pmpm_data}
        mlr_by_lob = {_lob(r): _safe_float(r.get("mlr")) for r in mlr_data}
        members_by_lob = {
            _lob(r): _safe_int(r.get("active_member_count") or r.get("member_count") or r.get("total_members"))
            for r in enrollment
        }

        return {
            "total_premium": total_premium,
            "total_claims": total_claims,
            "total_members": total_members,
            "total_member_months": total_mm,
            "overall_mlr": overall_mlr,
            "pmpm_by_lob": pmpm_by_lob,
            "mlr_by_lob": mlr_by_lob,
            "member_count_by_lob": members_by_lob,
        }


# Module-level singleton
data_cache = DataCache()

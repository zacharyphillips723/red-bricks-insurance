"""Layered denial-risk engine — the heart of the Claim Scrubber.

Given a draft claim or prior-auth request, fan out three layers and compose a
0-100 denial-risk score plus ranked, remediable reason cards:

  Layer 1  deterministic rules  — eligibility (CO-27), dx<->px coding (CO-11),
                                   auth-required-but-missing (CO-197),
                                   frequency/limits (CO-151), completeness (CO-16)
  Layer 2  ML classifier        — denial-risk-scorer endpoint: P(denied) +
                                   per-reason probabilities
  Layer 3  medical-policy RAG    — CO-50 (not medically necessary),
                                   CO-55/CO-96 (experimental/investigational)

Deterministic hits use exact code matching against silver_medical_policy_rules
(reusing the app-prior-auth Tier-1 helpers). ML + RAG run in parallel with the
rules. Every card is enriched with a remediation from denial_remediation_playbook.
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any

import mlflow
from databricks.sdk import WorkspaceClient

from .env_config import UC_CATALOG, UC_SCHEMA, DENIAL_ENDPOINT
from .denial_agent import _execute_sql, assess_policy_rag

_CAT = f"`{UC_CATALOG}`"
_RULES_TABLE = f"{_CAT}.prior_auth.silver_medical_policy_rules"

# ---------------------------------------------------------------------------
# Reason taxonomy (aligned with claims.carc_reference / the reason classifier)
# ---------------------------------------------------------------------------

REASON_LABELS = {
    "no_auth": "Prior authorization missing",
    "not_medically_necessary": "Not medically necessary",
    "experimental": "Experimental / investigational",
    "missing_info": "Missing information / documentation",
    "coding_mismatch": "Diagnosis–procedure mismatch",
    "eligibility": "Coverage / eligibility",
    "frequency_limit": "Frequency / benefit limit exceeded",
    "other": "Other payer edit",
}

CATEGORY_TO_CARC = {
    "no_auth": "CO-197",
    "not_medically_necessary": "CO-50",
    "experimental": "CO-55",
    "missing_info": "CO-16",
    "coding_mismatch": "CO-11",
    "eligibility": "CO-27",
    "frequency_limit": "CO-151",
    "other": "CO-16",
}

CARC_TO_CATEGORY = {
    "CO-197": "no_auth", "CO-50": "not_medically_necessary",
    "CO-55": "experimental", "CO-96": "experimental",
    "CO-16": "missing_info", "CO-11": "coding_mismatch",
    "CO-27": "eligibility", "CO-151": "frequency_limit",
}

_LAYER_PRIORITY = {"rule": 3, "rag": 2, "ml": 1}
_ML_REASON_THRESHOLD = 0.15


# ---------------------------------------------------------------------------
# Tier-1 helpers (adapted from app-prior-auth/backend/documents.py)
# ---------------------------------------------------------------------------

def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, dict)):
        return len(value) > 0
    s = str(value).strip()
    return s not in ("", "null", "None", "[]", "{}", '""')


def _split_codes(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    value = parsed
            except json.JSONDecodeError:
                pass
    if isinstance(value, list):
        items: list[str] = []
        for v in value:
            items.extend(re.split(r"[|,;\s]+", str(v)))
    else:
        items = re.split(r"[|,;\s]+", str(value))
    return [c.strip().upper() for c in items if c and c.strip()]


def _match_policy(procedure_codes: list[str], diagnosis_codes: list[str]) -> dict | None:
    """Find a medical policy whose covered codes match the request (exact codes)."""
    rows = _execute_sql(
        f"""SELECT policy_id, policy_name, service_category,
                   procedure_codes, diagnosis_codes
            FROM {_RULES_TABLE}
            WHERE rule_type IN ('clinical_criteria', 'coverage_criteria')"""
    )
    proc_set = set(procedure_codes)
    dx_set = set(diagnosis_codes)

    best = None
    for r in rows:
        policy_procs = set(_split_codes(r.get("procedure_codes")))
        policy_dx = set(_split_codes(r.get("diagnosis_codes")))
        proc_hits = sorted(proc_set & policy_procs)
        dx_hits = sorted(dx_set & policy_dx)
        if not proc_hits:
            continue
        candidate = {
            "policy_id": r.get("policy_id"),
            "policy_name": r.get("policy_name"),
            "service_category": r.get("service_category"),
            "procedure_match": bool(proc_hits),
            "diagnosis_match": bool(dx_hits),
            "matched_procedure_codes": proc_hits,
            "matched_diagnosis_codes": dx_hits,
        }
        if best is None or (candidate["diagnosis_match"] and not best["diagnosis_match"]):
            best = candidate
        if candidate["diagnosis_match"]:
            break
    return best


# ---------------------------------------------------------------------------
# Member / eligibility context
# ---------------------------------------------------------------------------

def get_member_context(member_id: str) -> dict:
    """Name + most-recent enrollment record for a member."""
    ctx: dict = {"member_id": member_id}
    try:
        name_rows = _execute_sql(
            f"SELECT full_name FROM {_CAT}.members.silver_members WHERE member_id = :mid LIMIT 1",
            [{"name": "mid", "value": member_id}],
        )
        if name_rows:
            ctx["member_name"] = name_rows[0].get("full_name")
    except Exception as e:
        print(f"[Scrub] member name lookup failed: {e}")
    try:
        enr = _execute_sql(
            f"""SELECT line_of_business,
                       CAST(eligibility_start_date AS STRING) AS eligibility_start_date,
                       CAST(eligibility_end_date AS STRING) AS eligibility_end_date,
                       is_active
                FROM {_CAT}.members.silver_enrollment
                WHERE member_id = :mid
                ORDER BY eligibility_start_date DESC LIMIT 1""",
            [{"name": "mid", "value": member_id}],
        )
        if enr:
            ctx.update(enr[0])
    except Exception as e:
        print(f"[Scrub] enrollment lookup failed: {e}")
    return ctx


def search_members(q: str, limit: int = 20) -> list[dict]:
    """Autocomplete members by id or name."""
    like = f"%{q}%"
    try:
        return _execute_sql(
            f"""SELECT m.member_id, m.full_name AS member_name,
                       e.line_of_business, e.is_active,
                       CAST(e.eligibility_start_date AS STRING) AS eligibility_start_date,
                       CAST(e.eligibility_end_date AS STRING) AS eligibility_end_date
                FROM {_CAT}.members.silver_members m
                LEFT JOIN (
                    SELECT member_id, line_of_business, is_active,
                           eligibility_start_date, eligibility_end_date,
                           ROW_NUMBER() OVER (PARTITION BY member_id
                               ORDER BY eligibility_start_date DESC) AS rn
                    FROM {_CAT}.members.silver_enrollment
                ) e ON m.member_id = e.member_id AND e.rn = 1
                WHERE m.member_id ILIKE :like OR m.full_name ILIKE :like
                LIMIT {int(limit)}""",
            [{"name": "like", "value": like}],
        )
    except Exception as e:
        print(f"[Scrub] member search failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Reference tables (CARC + remediation) — cached
# ---------------------------------------------------------------------------

_reference_cache: dict[str, dict] | None = None
_remediation_cache: dict[str, dict] | None = None


def _load_reference() -> tuple[dict, dict]:
    global _reference_cache, _remediation_cache
    if _reference_cache is not None and _remediation_cache is not None:
        return _reference_cache, _remediation_cache
    ref: dict[str, dict] = {}
    rem: dict[str, dict] = {}
    try:
        for r in _execute_sql(
            f"SELECT carc_code, group_code, reason_category, description, patient_vs_payer "
            f"FROM {_CAT}.{UC_SCHEMA}.carc_reference"
        ):
            ref[r["carc_code"]] = r
    except Exception as e:
        print(f"[Scrub] carc_reference load failed (using static labels): {e}")
    try:
        for r in _execute_sql(
            f"SELECT carc_code, remediation_text, required_action, doc_needed "
            f"FROM {_CAT}.{UC_SCHEMA}.denial_remediation_playbook"
        ):
            rem[r["carc_code"]] = r
    except Exception as e:
        print(f"[Scrub] remediation playbook load failed: {e}")
    _reference_cache, _remediation_cache = ref, rem
    return ref, rem


def get_carc_reference() -> list[dict]:
    ref, _ = _load_reference()
    if ref:
        return list(ref.values())
    # Fallback to the static taxonomy if the reference table isn't built yet.
    return [
        {"carc_code": c, "reason_category": cat, "description": REASON_LABELS[cat],
         "group_code": c.split("-")[0], "patient_vs_payer": "payer"}
        for cat, c in CATEGORY_TO_CARC.items()
    ]


# ---------------------------------------------------------------------------
# Denial Intelligence — book-level propensity, drivers, correlations
# ---------------------------------------------------------------------------

def get_propensity_distribution() -> dict:
    """Denial-propensity distribution + reason mix + summary from batch scores."""
    out: dict = {"buckets": [], "reasons": [], "summary": {}}
    scores = f"{_CAT}.{UC_SCHEMA}.gold_denial_risk_scores"
    try:
        out["buckets"] = _execute_sql(f"""
            SELECT CASE
                     WHEN denial_prob < 0.2 THEN '0–20%'
                     WHEN denial_prob < 0.4 THEN '20–40%'
                     WHEN denial_prob < 0.6 THEN '40–60%'
                     WHEN denial_prob < 0.8 THEN '60–80%'
                     ELSE '80–100%'
                   END AS bucket,
                   COUNT(*) AS n
            FROM {scores} GROUP BY 1 ORDER BY 1""")
        # Actual denial-reason mix from historically denied claims (ground truth),
        # mapped to reason categories via carc_reference. This is more meaningful
        # and robust than the model's argmax, and reflects the real denial drivers.
        out["reasons"] = _execute_sql(f"""
            SELECT COALESCE(ref.reason_category, 'other') AS reason, COUNT(*) AS n
            FROM {_CAT}.claims.silver_claims_medical c
            LEFT JOIN {_CAT}.{UC_SCHEMA}.carc_reference ref
                ON c.denial_reason_code = ref.carc_code
            WHERE LOWER(c.claim_status) = 'denied'
            GROUP BY 1 ORDER BY n DESC""")
        s = _execute_sql(f"""
            SELECT COUNT(*) AS total,
                   ROUND(AVG(denial_prob), 4) AS avg_prob,
                   SUM(CASE WHEN denial_prob >= 0.5 THEN 1 ELSE 0 END) AS high_risk
            FROM {scores}""")
        out["summary"] = s[0] if s else {}
    except Exception as e:
        print(f"[Scrub] propensity distribution failed: {e}")
    return out


def get_denial_drivers() -> list[dict]:
    """Global denial drivers (SHAP / feature importance) from the trained model."""
    try:
        return _execute_sql(f"""
            SELECT rank, feature, label, importance, importance_pct, method
            FROM {_CAT}.{UC_SCHEMA}.denial_model_drivers ORDER BY rank""")
    except Exception as e:
        print(f"[Scrub] denial drivers failed: {e}")
        return []


def get_denial_forecast() -> list[dict]:
    """Monthly denial-trend history + 6-month forecast from gold_denial_forecast."""
    try:
        return _execute_sql(f"""
            SELECT CAST(ds AS STRING) AS ds, metric, actual, forecast, lower, upper,
                   is_forecast, method
            FROM {_CAT}.{UC_SCHEMA}.gold_denial_forecast
            ORDER BY metric, ds""")
    except Exception as e:
        print(f"[Scrub] denial forecast failed: {e}")
        return []


def get_denial_correlations(dimension: str = "procedure", limit: int = 12) -> list[dict]:
    """Denial-rate correlation by procedure / diagnosis / provider / line of business."""
    col_map = {
        "procedure": "procedure_code",
        "diagnosis": "primary_diagnosis_code",
        "provider": "rendering_provider_npi",
    }
    try:
        if dimension == "lob":
            return _execute_sql(f"""
                SELECT COALESCE(e.line_of_business, 'Unknown') AS dimension_value,
                       COUNT(*) AS total,
                       SUM(CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END) AS denied,
                       ROUND(AVG(CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1.0 ELSE 0 END), 4) AS denial_rate
                FROM {_CAT}.claims.silver_claims_medical c
                LEFT JOIN (
                    SELECT member_id, MAX(line_of_business) AS line_of_business
                    FROM {_CAT}.members.silver_enrollment GROUP BY member_id
                ) e ON c.member_id = e.member_id
                GROUP BY 1 ORDER BY denial_rate DESC""")
        col = col_map.get(dimension, "procedure_code")
        return _execute_sql(f"""
            SELECT {col} AS dimension_value,
                   COUNT(*) AS total,
                   SUM(CASE WHEN LOWER(claim_status) = 'denied' THEN 1 ELSE 0 END) AS denied,
                   ROUND(AVG(CASE WHEN LOWER(claim_status) = 'denied' THEN 1.0 ELSE 0 END), 4) AS denial_rate
            FROM {_CAT}.claims.silver_claims_medical
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            HAVING COUNT(*) >= 30
            ORDER BY denial_rate DESC, denied DESC
            LIMIT {int(limit)}""")
    except Exception as e:
        print(f"[Scrub] denial correlations ({dimension}) failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Layer 1 — deterministic rules
# ---------------------------------------------------------------------------

def _parse_dos(dos: str) -> date | None:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(dos.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


@mlflow.trace(span_type="TOOL", name="rule_eligibility")
def _rule_eligibility(member_ctx: dict, dos: date | None) -> list[dict]:
    if not member_ctx.get("eligibility_start_date"):
        return [{
            "carc_code": "CO-27", "reason_category": "eligibility", "likelihood": 0.85,
            "evidence": "No active enrollment record found for this member.",
        }]
    if dos is None:
        return []
    start = _parse_dos(member_ctx.get("eligibility_start_date") or "")
    end = _parse_dos(member_ctx.get("eligibility_end_date") or "") if member_ctx.get("eligibility_end_date") else None
    if start and dos < start:
        return [{
            "carc_code": "CO-27", "reason_category": "eligibility", "likelihood": 0.9,
            "evidence": f"Date of service {dos} precedes coverage start {start}.",
        }]
    if end and dos > end:
        return [{
            "carc_code": "CO-27", "reason_category": "eligibility", "likelihood": 0.9,
            "evidence": f"Coverage ended {end}; date of service {dos} is not covered.",
        }]
    return []


@mlflow.trace(span_type="TOOL", name="rule_coding_consistency")
def _rule_coding(policy: dict | None, procedure_codes: list[str]) -> list[dict]:
    if policy and policy["procedure_match"] and not policy["diagnosis_match"]:
        return [{
            "carc_code": "CO-11", "reason_category": "coding_mismatch", "likelihood": 0.75,
            "evidence": (f"Procedure {', '.join(policy['matched_procedure_codes'])} matches policy "
                         f"{policy['policy_id']} ({policy['policy_name']}), but the submitted diagnosis "
                         f"codes are not covered indications."),
        }]
    return []


@mlflow.trace(span_type="TOOL", name="rule_auth_required")
def _rule_auth_required(policy: dict | None, member_id: str, procedure_codes: list[str],
                        request_type: str, auth_reference: str | None) -> list[dict]:
    # Prior-auth requests ARE the auth submission; do not flag them as missing.
    if request_type == "prior_auth" or auth_reference or policy is None:
        return []
    for proc in procedure_codes:
        try:
            approved = _execute_sql(
                f"""SELECT 1 FROM {_CAT}.prior_auth.silver_pa_requests
                    WHERE member_id = :mid AND procedure_code = :pc
                      AND determination = 'approved' LIMIT 1""",
                [{"name": "mid", "value": member_id}, {"name": "pc", "value": proc}],
            )
        except Exception as e:
            print(f"[Scrub] auth lookup failed: {e}")
            approved = []
        if not approved:
            return [{
                "carc_code": "CO-197", "reason_category": "no_auth", "likelihood": 0.85,
                "evidence": (f"Procedure {proc} is auth-managed under policy "
                             f"{policy['policy_id']} ({policy['policy_name']}), but no approved "
                             f"prior authorization is on file for this member."),
            }]
    return []


@mlflow.trace(span_type="TOOL", name="rule_frequency_limits")
def _rule_frequency(member_id: str, procedure_codes: list[str], dos: date | None,
                    requested_units: int) -> list[dict]:
    try:
        cap_rows = _execute_sql(
            f"""SELECT MIN(visit_limit) AS cap FROM {_CAT}.benefits.silver_benefits
                WHERE member_id = :mid AND visit_limit IS NOT NULL AND visit_limit > 0""",
            [{"name": "mid", "value": member_id}],
        )
    except Exception as e:
        print(f"[Scrub] benefit limit lookup failed: {e}")
        return []
    cap = cap_rows[0].get("cap") if cap_rows else None
    if not cap:
        return []
    cap = int(cap)
    for proc in procedure_codes:
        try:
            cnt_rows = _execute_sql(
                f"""SELECT COUNT(*) AS n FROM {_CAT}.claims.silver_claims_medical
                    WHERE member_id = :mid AND procedure_code = :pc
                      AND LOWER(claim_status) = 'paid'
                      AND service_from_date >= add_months(CAST(:dos AS DATE), -12)
                      AND service_from_date <= CAST(:dos AS DATE)""",
                [{"name": "mid", "value": member_id}, {"name": "pc", "value": proc},
                 {"name": "dos", "value": (dos or date.today()).isoformat()}],
            )
        except Exception as e:
            print(f"[Scrub] frequency lookup failed: {e}")
            continue
        prior = int(cnt_rows[0].get("n") or 0) if cnt_rows else 0
        if prior + requested_units > cap:
            return [{
                "carc_code": "CO-151", "reason_category": "frequency_limit", "likelihood": 0.65,
                "evidence": (f"{prior} prior paid claim(s) for {proc} in the last 12 months + "
                             f"{requested_units} requested exceeds the plan visit limit of {cap}."),
            }]
    return []


@mlflow.trace(span_type="TOOL", name="rule_completeness")
def _rule_completeness(draft: dict, procedure_codes: list[str], diagnosis_codes: list[str]) -> list[dict]:
    problems: list[str] = []
    npi = str(draft.get("provider_npi") or "")
    if not re.fullmatch(r"\d{10}", npi):
        problems.append("provider NPI is not a valid 10-digit identifier")
    if not diagnosis_codes:
        problems.append("no diagnosis code submitted")
    invalid_cpt = [c for c in procedure_codes if not re.fullmatch(r"[A-Z]?\d{4,5}", c)]
    if invalid_cpt:
        problems.append(f"invalid procedure code(s): {', '.join(invalid_cpt)}")
    if not procedure_codes:
        problems.append("no procedure code submitted")
    if draft.get("request_type") == "prior_auth" and not _has_value(draft.get("clinical_notes")):
        problems.append("prior-auth request has no clinical documentation attached")
    if problems:
        return [{
            "carc_code": "CO-16", "reason_category": "missing_info", "likelihood": 0.7,
            "evidence": "Claim lacks required information: " + "; ".join(problems) + ".",
        }]
    return []


# ---------------------------------------------------------------------------
# Layer 2 — ML denial scorer
# ---------------------------------------------------------------------------

def _claim_type_from_pos(pos: str | None) -> str:
    return {"21": "inpatient", "22": "outpatient", "23": "emergency"}.get(str(pos or ""), "professional")


@mlflow.trace(span_type="TOOL", name="ml_denial_scorer")
def _score_ml(draft: dict, procedure_codes: list[str], diagnosis_codes: list[str],
              member_ctx: dict) -> dict:
    """Query the denial-risk-scorer endpoint. Returns {denial_prob, reason_probs}."""
    pos = draft["lines"][0].get("pos") if draft.get("lines") else None
    billed = float(draft.get("billed_amount") or 0.0)
    record = {
        "procedure_code": procedure_codes[0] if procedure_codes else "",
        "primary_diagnosis_code": diagnosis_codes[0] if diagnosis_codes else "",
        "claim_type": _claim_type_from_pos(pos),
        "place_of_service_code": str(pos or "11"),
        "billed_amount": billed,
        "allowed_amount": billed,  # pre-submission proxy (no adjudicated allowed yet)
        "line_of_business": draft.get("line_of_business") or member_ctx.get("line_of_business") or "Commercial",
        "rendering_provider_npi": str(draft.get("provider_npi") or ""),
    }
    try:
        w = WorkspaceClient()
        resp = w.serving_endpoints.query(name=DENIAL_ENDPOINT, dataframe_records=[record])
        preds = resp.predictions if hasattr(resp, "predictions") else resp
        pred = preds[0] if isinstance(preds, list) and preds else preds
        if isinstance(pred, str):
            pred = json.loads(pred)
        return {
            "denial_prob": float(pred.get("denial_prob", 0.0)),
            "reason_probs": pred.get("reason_probs", {}) or {},
            "feature_contributions": pred.get("feature_contributions", []) or [],
        }
    except Exception as e:
        print(f"[Scrub] ML scorer error: {e}")
        return {"denial_prob": 0.0, "reason_probs": {}, "feature_contributions": []}


# ---------------------------------------------------------------------------
# Orchestration + composition
# ---------------------------------------------------------------------------

def _enrich_card(finding: dict, layer: str) -> dict:
    ref, rem = _load_reference()
    carc = finding["carc_code"]
    category = finding.get("reason_category") or CARC_TO_CATEGORY.get(carc, "other")
    ref_row = ref.get(carc, {})
    rem_row = rem.get(carc, {})
    return {
        "carc_code": carc,
        "reason_category": category,
        "reason_label": ref_row.get("description") or REASON_LABELS.get(category, carc),
        "likelihood": round(float(finding.get("likelihood", 0.5)), 3),
        "layer": layer,
        "evidence": finding.get("evidence"),
        "remediation": rem_row.get("remediation_text"),
        "required_action": rem_row.get("required_action"),
        "doc_needed": rem_row.get("doc_needed"),
    }


def _compose(cards: list[dict], ml_denial_prob: float) -> tuple[int, str]:
    """Blend finding likelihoods + ML probability into a 0-100 risk score."""
    surviving = 1.0
    for c in cards:
        surviving *= (1.0 - float(c["likelihood"]))
    combined = 1.0 - surviving
    risk = max(combined, ml_denial_prob)
    score = int(round(risk * 100))
    if score >= 70:
        decision = "likely_denied"
    elif score >= 35:
        decision = "at_risk"
    else:
        decision = "clean"
    return score, decision


def _assemble(draft: dict, member_ctx: dict, rule_findings: list[dict],
              rag_findings: list[dict], ml_result: dict, request_type: str) -> dict:
    """Merge findings from all layers into a composed, de-duplicated result."""
    ml_denial_prob = float(ml_result.get("denial_prob", 0.0))
    reason_probs = ml_result.get("reason_probs", {})
    contributions = ml_result.get("feature_contributions", []) or []
    # One-line "why THIS claim" summary from the top local SHAP drivers.
    contrib_hint = ""
    if contributions:
        contrib_hint = " Top drivers for this claim: " + ", ".join(
            f"{c.get('label', c.get('feature'))} ({'+' if c.get('contribution', 0) >= 0 else ''}{c.get('contribution')})"
            for c in contributions
        ) + "."

    # Keep the highest layer-priority / likelihood card per reason category.
    candidates: dict[str, dict] = {}

    def _consider(finding: dict, layer: str):
        category = finding.get("reason_category") or CARC_TO_CATEGORY.get(finding["carc_code"], "other")
        card = _enrich_card(finding, layer)
        existing = candidates.get(category)
        if existing is None:
            candidates[category] = card
            return
        better = (_LAYER_PRIORITY[layer] > _LAYER_PRIORITY[existing["layer"]]
                  or (_LAYER_PRIORITY[layer] == _LAYER_PRIORITY[existing["layer"]]
                      and card["likelihood"] > existing["likelihood"]))
        if better:
            candidates[category] = card

    for f in rule_findings:
        _consider(f, "rule")
    for f in rag_findings:
        _consider(f, "rag")
    for category, prob in reason_probs.items():
        if category == "other" or float(prob) < _ML_REASON_THRESHOLD:
            continue
        _consider({
            "carc_code": CATEGORY_TO_CARC.get(category, "CO-16"),
            "reason_category": category,
            "likelihood": float(prob),
            "evidence": (f"Denial-reason model estimates {round(float(prob) * 100)}% likelihood for this reason "
                         f"based on comparable historical claims.{contrib_hint}"),
        }, "ml")

    cards = sorted(candidates.values(), key=lambda c: c["likelihood"], reverse=True)
    risk_score, decision = _compose(cards, ml_denial_prob)

    return {
        "member_id": draft["member_id"],
        "member_name": member_ctx.get("member_name"),
        "provider_npi": draft.get("provider_npi"),
        "date_of_service": draft.get("date_of_service"),
        "request_type": request_type,
        "risk_score": risk_score,
        "decision": decision,
        "ml_denial_prob": round(ml_denial_prob, 3),
        "ml_contributions": contributions,
        "reason_cards": cards,
        "resubmitted_from": draft.get("resubmitted_from"),
    }


def _current_trace_id() -> str | None:
    """Trace id of the active MLflow trace, for linking user feedback to it."""
    try:
        span = mlflow.get_current_active_span()
        return span.trace_id if span else None
    except Exception:
        return None


def _run_rules(draft: dict, member_ctx: dict, policy: dict | None,
               procedure_codes: list[str], diagnosis_codes: list[str],
               dos: date | None, requested_units: int, request_type: str) -> list[dict]:
    findings: list[dict] = []
    findings += _rule_eligibility(member_ctx, dos)
    findings += _rule_coding(policy, procedure_codes)
    findings += _rule_auth_required(policy, draft["member_id"], procedure_codes,
                                    request_type, draft.get("auth_reference"))
    findings += _rule_frequency(draft["member_id"], procedure_codes, dos, requested_units)
    findings += _rule_completeness(draft, procedure_codes, diagnosis_codes)
    return findings


@mlflow.trace(span_type="CHAIN", name="scrub_engine_run")
def run_scrub(draft: dict) -> dict:
    """Run all three layers and return a composed scrub result (dict).

    `draft` matches DraftClaimIn: member_id, provider_npi, date_of_service,
    request_type, lines[], dx_codes[], clinical_notes, billed_amount,
    line_of_business, auth_reference.
    """
    procedure_codes = _split_codes([ln.get("cpt") for ln in draft.get("lines", [])])
    diagnosis_codes = _split_codes(draft.get("dx_codes", []))
    dos = _parse_dos(draft.get("date_of_service") or "")
    requested_units = sum(int(ln.get("units") or 1) for ln in draft.get("lines", [])) or 1
    request_type = draft.get("request_type") or "claim"

    member_ctx = get_member_context(draft["member_id"])
    policy = _match_policy(procedure_codes, diagnosis_codes) if procedure_codes else None

    rule_findings = _run_rules(draft, member_ctx, policy, procedure_codes,
                               diagnosis_codes, dos, requested_units, request_type)

    with ThreadPoolExecutor(max_workers=2) as pool:
        ml_future = pool.submit(_score_ml, draft, procedure_codes, diagnosis_codes, member_ctx)
        rag_future = pool.submit(assess_policy_rag, procedure_codes, diagnosis_codes,
                                 draft.get("clinical_notes") or "")
        ml_result = ml_future.result()
        rag_findings = rag_future.result()

    result = _assemble(draft, member_ctx, rule_findings, rag_findings, ml_result, request_type)
    result["trace_id"] = _current_trace_id()
    return result


@mlflow.trace(span_type="CHAIN", name="scrub_engine_run_stream")
def run_scrub_stream(draft: dict):
    """Streaming variant — yields (event_type, payload) tuples.

    Milestones:
        status  -> {"stage": "...", "message": "..."}
        result  -> full scrub-result dict
        error   -> {"message": "..."}
    """
    try:
        procedure_codes = _split_codes([ln.get("cpt") for ln in draft.get("lines", [])])
        diagnosis_codes = _split_codes(draft.get("dx_codes", []))
        dos = _parse_dos(draft.get("date_of_service") or "")
        requested_units = sum(int(ln.get("units") or 1) for ln in draft.get("lines", [])) or 1
        request_type = draft.get("request_type") or "claim"

        yield ("status", {"stage": "eligibility", "message": "Checking member eligibility on the date of service…"})
        member_ctx = get_member_context(draft["member_id"])
        policy = _match_policy(procedure_codes, diagnosis_codes) if procedure_codes else None

        yield ("status", {"stage": "rules", "message": "Running coding, authorization, and benefit-limit rules…"})
        rule_findings = _run_rules(draft, member_ctx, policy, procedure_codes,
                                   diagnosis_codes, dos, requested_units, request_type)

        yield ("status", {"stage": "ml", "message": "Scoring against the denial-prediction model…"})
        ml_result = _score_ml(draft, procedure_codes, diagnosis_codes, member_ctx)

        yield ("status", {"stage": "rag", "message": "Checking medical-necessity & experimental status against policy…"})
        rag_findings = assess_policy_rag(procedure_codes, diagnosis_codes, draft.get("clinical_notes") or "")

        yield ("status", {"stage": "composing", "message": "Composing denial-risk score and remediation…"})
        result = _assemble(draft, member_ctx, rule_findings, rag_findings, ml_result, request_type)
        result["trace_id"] = _current_trace_id()
        yield ("result", result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        yield ("error", {"message": str(e)})

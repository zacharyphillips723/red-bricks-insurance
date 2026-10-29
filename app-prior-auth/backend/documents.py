"""Document intake, parsing, extraction, and auto-adjudication.

Pipeline:
  1. upload_document()        — store an uploaded medical record in a UC Volume
  2. parse_document()         — ai_parse_document(blob) -> plain text (OCR)
  3. extract_clinical_facts() — ai_extract(text, [...]) -> structured facts
  4. adjudicate()             — apply Tier-1 deterministic rules to the facts
                                against silver_medical_policy_rules

The adjudication reuses the SAME deterministic logic as the Tier-1 gold view,
but with EXACT code matching (split on '|') instead of LIKE '%code%' substring
matching, which produces false positives (e.g. 99213 matching 992130).

All SQL runs on the SQL warehouse via the Statement Execution API so the demo
showcases Databricks-native AI functions (ai_parse_document / ai_extract).
"""

import json
import re
import uuid
from typing import Any

import mlflow
from databricks.sdk import WorkspaceClient

from .env_config import UC_CATALOG, UC_SCHEMA, SQL_WAREHOUSE_ID, PA_DOC_VOLUME_PATH
from .agent import _execute_sql  # reuse the shared Statement Execution helper

_CAT = f"`{UC_CATALOG}`"
_RULES_TABLE = f"{_CAT}.{UC_SCHEMA}.silver_medical_policy_rules"

# Fields ai_extract pulls from the parsed medical record.
EXTRACT_FIELDS = [
    "member_name",
    "member_id",
    "requesting_provider",
    "provider_npi",
    "procedure_codes",
    "diagnosis_codes",
    "diagnoses",
    "lab_values",
    "treatments_tried",
    "functional_status",
    "clinical_summary",
]


# ---------------------------------------------------------------------------
# 1. Upload
# ---------------------------------------------------------------------------

def upload_document(file_bytes: bytes, filename: str) -> dict:
    """Store an uploaded file in the pa_documents UC Volume.

    Returns a handle: {document_id, filename, volume_path}.
    """
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", filename or "upload.pdf")
    document_id = uuid.uuid4().hex[:12]
    object_name = f"{document_id}_{safe_name}"
    volume_path = f"{PA_DOC_VOLUME_PATH}/{object_name}"

    w = WorkspaceClient()
    import io
    w.files.upload(volume_path, io.BytesIO(file_bytes), overwrite=True)

    return {
        "document_id": document_id,
        "filename": safe_name,
        "volume_path": volume_path,
    }


# ---------------------------------------------------------------------------
# 2. Parse — ai_parse_document
# ---------------------------------------------------------------------------

@mlflow.trace(span_type="TOOL", name="ai_parse_document")
def parse_document(volume_path: str) -> str:
    """Parse an uploaded document to plain text via ai_parse_document.

    ai_parse_document returns a VARIANT with the parsed structure; we flatten
    the recognized text elements into a single string for extraction + display.
    """
    esc = volume_path.replace("'", "''")
    # ai_parse_document returns a VARIANT with document.elements[] where each
    # element has a `content` string. Flatten those into one text body directly
    # in SQL (verified against the ai_parse_document output shape).
    rows = _execute_sql(
        f"""
        WITH parsed AS (
            SELECT ai_parse_document(content) AS doc
            FROM READ_FILES('{esc}', format => 'binaryFile')
        )
        SELECT array_join(
                 transform(
                   cast(doc:document:elements AS ARRAY<STRING>),
                   x -> parse_json(x):content::string
                 ),
                 '\n'
               ) AS body
        FROM parsed
        """
    )
    if not rows:
        return ""
    return (rows[0].get("body") or "").strip()


# ---------------------------------------------------------------------------
# 3. Extract — ai_extract
# ---------------------------------------------------------------------------

# CPT/HCPCS procedure codes: 5 digits, or a letter + 4 digits (e.g. 95249, E0784, J3490).
_PROC_CODE_RE = re.compile(r"\b([A-Z]?\d{4,5})\b")
# ICD-10 diagnosis codes: letter + 2 digits, optional dot + more (e.g. E11.65, M17.11).
_ICD10_RE = re.compile(r"\b([A-TV-Z]\d{2}(?:\.\d{1,4})?)\b")


def _known_procedure_codes() -> set[str]:
    """The universe of procedure codes referenced by any medical policy rule.

    Used to filter regex fallback candidates so free-text numbers like years
    ('2025') or dosages ('1000mg' -> '1000') can't be mistaken for CPT codes.
    """
    try:
        rows = _execute_sql(
            f"SELECT procedure_codes FROM {_RULES_TABLE} "
            "WHERE rule_type IN ('clinical_criteria', 'coverage_criteria')"
        )
    except Exception:
        return set()
    codes: set[str] = set()
    for r in rows:
        codes.update(_split_codes(r.get("procedure_codes")))
    return codes


def _regex_codes_from_text(document_text: str) -> tuple[list[str], list[str]]:
    """Deterministic fallback: pull procedure/diagnosis codes straight from text.

    ai_extract occasionally omits a field on a multi-field call. Codes follow
    strict formats, so a regex over the parsed text is a reliable backstop.
    Procedure candidates are validated against the known policy code universe so
    incidental numbers (years, dosages) are not treated as CPT/HCPCS codes.
    """
    def _dedupe(seq):
        seen, out = set(), []
        for x in seq:
            if x not in seen:
                seen.add(x); out.append(x)
        return out

    icd = _dedupe(m.upper() for m in _ICD10_RE.findall(document_text))
    icd_set = set(icd)
    known = _known_procedure_codes()
    proc_candidates = [c.upper() for c in _PROC_CODE_RE.findall(document_text) if c.upper() not in icd_set]
    # Keep only candidates that are real policy procedure codes when we have the
    # universe; if the lookup failed, fall back to all candidates.
    proc = [c for c in proc_candidates if c in known] if known else proc_candidates
    return _dedupe(proc), icd


@mlflow.trace(span_type="TOOL", name="ai_extract_clinical_facts")
def extract_clinical_facts(document_text: str) -> dict:
    """Extract structured clinical facts from parsed text via ai_extract.

    Backfills procedure/diagnosis codes with a regex over the source text when
    ai_extract omits them, so the deterministic adjudication stays reliable.
    """
    if not document_text.strip():
        return {}

    text_esc = document_text.replace("'", "''")
    fields_sql = ", ".join(f"'{f}'" for f in EXTRACT_FIELDS)

    def _run_extract() -> dict:
        rows = _execute_sql(
            f"SELECT ai_extract('{text_esc}', array({fields_sql})) AS facts"
        )
        if not rows:
            return {}
        raw = rows[0].get("facts")
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"_raw": raw}
        return raw or {}

    # ai_extract is unreliable at returning all 11 fields on one call — it often
    # returns only the codes and drops the narrative fields. Retry once and keep
    # the more complete result.
    def _completeness(f: dict) -> int:
        return (
            (1 if _split_codes(f.get("procedure_codes")) else 0)
            + (1 if _split_codes(f.get("diagnosis_codes")) else 0)
            + (1 if (f.get("clinical_summary") or "").strip() else 0)
        )

    facts = _run_extract()
    if _completeness(facts) < 3:
        retry = _run_extract()
        if _completeness(retry) > _completeness(facts):
            facts = retry

    # If the multi-field call still dropped the clinical summary, recover it with
    # a dedicated single-field extraction — far more reliable than 1-of-11.
    if not (facts.get("clinical_summary") or "").strip():
        try:
            s_rows = _execute_sql(
                f"SELECT ai_extract('{text_esc}', array('clinical_summary')) AS s"
            )
            if s_rows:
                raw = s_rows[0].get("s")
                parsed = json.loads(raw) if isinstance(raw, str) else (raw or {})
                summary = (parsed.get("clinical_summary") or "").strip()
                if summary:
                    facts["clinical_summary"] = summary
                    facts["_clinical_summary_source"] = "single_field_extract"
        except Exception as e:
            print(f"[PA docs] clinical_summary re-extract failed: {e}")

    # Backstop: if ai_extract still missed the codes, recover them from the text.
    if not _split_codes(facts.get("procedure_codes")) or not _split_codes(facts.get("diagnosis_codes")):
        rx_proc, rx_dx = _regex_codes_from_text(document_text)
        if not _split_codes(facts.get("procedure_codes")) and rx_proc:
            facts["procedure_codes"] = rx_proc
            facts["_procedure_codes_source"] = "regex_fallback"
        if not _split_codes(facts.get("diagnosis_codes")) and rx_dx:
            facts["diagnosis_codes"] = rx_dx
            facts["_diagnosis_codes_source"] = "regex_fallback"

    return facts


# ---------------------------------------------------------------------------
# 4. Adjudicate — Tier-1 deterministic rules (EXACT code matching)
# ---------------------------------------------------------------------------

def _has_value(value: Any) -> bool:
    """True if an ai_extract field holds real content (not null/empty/'[]'/'{}')."""
    if value is None:
        return False
    if isinstance(value, (list, dict)):
        return len(value) > 0
    s = str(value).strip()
    return s not in ("", "null", "None", "[]", "{}", '""')


def _split_codes(value: Any) -> list[str]:
    """Normalize a code field to a list of upper-cased codes.

    Handles: a Python list, a pipe/comma/space-delimited string, or a JSON-array
    string (ai_extract returns array fields as JSON strings, e.g. '["95249"]').
    """
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
    """Find a medical policy whose covered codes exactly match the request.

    Returns the best-matching policy rule with per-rule match flags, or None
    if no policy references any of the submitted procedure codes.
    """
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
            continue  # policy doesn't cover any submitted procedure

        candidate = {
            "policy_id": r.get("policy_id"),
            "policy_name": r.get("policy_name"),
            "service_category": r.get("service_category"),
            "procedure_match": bool(proc_hits),
            "diagnosis_match": bool(dx_hits),
            "matched_procedure_codes": proc_hits,
            "matched_diagnosis_codes": dx_hits,
        }
        # Prefer a policy where BOTH procedure and diagnosis align.
        if best is None or (candidate["diagnosis_match"] and not best["diagnosis_match"]):
            best = candidate
        if candidate["diagnosis_match"]:
            break
    return best


@mlflow.trace(span_type="TOOL", name="tier1_adjudicate")
def adjudicate(facts: dict, document_text: str = "") -> dict:
    """Apply Tier-1 deterministic rules to extracted facts.

    Decision:
      - Auto-Approve         : procedure + diagnosis match a policy AND documented
      - Needs Clinical Review: partial match, or missing documentation
      - Auto-Deny            : no policy covers the submitted procedure
    """
    procedure_codes = _split_codes(facts.get("procedure_codes"))
    diagnosis_codes = _split_codes(facts.get("diagnosis_codes"))
    clinical_summary = (facts.get("clinical_summary") or "").strip()
    # Documentation is sufficient if there's a real clinical narrative OR the
    # extraction surfaced concrete clinical evidence (treatments tried, labs,
    # functional status). This stays robust when ai_extract drops the summary
    # field, without passing a sparse record (e.g. "Knee pain.") that has none
    # of these signals.
    has_clinical_evidence = any(
        _has_value(facts.get(k)) for k in ("treatments_tried", "lab_values", "functional_status")
    )
    has_documentation = len(clinical_summary) > 50 or has_clinical_evidence

    reasons: list[str] = []
    policy = _match_policy(procedure_codes, diagnosis_codes) if procedure_codes else None

    if not procedure_codes:
        decision = "Needs Clinical Review"
        confidence = 0.30
        reasons.append("No procedure code could be extracted from the document.")
    elif policy is None:
        decision = "Auto-Deny"
        confidence = 0.80
        reasons.append(
            f"No medical policy covers the submitted procedure code(s) "
            f"{', '.join(procedure_codes)}."
        )
    elif policy["procedure_match"] and policy["diagnosis_match"] and has_documentation:
        decision = "Auto-Approve"
        confidence = 0.94
        reasons.append(
            f"Procedure {', '.join(policy['matched_procedure_codes'])} and diagnosis "
            f"{', '.join(policy['matched_diagnosis_codes'])} both satisfy policy "
            f"{policy['policy_id']} ({policy['policy_name']})."
        )
        reasons.append("Clinical documentation is present and sufficient.")
    else:
        decision = "Needs Clinical Review"
        confidence = 0.55
        if not policy["diagnosis_match"]:
            reasons.append(
                f"Procedure matches policy {policy['policy_id']} but the submitted "
                f"diagnosis codes do not align with covered indications."
            )
        if not has_documentation:
            reasons.append("Clinical documentation is missing or insufficient (<50 chars).")

    return {
        "decision": decision,
        "confidence": confidence,
        "reasons": reasons,
        "matched_policy": policy,
        "extracted_procedure_codes": procedure_codes,
        "extracted_diagnosis_codes": diagnosis_codes,
        "has_documentation": has_documentation,
    }


# ---------------------------------------------------------------------------
# 5. Write-back to the Lakebase review queue
# ---------------------------------------------------------------------------

# Map an adjudication decision to (queue status, determination tier).
_DECISION_TO_STATUS = {
    "Auto-Approve": ("Approved", "tier_1_auto"),
    "Auto-Deny": ("Denied", "tier_1_auto"),
    "Needs Clinical Review": ("Pending Review", "manual"),
}

# CMS turnaround limits (hours) used to compute the deadline.
_URGENCY_DEADLINE_HOURS = {"expedited": 72, "standard": 168, "retrospective": 336}


async def write_back_to_queue(session, facts: dict, result: dict, handle: dict) -> str:
    """Insert an auto-adjudicated upload as a pa_review_queue row + audit action.

    Uses the provided async SQLAlchemy session. Returns the new auth_request_id.
    """
    from sqlalchemy import text

    status, tier = _DECISION_TO_STATUS.get(result["decision"], ("Pending Review", "manual"))
    is_determined = status in ("Approved", "Denied")
    policy = result.get("matched_policy") or {}
    auth_request_id = f"UPL-{handle['document_id'].upper()}"

    proc_codes = result.get("extracted_procedure_codes") or []
    dx_codes = result.get("extracted_diagnosis_codes") or []
    reason_text = " ".join(result.get("reasons", []))

    # Uploaded requests default to standard urgency; compute the CMS deadline
    # from the urgency SLA rather than hardcoding it.
    urgency = "standard"
    deadline_hours = _URGENCY_DEADLINE_HOURS.get(urgency, 168)
    # Human-readable procedure description (codes + matched policy), not the raw
    # JSON-array string ai_extract returns.
    proc_desc = ", ".join(proc_codes) if proc_codes else "Uploaded document"
    if policy.get("policy_name"):
        proc_desc = f"{proc_desc} — {policy['policy_name']}"

    await session.execute(
        text("""
            INSERT INTO pa_review_queue (
                auth_request_id, member_id, member_name,
                requesting_provider_npi, provider_name,
                service_type, procedure_code, procedure_description,
                diagnosis_codes, policy_id, policy_name, line_of_business,
                clinical_summary, urgency, status, determination_tier,
                ai_recommendation, ai_confidence, tier1_auto_eligible,
                clinical_extraction, determination_reason,
                request_date, determination_date, cms_deadline, cms_compliant
            ) VALUES (
                :aid, :member_id, :member_name,
                :npi, :provider_name,
                :service_type, :proc_code, :proc_desc,
                :dx, :policy_id, :policy_name, :lob,
                :clinical, CAST(:urgency AS pa_urgency), CAST(:status AS pa_review_status),
                CAST(:tier AS pa_determination_tier),
                :ai_rec, :ai_conf, :auto_eligible,
                :extraction, :det_reason,
                now(),
                CASE WHEN :is_determined THEN now() ELSE NULL END,
                now() + make_interval(hours => :deadline_hours), TRUE
            )
            ON CONFLICT (auth_request_id) DO NOTHING
        """),
        {
            "aid": auth_request_id,
            "member_id": facts.get("member_id") or "UNKNOWN",
            "member_name": facts.get("member_name"),
            "npi": facts.get("provider_npi") or "UNKNOWN",
            "provider_name": facts.get("requesting_provider"),
            "service_type": policy.get("service_category") or "uploaded_document",
            "proc_code": proc_codes[0] if proc_codes else "UNKNOWN",
            "proc_desc": proc_desc,
            "urgency": urgency,
            "deadline_hours": deadline_hours,
            "dx": "|".join(dx_codes) if dx_codes else None,
            "policy_id": policy.get("policy_id"),
            "policy_name": policy.get("policy_name"),
            "lob": "Commercial",
            "clinical": facts.get("clinical_summary"),
            "status": status,
            "tier": tier,
            "ai_rec": f"{result['decision']}: {reason_text}"[:2000],
            "ai_conf": result.get("confidence"),
            "auto_eligible": result["decision"] in ("Auto-Approve", "Auto-Deny"),
            "extraction": json.dumps(facts, default=str),
            "det_reason": reason_text if is_determined else None,
            "is_determined": is_determined,
        },
    )

    await session.execute(
        text("""
            INSERT INTO pa_review_actions
                (auth_request_id, action_type, new_status, note, metadata_json)
            VALUES (:aid, 'auto_generated', CAST(:status AS pa_review_status),
                    :note, CAST(:meta AS jsonb))
        """),
        {
            "aid": auth_request_id,
            "status": status,
            "note": f"Auto-adjudicated from uploaded document '{handle['filename']}': "
                    f"{result['decision']} (confidence {result.get('confidence')}).",
            "meta": json.dumps({
                "source": "document_upload",
                "document_id": handle["document_id"],
                "volume_path": handle["volume_path"],
                "decision": result["decision"],
                "matched_policy": policy.get("policy_id"),
            }, default=str),
        },
    )
    await session.commit()
    return auth_request_id

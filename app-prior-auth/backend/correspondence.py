"""Determination notice generation (Decision Processing + Correspondence Management).

RFI mapping — Decision Processing & Correspondence Management tabs:
  - Regulatory-compliant approval / denial / partial-approval notices
  - Required elements: decision, clinical rationale, criteria citation + version,
    denial reason code, effective dates, and member/provider APPEAL RIGHTS
  - AI-assisted drafting (ai_query) of the case-specific rationale paragraph,
    wrapped in a deterministic regulatory template so required language is never
    dropped or hallucinated
  - PHI-redaction gate BEFORE release (SSN / MRN / phone / email masking)
  - Rendered to PDF in the pa_documents UC Volume with delivery tracking

DB access stays in the router (async Lakebase). This module is pure sync AI +
formatting + rendering, invoked via asyncio.to_thread.
"""

import io
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from .env_config import LLM_ENDPOINT, PA_DOC_VOLUME_PATH
from .agent import _execute_sql

# Bump when the regulatory scaffolding changes — persisted on each notice so a
# determination can always be reconstructed against the template it was issued
# under (RFI: effective-dated templates + case reconstruction).
TEMPLATE_VERSION = "2026.08-v1"

# Standard CMS/ERISA appeal-rights language appended to adverse determinations.
_APPEAL_RIGHTS = (
    "**Your Appeal Rights**\n\n"
    "You have the right to appeal this determination. A standard appeal will be "
    "resolved within 30 calendar days of receipt; an expedited appeal (when a delay "
    "could jeopardize health) within 72 hours. To file an appeal, contact Member "
    "Services or submit a request through the provider portal within 60 calendar "
    "days of this notice. You may submit additional clinical documentation in "
    "support of your appeal. You also have the right to request the specific criteria "
    "used in this determination free of charge."
)

# Notice scaffolds: which required elements each notice type must carry.
_NOTICE_SPEC: dict[str, dict[str, Any]] = {
    "approval": {
        "title": "Prior Authorization — APPROVAL",
        "adverse": False,
        "appeal_rights": False,
    },
    "denial": {
        "title": "Prior Authorization — ADVERSE DETERMINATION (DENIAL)",
        "adverse": True,
        "appeal_rights": True,
    },
    "partial_approval": {
        "title": "Prior Authorization — PARTIAL APPROVAL",
        "adverse": True,
        "appeal_rights": True,
    },
    "additional_info_request": {
        "title": "Request for Additional Information",
        "adverse": False,
        "appeal_rights": False,
    },
    "appeal_acknowledgement": {
        "title": "Appeal Received — Acknowledgement",
        "adverse": False,
        "appeal_rights": False,
    },
    "appeal_determination": {
        "title": "Appeal Determination Notice",
        "adverse": True,
        "appeal_rights": True,
    },
}

# PHI patterns the redaction gate masks before a notice can be released.
_PHI_PATTERNS = [
    ("SSN", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("SSN", re.compile(r"\b\d{9}\b")),
    ("MRN", re.compile(r"\bMRN[:#]?\s*\w+\b", re.IGNORECASE)),
    ("phone", re.compile(r"\b\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b")),
    ("email", re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")),
]


def _sql_str(value: str) -> str:
    return (value or "").replace("'", "''")


def redact_phi(text: str) -> tuple[str, bool, list[str]]:
    """Mask PHI/PII patterns. Returns (clean_text, was_redacted, categories).

    Member ID and clinical facts are legitimately present on a determination
    notice; this gate targets identifiers that must NOT leak into correspondence
    (SSN, MRN, phone, email) — the RFI's "PII/PHI validation prior to release."
    """
    found: list[str] = []
    clean = text or ""
    for label, pat in _PHI_PATTERNS:
        if pat.search(clean):
            found.append(label)
            clean = pat.sub("[REDACTED]", clean)
    return clean, bool(found), sorted(set(found))


def _draft_rationale(notice_type: str, facts: dict) -> str:
    """AI-draft the case-specific clinical rationale paragraph via ai_query.

    Only the rationale narrative is model-generated; all regulatory scaffolding
    (decision statement, criteria citation, appeal rights) is deterministic.
    Falls back to a templated sentence if the model call fails.
    """
    spec = _NOTICE_SPEC.get(notice_type, {})
    decision_word = {
        "approval": "approved",
        "denial": "denied",
        "partial_approval": "partially approved",
        "appeal_determination": facts.get("determination", "determined"),
    }.get(notice_type, "processed")

    prompt = (
        "You are drafting the clinical rationale paragraph of a health-plan prior "
        "authorization determination notice. Write 2-3 professional, plain-language "
        "sentences a member and provider can understand. State that the request for "
        f"{facts.get('procedure_description') or facts.get('procedure_code') or 'the requested service'} "
        f"was {decision_word}, and summarize the clinical basis. Do NOT invent facts, "
        "do NOT include SSNs, phone numbers, or email addresses, and do NOT restate "
        "appeal rights (added separately). "
        f"Determination reason: {facts.get('determination_reason') or 'per medical policy criteria'}. "
        f"Clinical summary: {facts.get('clinical_summary') or 'not provided'}."
    )
    try:
        rows = _execute_sql(
            f"SELECT ai_query('{LLM_ENDPOINT}', '{_sql_str(prompt)}') AS body"
        )
        if rows and rows[0].get("body"):
            return str(rows[0]["body"]).strip()
    except Exception as e:  # pragma: no cover - defensive
        print(f"[correspondence] ai_query rationale draft failed: {e}")
    # Deterministic fallback keeps the notice issuable if the model is unavailable.
    return (
        f"The request for {facts.get('procedure_description') or facts.get('procedure_code')} "
        f"was {decision_word}. {facts.get('determination_reason') or 'Determination made per applicable medical policy criteria.'}"
    )


# Supported correspondence languages (RFI: multilingual generation). English is
# native; others are produced by an ai_query translation pass over the assembled
# regulatory body, so required language is authored once and translated verbatim.
SUPPORTED_LANGUAGES = {
    "en": "English", "es": "Spanish", "zh": "Chinese (Simplified)",
    "vi": "Vietnamese", "tl": "Tagalog", "ru": "Russian",
}


def _translate_body(body_markdown: str, language: str) -> str:
    """Translate an assembled notice body into a target language via ai_query.

    Falls back to the English body (with a bilingual header note) if the model
    call fails, so a notice is always issuable.
    """
    lang_name = SUPPORTED_LANGUAGES.get(language, language)
    prompt = (
        f"Translate the following health-plan prior authorization notice into {lang_name}. "
        "Preserve all markdown structure, headings, dates, codes, identifiers, and the "
        "meaning of the appeal-rights language exactly. Translate only the natural-language "
        "text; do not add commentary. Notice:\n\n" + body_markdown
    )
    try:
        rows = _execute_sql(
            f"SELECT ai_query('{LLM_ENDPOINT}', '{_sql_str(prompt)}') AS body"
        )
        if rows and rows[0].get("body"):
            return str(rows[0]["body"]).strip()
    except Exception as e:  # pragma: no cover - defensive
        print(f"[correspondence] ai_query translation failed: {e}")
    return f"> _[Translation to {lang_name} unavailable — English text follows]_\n\n{body_markdown}"


def validate_delivery(facts: dict) -> tuple[str, str]:
    """Beneficiary / address validation before release (RFI: pre-release validation,
    prevention of misdirected correspondence). Returns (status, notes).

    Heuristic completeness checks a real integration would run against a
    beneficiary/address master; here it demonstrates the pre-release gate.
    """
    issues: list[str] = []
    if not (facts.get("member_name") or facts.get("member_id")):
        issues.append("missing beneficiary identity")
    if not facts.get("member_id"):
        issues.append("missing member ID")
    if not (facts.get("provider_name") or facts.get("requesting_provider_npi")):
        issues.append("missing provider")
    if issues:
        return "warning", "Delivery validation warnings: " + "; ".join(issues) + "."
    return "passed", "Beneficiary and provider identity validated for delivery."


# Inbound-correspondence classification labels (RFI: automated indexing/classification).
_INBOUND_TYPES = ["clinical_records", "appeal", "p2p_request", "additional_info", "other"]


def classify_inbound(raw_text: str) -> dict:
    """AI-classify a piece of inbound correspondence + extract a short summary.

    RFI: automated indexing and classification of incoming correspondence +
    AI-assisted extraction. Uses ai_query; falls back to a keyword heuristic.
    """
    snippet = (raw_text or "")[:3000]
    prompt = (
        "Classify this inbound health-plan correspondence into exactly one of: "
        f"{', '.join(_INBOUND_TYPES)}. Then give a one-sentence summary. "
        "Respond as 'TYPE | summary'. Correspondence:\n\n" + snippet
    )
    try:
        rows = _execute_sql(f"SELECT ai_query('{LLM_ENDPOINT}', '{_sql_str(prompt)}') AS out")
        if rows and rows[0].get("out"):
            out = str(rows[0]["out"]).strip()
            label, _, summary = out.partition("|")
            label = label.strip().lower().replace(" ", "_")
            if label not in _INBOUND_TYPES:
                label = next((t for t in _INBOUND_TYPES if t in out.lower()), "other")
            return {"classified_type": label, "extracted_summary": summary.strip() or out,
                    "classification_confidence": 0.9}
    except Exception as e:  # pragma: no cover - defensive
        print(f"[correspondence] ai_query inbound classification failed: {e}")
    # Keyword fallback.
    low = snippet.lower()
    label = ("appeal" if "appeal" in low else
             "p2p_request" if "peer-to-peer" in low or "p2p" in low else
             "additional_info" if "additional information" in low or "requested records" in low else
             "clinical_records" if any(k in low for k in ("history", "diagnosis", "clinical", "chart")) else
             "other")
    return {"classified_type": label, "extracted_summary": snippet[:200],
            "classification_confidence": 0.5}


def build_notice(notice_type: str, facts: dict, language: str = "en") -> dict:
    """Assemble a full determination notice: subject + markdown body + metadata.

    Returns everything the router persists into pa_correspondence, including the
    PHI-redaction result so an un-redacted notice can never be marked released.
    When ``language`` is not English the redacted body is translated via ai_query.
    """
    spec = _NOTICE_SPEC.get(notice_type)
    if not spec:
        raise ValueError(f"Unknown notice_type: {notice_type}")

    effective_start = datetime.now(timezone.utc).date()
    effective_end = effective_start + timedelta(days=180)

    criteria_citation = (
        f"{facts.get('policy_name') or 'Medical Policy'} "
        f"(Policy ID {facts.get('policy_id') or 'N/A'}"
        + (f", criteria version {facts['criteria_version']}" if facts.get("criteria_version") else "")
        + ")"
    )

    rationale = _draft_rationale(notice_type, facts)

    lines = [
        f"# {spec['title']}",
        "",
        f"**Date:** {effective_start.isoformat()}  ",
        f"**Member:** {facts.get('member_name') or facts.get('member_id') or 'N/A'} "
        f"(ID {facts.get('member_id') or 'N/A'})  ",
        f"**Requesting Provider:** {facts.get('provider_name') or facts.get('requesting_provider_npi') or 'N/A'}  ",
        f"**Authorization Reference:** {facts.get('auth_request_id') or 'N/A'}  ",
        f"**Service:** {facts.get('procedure_description') or facts.get('procedure_code') or 'N/A'} "
        f"({facts.get('procedure_code') or 'N/A'})  ",
        f"**Line of Business:** {facts.get('line_of_business') or 'N/A'}",
        "",
        "## Determination",
        "",
        rationale,
        "",
        f"**Criteria applied:** {criteria_citation}",
    ]

    if spec["adverse"] and facts.get("denial_reason_code"):
        lines += ["", f"**Denial Reason Code:** {facts['denial_reason_code']}"]

    if notice_type in ("approval", "partial_approval"):
        lines += [
            "",
            f"**Authorization Effective Dates:** {effective_start.isoformat()} through "
            f"{effective_end.isoformat()}",
        ]

    if spec["appeal_rights"]:
        lines += ["", "---", "", _APPEAL_RIGHTS]

    body_markdown = "\n".join(lines)
    clean_body, _redacted_input, phi_found = redact_phi(body_markdown)
    # was_redacted True means PHI WAS present and has now been masked; the notice
    # is safe to release only once this gate has run.
    redaction_notes = (
        f"Masked identifiers: {', '.join(phi_found)}" if phi_found
        else "No PHI/PII identifiers detected."
    )

    # Multilingual pass runs AFTER redaction so PHI is never sent to translation.
    if language and language != "en":
        clean_body = _translate_body(clean_body, language)

    validation_status, validation_notes = validate_delivery(facts)

    return {
        "notice_type": notice_type,
        "subject": spec["title"],
        "language": language or "en",
        "body_markdown": clean_body,
        "body_redacted": True,  # gate has run
        "redaction_notes": redaction_notes,
        "includes_appeal_rights": spec["appeal_rights"],
        "criteria_citation": criteria_citation,
        "template_version": TEMPLATE_VERSION,
        "validation_status": validation_status,
        "validation_notes": validation_notes,
    }


def render_notice_pdf(notice: dict, facts: dict) -> str:
    """Render the notice markdown to a simple PDF in the pa_documents UC Volume.

    Returns the Volume path. Reuses the same reportlab + UC Volume approach as
    the sample-record generator.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas
    from databricks.sdk import WorkspaceClient

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    width, height = letter
    y = height - inch

    for raw in notice["body_markdown"].split("\n"):
        line = raw.replace("**", "").replace("# ", "").replace("#", "").rstrip()
        if not line:
            y -= 10
            continue
        # Simple wrap at ~95 chars
        for chunk in [line[i:i + 95] for i in range(0, len(line), 95)] or [""]:
            if y < inch:
                c.showPage()
                y = height - inch
            c.setFont("Helvetica", 10)
            c.drawString(inch, y, chunk)
            y -= 14

    c.save()
    buf.seek(0)

    aid = (facts.get("auth_request_id") or "notice").replace("/", "_")
    object_name = f"notices/{aid}_{notice['notice_type']}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.pdf"
    volume_path = f"{PA_DOC_VOLUME_PATH}/{object_name}"
    WorkspaceClient().files.upload(volume_path, buf, overwrite=True)
    return volume_path

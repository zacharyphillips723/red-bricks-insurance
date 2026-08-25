"""Standards-based Prior Authorization intake: X12 278 + Da Vinci FHIR PAS.

RFI mapping — Intake + Integration & Interoperability tabs:
  - X12 278 (Health Care Services Review — Request for Review & Response)
  - Da Vinci PAS (Prior Authorization Support) FHIR Claim/$submit
  - Demonstrates multi-channel, standards-based intake normalized into the same
    PA request shape the rest of the medallion consumes.

Pure Python (no Databricks / no third-party deps) so the build+parse round-trip
is fully unit-testable offline. In production the parse side is handled by the
Databricks **X12 EDI (Ember)** accelerator and **dbignite** (FHIR); these
functions are a self-contained, demo-grade equivalent that produces the same
normalized columns those accelerators would.
"""

from __future__ import annotations

import json
from typing import Any

SEG = "~"      # X12 segment terminator
ELEM = "*"     # X12 element separator
SUB = ":"      # X12 component (sub-element) separator


# ---------------------------------------------------------------------------
# X12 278 — build
# ---------------------------------------------------------------------------

def build_x12_278(req: dict[str, Any]) -> str:
    """Render a PA request as a (simplified but well-formed) X12 278 request.

    Encodes the fields the parser recovers: auth id (BHT03), provider NPI
    (NM1*1P), member id (NM1*IL), urgency + service type (UM), diagnosis codes
    (HI), procedure code (SV2), and request date (DTP*435).
    """
    dx = _as_list(req.get("diagnosis_codes"))
    # HI diagnosis composites: first is principal (ABK), rest are secondary (ABF).
    hi_parts = []
    for i, code in enumerate(dx):
        qual = "ABK" if i == 0 else "ABF"
        hi_parts.append(f"{qual}{SUB}{code.replace('.', '')}")
    hi_seg = f"HI{ELEM}" + ELEM.join(hi_parts) if hi_parts else ""

    urgency_code = "U" if req.get("urgency") == "expedited" else "R"  # U=urgent, R=routine
    date_str = _date_str(req.get("request_date"))

    segments = [
        f"ST{ELEM}278{ELEM}0001",
        f"BHT{ELEM}0007{ELEM}13{ELEM}{req.get('auth_request_id','')}{ELEM}{date_str}",
        f"HL{ELEM}1{ELEM}{ELEM}20{ELEM}1",                                  # payer/UMO
        _nm1("X3", "2", "RED BRICKS HEALTH PLAN", "", ""),
        f"HL{ELEM}2{ELEM}1{ELEM}21{ELEM}1",                                 # requester (provider)
        _nm1("1P", "2", "REQUESTING PROVIDER", "XX", req.get("requesting_provider_npi", "")),
        f"HL{ELEM}3{ELEM}2{ELEM}22{ELEM}1",                                 # subscriber (member)
        _nm1("IL", "1", "", "MI", req.get("member_id", "")),
        f"UM{ELEM}SC{ELEM}{urgency_code}{ELEM}{req.get('service_type','')}",  # services review
        hi_seg,
        f"SV2{ELEM}{ELEM}HC{SUB}{req.get('procedure_code','')}",             # institutional service line
        f"DTP{ELEM}435{ELEM}D8{ELEM}{date_str}",                             # event/admission date
        f"SE{ELEM}12{ELEM}0001",
    ]
    return SEG.join(s for s in segments if s) + SEG


def _nm1(entity: str, entity_type: str, name: str, id_qual: str, id_val: str) -> str:
    """Build an NM1 segment with the identifier correctly at NM108/NM109.

    Elements NM101..NM109: entity code, type, name, first, middle, prefix,
    suffix, id qualifier, id value. Exactly 9 elements => id at parts[9].
    """
    elems = [entity, entity_type, name, "", "", "", "", id_qual, id_val]
    return "NM1" + ELEM + ELEM.join(elems)


def parse_x12_278(edi_text: str) -> dict[str, Any]:
    """Parse a 278 request back into normalized PA-request fields."""
    out: dict[str, Any] = {"source_channel": "x12_278"}
    dx: list[str] = []
    for raw in edi_text.split(SEG):
        seg = raw.strip()
        if not seg:
            continue
        parts = seg.split(ELEM)
        tag = parts[0]
        if tag == "BHT" and len(parts) > 3:
            out["auth_request_id"] = parts[3]
            if len(parts) > 4 and parts[4]:
                out["request_date"] = _from_date_str(parts[4])
        elif tag == "NM1" and len(parts) > 1:
            entity = parts[1]
            if entity == "1P" and len(parts) > 9:
                out["requesting_provider_npi"] = parts[9]
            elif entity == "IL" and len(parts) > 9:
                out["member_id"] = parts[9]
        elif tag == "UM" and len(parts) > 3:
            out["urgency"] = "expedited" if parts[2] == "U" else "standard"
            out["service_type"] = parts[3]
        elif tag == "HI" and len(parts) > 1:
            for comp in parts[1:]:
                bits = comp.split(SUB)
                if len(bits) == 2 and bits[1]:
                    dx.append(_reformat_icd10(bits[1]))
        elif tag == "SV2" and len(parts) > 2:
            svc = parts[2].split(SUB)
            if len(svc) == 2:
                out["procedure_code"] = svc[1]
    if dx:
        out["diagnosis_codes"] = "|".join(dx)
    return out


# ---------------------------------------------------------------------------
# Da Vinci PAS — FHIR Claim ($submit) build + parse
# ---------------------------------------------------------------------------

def build_fhir_pas_claim(req: dict[str, Any]) -> dict[str, Any]:
    """Render a PA request as a Da Vinci PAS Claim resource (use=preauthorization)."""
    dx = _as_list(req.get("diagnosis_codes"))
    diagnosis = [
        {
            "sequence": i + 1,
            "diagnosisCodeableConcept": {
                "coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": code}]
            },
        }
        for i, code in enumerate(dx)
    ]
    return {
        "resourceType": "Claim",
        "identifier": [{"system": "urn:redbricks:pa", "value": req.get("auth_request_id", "")}],
        "status": "active",
        "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/claim-type", "code": "institutional"}]},
        "use": "preauthorization",
        "created": _iso_date(req.get("request_date")),
        "priority": {"coding": [{"code": "stat" if req.get("urgency") == "expedited" else "normal"}]},
        "patient": {"identifier": {"system": "urn:redbricks:member", "value": req.get("member_id", "")}},
        "provider": {"identifier": {"system": "http://hl7.org/fhir/sid/us-npi", "value": req.get("requesting_provider_npi", "")}},
        "diagnosis": diagnosis,
        "item": [
            {
                "sequence": 1,
                "category": {"text": req.get("service_type", "")},
                "productOrService": {
                    "coding": [{"system": "http://www.ama-assn.org/go/cpt", "code": req.get("procedure_code", "")}]
                },
            }
        ],
    }


def parse_fhir_pas_claim(claim: dict[str, Any]) -> dict[str, Any]:
    """Flatten a PAS Claim into normalized PA-request fields."""
    out: dict[str, Any] = {"source_channel": "fhir_pas"}
    idents = claim.get("identifier") or []
    if idents:
        out["auth_request_id"] = idents[0].get("value", "")
    created = claim.get("created")
    if created:
        out["request_date"] = created[:10]
    prio = (((claim.get("priority") or {}).get("coding") or [{}])[0]).get("code")
    out["urgency"] = "expedited" if prio == "stat" else "standard"
    out["member_id"] = ((claim.get("patient") or {}).get("identifier") or {}).get("value", "")
    out["requesting_provider_npi"] = ((claim.get("provider") or {}).get("identifier") or {}).get("value", "")

    dx = []
    for d in claim.get("diagnosis") or []:
        for c in ((d.get("diagnosisCodeableConcept") or {}).get("coding") or []):
            if c.get("code"):
                dx.append(c["code"])
    if dx:
        out["diagnosis_codes"] = "|".join(dx)

    items = claim.get("item") or []
    if items:
        out["service_type"] = (items[0].get("category") or {}).get("text", "")
        for c in ((items[0].get("productOrService") or {}).get("coding") or []):
            if c.get("code"):
                out["procedure_code"] = c["code"]
                break
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).replace("|", ",").split(",") if v.strip()]


def _date_str(d: Any) -> str:
    """X12 CCYYMMDD from a date/datetime/ISO string."""
    s = _iso_date(d)
    return s.replace("-", "")[:8] if s else ""


def _from_date_str(s: str) -> str:
    """CCYYMMDD -> ISO YYYY-MM-DD."""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}" if s and len(s) >= 8 else s


def _iso_date(d: Any) -> str:
    if d is None:
        return ""
    if hasattr(d, "strftime"):
        return d.strftime("%Y-%m-%d")
    return str(d)[:10]


def _reformat_icd10(code: str) -> str:
    """Reinsert the ICD-10-CM dot dropped for X12 transport (E1165 -> E11.65)."""
    if len(code) > 3 and code[3] != ".":
        return f"{code[:3]}.{code[3:]}"
    return code

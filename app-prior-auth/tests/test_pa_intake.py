"""Offline round-trip tests for standards-based PA intake (X12 278 + FHIR PAS).

Imports the pure-Python intake module from src/data_generation/domains — no
Databricks or workspace access needed.
"""

import sys
from pathlib import Path

# Repo root = app-prior-auth/tests/ -> parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.data_generation.domains.pa_intake import (  # noqa: E402
    build_x12_278, parse_x12_278, build_fhir_pas_claim, parse_fhir_pas_claim,
)

REQ = {
    "auth_request_id": "PA-2025-004321",
    "member_id": "MBR00087654",
    "requesting_provider_npi": "1982746501",
    "service_type": "imaging",
    "procedure_code": "72148",
    "diagnosis_codes": ["M54.5", "E11.65"],
    "urgency": "expedited",
    "request_date": "2025-06-14",
}

# Normalized fields both channels must recover identically.
_KEYS = ["auth_request_id", "member_id", "requesting_provider_npi",
         "service_type", "procedure_code", "urgency", "diagnosis_codes"]


def test_x12_278_roundtrip():
    edi = build_x12_278(REQ)
    assert edi.startswith("ST*278*")
    assert edi.strip().endswith("~")
    parsed = parse_x12_278(edi)
    assert parsed["source_channel"] == "x12_278"
    assert parsed["auth_request_id"] == REQ["auth_request_id"]
    assert parsed["member_id"] == REQ["member_id"]
    assert parsed["requesting_provider_npi"] == REQ["requesting_provider_npi"]
    assert parsed["procedure_code"] == "72148"
    assert parsed["urgency"] == "expedited"
    assert parsed["service_type"] == "imaging"
    # ICD-10 dot is stripped for X12 transport then restored on parse
    assert parsed["diagnosis_codes"] == "M54.5|E11.65"
    assert parsed["request_date"] == "2025-06-14"


def test_x12_278_standard_urgency():
    edi = build_x12_278({**REQ, "urgency": "standard"})
    assert parse_x12_278(edi)["urgency"] == "standard"


def test_fhir_pas_roundtrip():
    claim = build_fhir_pas_claim(REQ)
    assert claim["resourceType"] == "Claim"
    assert claim["use"] == "preauthorization"
    parsed = parse_fhir_pas_claim(claim)
    assert parsed["source_channel"] == "fhir_pas"
    assert parsed["auth_request_id"] == REQ["auth_request_id"]
    assert parsed["member_id"] == REQ["member_id"]
    assert parsed["requesting_provider_npi"] == REQ["requesting_provider_npi"]
    assert parsed["procedure_code"] == "72148"
    assert parsed["urgency"] == "expedited"
    assert parsed["service_type"] == "imaging"
    assert parsed["diagnosis_codes"] == "M54.5|E11.65"
    assert parsed["request_date"] == "2025-06-14"


def test_both_channels_normalize_identically():
    """The two standards must produce the same normalized record."""
    x = parse_x12_278(build_x12_278(REQ))
    f = parse_fhir_pas_claim(build_fhir_pas_claim(REQ))
    for k in _KEYS:
        assert x[k] == f[k], f"channel mismatch on {k}: x12={x[k]!r} fhir={f[k]!r}"


def test_single_diagnosis_and_missing_fields():
    req = {"auth_request_id": "PA-1", "member_id": "MBR1", "requesting_provider_npi": "1234567890",
           "service_type": "surgery", "procedure_code": "27447", "diagnosis_codes": ["M17.11"],
           "urgency": "standard", "request_date": "2025-01-02"}
    assert parse_x12_278(build_x12_278(req))["diagnosis_codes"] == "M17.11"
    assert parse_fhir_pas_claim(build_fhir_pas_claim(req))["diagnosis_codes"] == "M17.11"

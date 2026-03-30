"""FWA (Fraud, Waste & Abuse) synthetic data generation.

Derived domain — reads existing claims/providers/members and produces
FWA signal records, provider risk profiles, and investigation cases.
"""

import hashlib
import json
import random
from datetime import date, timedelta
from typing import List, Dict, Optional

from ..reference_data import (
    FWA_FRAUD_TYPES,
    FWA_DETECTION_METHODS,
    FWA_SEVERITY_LEVELS,
    FWA_PROVIDER_THRESHOLDS,
    FWA_INVESTIGATION_STATUSES,
    SPECIALTIES,
)
from ..helpers import weighted_choice, random_date_between


def _seeded_hash(claim_id: str, seed: int = 42) -> float:
    """Deterministic hash to select claims for flagging."""
    h = hashlib.md5(f"{seed}:{claim_id}".encode()).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def _generate_evidence_summary(fraud_type: str, claim: dict, provider_npi: str) -> tuple[str, dict]:
    """Generate type-specific evidence summary and detail JSON."""
    procedure = claim.get("procedure_code") or "99213"
    billed = claim.get("billed_amount") or 0
    member_id = claim.get("member_id") or "UNK"
    service_date = claim.get("service_from_date") or "2025-01-15"

    evidence_map = {
        "duplicate_billing": (
            f"Duplicate claim detected: member {member_id} billed for {procedure} on {service_date} "
            f"by provider {provider_npi}. Second claim with identical service parameters found.",
            {"duplicate_claim_count": random.randint(2, 4), "matching_fields": ["member_id", "service_date", "procedure_code"]}
        ),
        "upcoding": (
            f"E&M upcoding suspected: provider {provider_npi} billed 99215 (level 5) at "
            f"{random.randint(45, 75)}% rate vs specialty average of {random.randint(15, 25)}%. "
            f"Billed ${billed:.2f} for this claim.",
            {"e5_visit_pct": round(random.uniform(0.45, 0.75), 3), "specialty_avg": round(random.uniform(0.15, 0.25), 3)}
        ),
        "unbundling": (
            f"Unbundling detected: {random.randint(2, 5)} related CPT codes billed separately on "
            f"{service_date} instead of bundled code. Total billed ${billed * random.uniform(1.5, 3.0):.2f}.",
            {"separate_codes": [procedure, str(int(procedure) + 1) if procedure.isdigit() else "99214"], "should_be_bundled": True}
        ),
        "impossible_day": (
            f"Impossible day: provider {provider_npi} billed {random.randint(55, 120)} unique patients "
            f"on {service_date}, exceeding the {FWA_PROVIDER_THRESHOLDS['max_patients_per_day']}-patient daily threshold.",
            {"patients_billed": random.randint(55, 120), "threshold": FWA_PROVIDER_THRESHOLDS["max_patients_per_day"]}
        ),
        "phantom_billing": (
            f"Phantom billing: claim for {procedure} on {service_date} with no other clinical "
            f"activity at provider {provider_npi} within ±90 days for member {member_id}.",
            {"days_to_nearest_activity": random.randint(91, 365), "activity_window_days": 90}
        ),
        "provider_ring": (
            f"Provider ring detected: {provider_npi} is part of a {random.randint(3, 5)}-provider "
            f"cluster sharing {random.randint(30, 60)}% of their member panels.",
            {"ring_size": random.randint(3, 5), "overlap_pct": round(random.uniform(0.30, 0.60), 3)}
        ),
        "doctor_shopping": (
            f"Doctor shopping: member {member_id} visited {random.randint(5, 12)} providers for "
            f"the same diagnosis within 90 days. Total claims: {random.randint(8, 25)}.",
            {"provider_count_90d": random.randint(5, 12), "same_diagnosis": True, "window_days": 90}
        ),
        "short_refill": (
            f"Short refill: next pharmacy fill occurred at {random.randint(40, 70)}% of days_supply "
            f"({random.randint(7, 21)} days into a {random.randint(28, 90)}-day supply).",
            {"days_to_next_fill": random.randint(7, 21), "days_supply": random.randint(28, 90), "threshold_pct": 0.75}
        ),
        "drug_switching": (
            f"Drug switching: generic-to-brand switch within {random.randint(15, 55)} days in the "
            f"same therapeutic class. Brand cost ${random.uniform(200, 800):.2f} vs generic ${random.uniform(5, 50):.2f}.",
            {"switch_days": random.randint(15, 55), "brand_cost": round(random.uniform(200, 800), 2), "generic_cost": round(random.uniform(5, 50), 2)}
        ),
    }

    summary, detail = evidence_map.get(fraud_type, ("Suspicious pattern detected.", {}))
    return summary, detail


def generate_fwa_signals(
    medical_claims: List[Dict],
    pharmacy_claims: List[Dict],
    providers: List[Dict],
    members: List[Dict],
    fraud_rate: float = 0.07,
) -> List[Dict]:
    """Generate FWA signal records from existing claims data.

    Selects ~7% of claims deterministically and assigns fraud types,
    scores, and evidence based on claim characteristics.

    Returns ~10K+ signal records.
    """
    random.seed(42)

    fraud_type_names = [ft[0] for ft in FWA_FRAUD_TYPES]
    fraud_type_descs = {ft[0]: ft[1] for ft in FWA_FRAUD_TYPES}
    fraud_type_weights = [ft[2] for ft in FWA_FRAUD_TYPES]
    severity_names = [s[0] for s in FWA_SEVERITY_LEVELS]
    severity_weights = [s[1] for s in FWA_SEVERITY_LEVELS]

    # Build provider NPI lookup
    provider_map = {p["npi"]: p for p in providers if p.get("npi")}
    member_set = {m["member_id"] for m in members if m.get("member_id")}

    signals = []
    signal_counter = 0

    # Process medical claims
    for claim in medical_claims:
        claim_id = claim.get("claim_id", "")
        if not claim_id:
            continue

        # Deterministic selection based on seeded hash
        if _seeded_hash(claim_id) > fraud_rate:
            continue

        signal_counter += 1
        provider_npi = claim.get("rendering_provider_npi") or ""
        member_id = claim.get("member_id") or ""

        # Assign fraud type and severity
        fraud_type = weighted_choice(fraud_type_names, fraud_type_weights)
        severity = weighted_choice(severity_names, severity_weights)

        # Fraud score: higher for more severe types, with randomness
        base_score = {
            "Critical": random.uniform(0.75, 0.98),
            "High": random.uniform(0.55, 0.80),
            "Medium": random.uniform(0.35, 0.60),
            "Low": random.uniform(0.15, 0.40),
        }[severity]
        fraud_score = round(min(1.0, base_score + random.gauss(0, 0.05)), 4)

        # Detection method — weighted toward rules_engine and statistical
        detection_method = random.choices(
            FWA_DETECTION_METHODS,
            weights=[30, 20, 15, 10, 5, 5, 10, 3, 2],
            k=1,
        )[0]

        # Generate evidence
        evidence_summary, evidence_detail = _generate_evidence_summary(fraud_type, claim, provider_npi)

        # Estimated overpayment
        paid = claim.get("paid_amount", 0) or 0
        overpayment_pct = {
            "duplicate_billing": random.uniform(0.80, 1.00),
            "upcoding": random.uniform(0.20, 0.50),
            "unbundling": random.uniform(0.15, 0.40),
            "impossible_day": random.uniform(0.50, 1.00),
            "phantom_billing": random.uniform(0.90, 1.00),
            "provider_ring": random.uniform(0.10, 0.30),
            "doctor_shopping": random.uniform(0.30, 0.70),
            "short_refill": random.uniform(0.40, 0.80),
            "drug_switching": random.uniform(0.50, 0.80),
        }.get(fraud_type, 0.30)
        estimated_overpayment = round(paid * overpayment_pct, 2)

        service_date = claim.get("service_from_date") or "2025-01-15"
        detection_date = str(
            random_date_between(date(2025, 1, 1), date(2026, 3, 15))
        )

        signals.append({
            "signal_id": f"FWA-{signal_counter:07d}",
            "claim_id": claim_id,
            "member_id": member_id,
            "provider_npi": provider_npi,
            "fraud_type": fraud_type,
            "fraud_type_desc": fraud_type_descs[fraud_type],
            "fraud_score": fraud_score,
            "severity": severity,
            "detection_method": detection_method,
            "evidence_summary": evidence_summary,
            "evidence_detail_json": json.dumps(evidence_detail),
            "service_date": str(service_date),
            "paid_amount": paid,
            "estimated_overpayment": estimated_overpayment,
            "detection_date": detection_date,
        })

    # Process pharmacy claims for drug-related fraud types
    for claim in pharmacy_claims:
        claim_id = claim.get("claim_id", "")
        if not claim_id:
            continue

        if _seeded_hash(claim_id, seed=43) > fraud_rate * 0.5:
            continue

        signal_counter += 1
        # Pharmacy fraud types only
        fraud_type = random.choices(
            ["short_refill", "drug_switching"],
            weights=[60, 40],
            k=1,
        )[0]
        severity = weighted_choice(severity_names, severity_weights)

        base_score = {
            "Critical": random.uniform(0.70, 0.95),
            "High": random.uniform(0.50, 0.75),
            "Medium": random.uniform(0.30, 0.55),
            "Low": random.uniform(0.15, 0.35),
        }[severity]
        fraud_score = round(min(1.0, base_score + random.gauss(0, 0.05)), 4)

        plan_paid = claim.get("plan_paid", 0) or 0
        overpayment_pct = random.uniform(0.40, 0.80)
        estimated_overpayment = round(plan_paid * overpayment_pct, 2)

        evidence_summary = (
            f"{'Short refill' if fraud_type == 'short_refill' else 'Drug switching'} detected for "
            f"{claim.get('drug_name', 'unknown')} ({claim.get('therapeutic_class', 'unknown')}). "
            f"Member {claim.get('member_id', '')} at pharmacy {claim.get('pharmacy_name', '')}."
        )

        signals.append({
            "signal_id": f"FWA-{signal_counter:07d}",
            "claim_id": claim_id,
            "member_id": claim.get("member_id", ""),
            "provider_npi": claim.get("prescriber_npi", ""),
            "fraud_type": fraud_type,
            "fraud_type_desc": fraud_type_descs[fraud_type],
            "fraud_score": fraud_score,
            "severity": severity,
            "detection_method": "rules_engine",
            "evidence_summary": evidence_summary,
            "evidence_detail_json": json.dumps({"drug_name": claim.get("drug_name"), "days_supply": claim.get("days_supply")}),
            "service_date": str(claim.get("fill_date", "2025-01-15")),
            "paid_amount": plan_paid,
            "estimated_overpayment": estimated_overpayment,
            "detection_date": str(random_date_between(date(2025, 1, 1), date(2026, 3, 15))),
        })

    print(f"  FWA signals generated: {len(signals)}")
    return signals


def generate_fwa_provider_profiles(
    medical_claims: List[Dict],
    providers: List[Dict],
    fwa_signals: List[Dict],
) -> List[Dict]:
    """Generate per-provider FWA risk profiles.

    Aggregates billing patterns and FWA signal counts per provider.
    Returns ~500 records (one per provider).
    """
    random.seed(42)

    # Aggregate claims per provider
    provider_claims: Dict[str, List[Dict]] = {}
    for claim in medical_claims:
        npi = claim.get("rendering_provider_npi", "")
        if npi:
            provider_claims.setdefault(npi, []).append(claim)

    # Aggregate FWA signals per provider
    provider_signals: Dict[str, List[Dict]] = {}
    for sig in fwa_signals:
        npi = sig.get("provider_npi", "")
        if npi:
            provider_signals.setdefault(npi, []).append(sig)

    # Build provider lookup
    provider_map = {p["npi"]: p for p in providers if p.get("npi")}

    profiles = []
    for npi, claims_list in provider_claims.items():
        provider = provider_map.get(npi, {})
        if not provider:
            continue

        total_claims = len(claims_list)
        total_billed = sum(c.get("billed_amount", 0) or 0 for c in claims_list)
        total_paid = sum(c.get("paid_amount", 0) or 0 for c in claims_list)
        total_allowed = sum(c.get("allowed_amount", 0) or 0 for c in claims_list)
        unique_members = len(set(c.get("member_id", "") for c in claims_list))
        denied = sum(1 for c in claims_list if c.get("claim_status") == "denied")

        # E&M level 5 percentage
        e5_count = sum(1 for c in claims_list if c.get("procedure_code") == "99215")
        professional_count = sum(1 for c in claims_list if c.get("claim_type") == "Professional")

        signals = provider_signals.get(npi, [])
        fwa_signal_count = len(signals)
        fwa_score_avg = (
            sum(s.get("fraud_score", 0) for s in signals) / fwa_signal_count
            if fwa_signal_count > 0
            else 0
        )

        # Risk tier
        if fwa_signal_count >= 15 or fwa_score_avg >= 0.7:
            risk_tier = "Critical"
        elif fwa_signal_count >= 8 or fwa_score_avg >= 0.5:
            risk_tier = "High"
        elif fwa_signal_count >= 3 or fwa_score_avg >= 0.3:
            risk_tier = "Medium"
        else:
            risk_tier = "Low"

        # Behavioral flags
        flags = []
        e5_pct = e5_count / max(professional_count, 1)
        if e5_pct > FWA_PROVIDER_THRESHOLDS["e5_visit_pct_threshold"]:
            flags.append("high_e5_pct")
        billed_ratio = total_billed / max(total_allowed, 1)
        if billed_ratio > FWA_PROVIDER_THRESHOLDS["billed_to_allowed_threshold"]:
            flags.append("high_billed_ratio")
        if denied / max(total_claims, 1) > 0.15:
            flags.append("high_denial_rate")
        if fwa_signal_count > 10:
            flags.append("multiple_fwa_signals")

        profiles.append({
            "provider_npi": npi,
            "provider_name": provider.get("provider_name", ""),
            "specialty": provider.get("specialty", ""),
            "total_claims": total_claims,
            "total_billed": round(total_billed, 2),
            "total_paid": round(total_paid, 2),
            "avg_billed_per_claim": round(total_billed / max(total_claims, 1), 2),
            "billed_to_allowed_ratio": round(billed_ratio, 4),
            "e5_visit_pct": round(e5_pct, 4),
            "unique_members": unique_members,
            "denial_rate": round(denied / max(total_claims, 1), 4),
            "fwa_signal_count": fwa_signal_count,
            "fwa_score_avg": round(fwa_score_avg, 4),
            "risk_tier": risk_tier,
            "behavioral_flags": ",".join(flags) if flags else "",
        })

    print(f"  FWA provider profiles generated: {len(profiles)}")
    return profiles


def generate_fwa_investigation_cases(
    fwa_signals: List[Dict],
    provider_profiles: List[Dict],
    n_cases: int = 75,
) -> List[Dict]:
    """Generate pre-seeded investigation cases.

    60% provider-focused, 25% member-focused, 15% network-focused.
    Returns 75 investigation case records.
    """
    random.seed(42)

    status_names = [s[0] for s in FWA_INVESTIGATION_STATUSES]
    status_weights = [s[1] for s in FWA_INVESTIGATION_STATUSES]
    severity_names = [s[0] for s in FWA_SEVERITY_LEVELS]
    severity_weights = [s[1] for s in FWA_SEVERITY_LEVELS]

    # Sort providers by signal count for realistic investigation targets
    sorted_providers = sorted(
        provider_profiles, key=lambda p: p.get("fwa_signal_count", 0), reverse=True
    )

    # Collect unique members with signals
    member_signals: Dict[str, List[Dict]] = {}
    for sig in fwa_signals:
        mid = sig.get("member_id", "")
        if mid:
            member_signals.setdefault(mid, []).append(sig)
    top_members = sorted(
        member_signals.items(), key=lambda x: len(x[1]), reverse=True
    )[:50]

    cases = []

    # Provider-focused cases (60% = 45 cases)
    n_provider = int(n_cases * 0.60)
    for i in range(min(n_provider, len(sorted_providers))):
        prov = sorted_providers[i]
        npi = prov["provider_npi"]
        prov_sigs = [s for s in fwa_signals if s.get("provider_npi") == npi]

        fraud_types = list(set(s["fraud_type"] for s in prov_sigs[:10]))
        severity = weighted_choice(severity_names, severity_weights)
        status = weighted_choice(status_names, status_weights)
        est_overpayment = sum(s.get("estimated_overpayment", 0) for s in prov_sigs)

        rules_score = round(random.uniform(0.3, 0.95), 3)
        ml_score = round(random.uniform(0.2, 0.90), 3)

        cases.append({
            "investigation_id": f"INV-{i + 1:04d}",
            "investigation_type": "Provider",
            "target_type": "provider",
            "target_id": npi,
            "target_name": prov.get("provider_name", f"Provider {npi}"),
            "fraud_types": ",".join(fraud_types[:3]),
            "severity": severity,
            "status": status,
            "estimated_overpayment": round(est_overpayment, 2),
            "claims_involved_count": len(prov_sigs),
            "investigation_summary": (
                f"Provider {prov.get('provider_name', npi)} ({prov.get('specialty', 'Unknown')}) "
                f"flagged for {', '.join(fraud_types[:2])}. {len(prov_sigs)} claims flagged with "
                f"estimated overpayment of ${est_overpayment:,.2f}."
            ),
            "evidence_summary": (
                f"Risk tier: {prov.get('risk_tier', 'Unknown')}. "
                f"Billed-to-allowed ratio: {prov.get('billed_to_allowed_ratio', 0):.2f}. "
                f"E5 visit %: {prov.get('e5_visit_pct', 0):.1%}. "
                f"Behavioral flags: {prov.get('behavioral_flags', 'none')}."
            ),
            "rules_risk_score": rules_score,
            "ml_risk_score": ml_score,
            "created_date": str(random_date_between(date(2025, 3, 1), date(2026, 3, 15))),
        })

    # Member-focused cases (25% = ~19 cases)
    n_member = int(n_cases * 0.25)
    for i, (member_id, sigs) in enumerate(top_members[:n_member]):
        fraud_types = list(set(s["fraud_type"] for s in sigs[:5]))
        severity = weighted_choice(severity_names, severity_weights)
        status = weighted_choice(status_names, status_weights)
        est_overpayment = sum(s.get("estimated_overpayment", 0) for s in sigs)

        case_num = n_provider + i + 1
        cases.append({
            "investigation_id": f"INV-{case_num:04d}",
            "investigation_type": "Member",
            "target_type": "member",
            "target_id": member_id,
            "target_name": f"Member {member_id}",
            "fraud_types": ",".join(fraud_types[:3]),
            "severity": severity,
            "status": status,
            "estimated_overpayment": round(est_overpayment, 2),
            "claims_involved_count": len(sigs),
            "investigation_summary": (
                f"Member {member_id} flagged for {', '.join(fraud_types[:2])}. "
                f"{len(sigs)} claims flagged with estimated overpayment of ${est_overpayment:,.2f}."
            ),
            "evidence_summary": (
                f"Fraud types: {', '.join(fraud_types[:3])}. "
                f"Signals: {len(sigs)}. Providers involved: {len(set(s.get('provider_npi', '') for s in sigs))}."
            ),
            "rules_risk_score": round(random.uniform(0.3, 0.90), 3),
            "ml_risk_score": round(random.uniform(0.2, 0.85), 3),
            "created_date": str(random_date_between(date(2025, 3, 1), date(2026, 3, 15))),
        })

    # Network-focused cases (15% = ~11 cases)
    n_network = n_cases - len(cases)
    for i in range(n_network):
        # Create synthetic network cases from top provider pairs
        ring_size = random.randint(3, 5)
        ring_providers = random.sample(sorted_providers[:30], min(ring_size, len(sorted_providers[:30])))
        ring_npis = [p["provider_npi"] for p in ring_providers]
        ring_sigs = [s for s in fwa_signals if s.get("provider_npi") in ring_npis]

        severity = weighted_choice(severity_names, severity_weights)
        status = weighted_choice(status_names, status_weights)
        est_overpayment = sum(s.get("estimated_overpayment", 0) for s in ring_sigs[:50])

        case_num = n_provider + n_member + i + 1
        cases.append({
            "investigation_id": f"INV-{case_num:04d}",
            "investigation_type": "Network",
            "target_type": "network",
            "target_id": f"RING-{i + 1:03d}",
            "target_name": f"Provider Ring #{i + 1} ({ring_size} providers)",
            "fraud_types": "provider_ring",
            "severity": severity,
            "status": status,
            "estimated_overpayment": round(est_overpayment, 2),
            "claims_involved_count": len(ring_sigs[:50]),
            "investigation_summary": (
                f"Network investigation: {ring_size}-provider ring detected with high member overlap. "
                f"Providers: {', '.join(ring_npis[:3])}{'...' if len(ring_npis) > 3 else ''}. "
                f"Estimated overpayment: ${est_overpayment:,.2f}."
            ),
            "evidence_summary": (
                f"Ring size: {ring_size} providers. "
                f"Combined signals: {len(ring_sigs[:50])}. "
                f"Shared member overlap detected via network analysis."
            ),
            "rules_risk_score": round(random.uniform(0.4, 0.95), 3),
            "ml_risk_score": round(random.uniform(0.3, 0.90), 3),
            "created_date": str(random_date_between(date(2025, 3, 1), date(2026, 3, 15))),
        })

    print(f"  FWA investigation cases generated: {len(cases)}")
    return cases[:n_cases]

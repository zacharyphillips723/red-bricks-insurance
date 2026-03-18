# Red Bricks Insurance — documents domain: case notes, call transcripts, claims summaries (PDF + metadata).

import json
import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker

from ..helpers import random_date_between, weighted_choice

fake = Faker()
Faker.seed(42)

# ---------------------------------------------------------------------------
# Clinical templates for realistic case notes
# ---------------------------------------------------------------------------

PRESENTING_COMPLAINTS = {
    "E11.9": [
        "Elevated blood glucose levels noted at routine visit",
        "Patient reports polyuria and increased thirst over past 2 weeks",
        "Follow-up for uncontrolled type 2 diabetes mellitus",
        "Patient presenting for diabetes management review",
    ],
    "E11.65": [
        "Diabetic patient with progressive peripheral neuropathy",
        "Type 2 diabetes with worsening renal function",
        "Hyperglycemia with complications, HbA1c > 9.0",
    ],
    "I10": [
        "Hypertension follow-up, BP readings consistently elevated",
        "Patient reports headaches and dizziness, BP 158/94",
        "Routine hypertension management visit",
    ],
    "I50.9": [
        "Increased shortness of breath and lower extremity edema",
        "Heart failure exacerbation, weight gain of 5 lbs in 1 week",
        "Follow-up for chronic systolic heart failure",
    ],
    "J44.1": [
        "COPD exacerbation with increased sputum production",
        "Worsening dyspnea on exertion over past month",
        "Follow-up for chronic obstructive pulmonary disease",
    ],
    "N18.3": [
        "CKD stage 3 monitoring, eGFR trending downward",
        "Chronic kidney disease follow-up with rising creatinine",
    ],
}

ASSESSMENT_TEMPLATES = [
    "Patient's condition is {status}. Current treatment plan {plan_status}.",
    "Assessment shows {status} disease progression. {plan_status} adjustments recommended.",
    "Clinical indicators suggest {status} control. Continue current regimen with {plan_status}.",
]

PLAN_ITEMS = [
    "Continue current medication regimen",
    "Increase monitoring frequency to every 2 weeks",
    "Refer to specialist for further evaluation",
    "Order follow-up labs in 4 weeks",
    "Adjust medication dosage per clinical guidelines",
    "Schedule telehealth check-in in 2 weeks",
    "Coordinate with pharmacy for medication adherence program",
    "Add home health monitoring",
    "Initiate care management referral",
    "Discuss dietary modifications and exercise plan",
]

MEDICATIONS = [
    "Metformin 1000mg BID", "Lisinopril 20mg daily", "Amlodipine 5mg daily",
    "Atorvastatin 40mg daily", "Furosemide 40mg daily", "Carvedilol 12.5mg BID",
    "Insulin glargine 30 units nightly", "Albuterol inhaler PRN",
    "Tiotropium 18mcg daily", "Empagliflozin 25mg daily",
    "Losartan 100mg daily", "Hydrochlorothiazide 25mg daily",
]

# ---------------------------------------------------------------------------
# Call transcript templates
# ---------------------------------------------------------------------------

CALL_REASONS = [
    "Care management outreach", "Medication adherence follow-up",
    "Post-discharge follow-up", "Appointment scheduling assistance",
    "Benefits and coverage inquiry", "Referral coordination",
    "Wellness program enrollment", "Lab results discussion",
]

CALL_OUTCOMES = [
    "Member engaged, agreed to follow-up plan",
    "Left voicemail, will attempt again in 48 hours",
    "Member declined services at this time",
    "Member receptive, scheduled follow-up appointment",
    "Unable to reach, number disconnected",
    "Member requested callback at a later time",
    "Completed assessment, referred to care manager",
    "Member confirmed medication compliance",
]

CALL_TRANSCRIPT_PARAGRAPHS = [
    "Caller introduced themselves and explained the purpose of the outreach call. Member was informed about available care management services and support programs.",
    "Discussed member's current health status and any recent changes in symptoms or medications. Member reported {symptom_status}.",
    "Reviewed upcoming appointments and lab orders. Confirmed member's preferred pharmacy and transportation needs for upcoming visits.",
    "Addressed member's questions about their benefits and coverage for upcoming procedures. Clarified copay and prior authorization requirements.",
    "Discussed importance of medication adherence and reviewed potential side effects. Member confirmed they are taking medications as prescribed.",
    "Reviewed dietary recommendations and physical activity goals. Member expressed interest in the wellness coaching program.",
    "Coordinated with member on scheduling a follow-up telehealth visit. Provided direct number for the care management team.",
]

SYMPTOM_STATUSES = [
    "stable symptoms with no significant changes",
    "mild improvement since last contact",
    "some worsening of symptoms requiring closer monitoring",
    "feeling well overall with good energy levels",
    "occasional dizziness but otherwise stable",
    "increased fatigue and shortness of breath",
]


def _generate_case_note_text(
    member_id: str,
    primary_dx: Optional[str],
    member_name: str,
    doc_date: date,
) -> str:
    """Generate realistic case note text content."""
    dx = primary_dx if primary_dx in PRESENTING_COMPLAINTS else "E11.9"
    complaint = random.choice(PRESENTING_COMPLAINTS[dx])

    status = random.choice(["stable", "improving", "worsening", "partially controlled"])
    plan_status = random.choice(["is effective", "requires minor", "needs significant"])
    assessment = random.choice(ASSESSMENT_TEMPLATES).format(
        status=status, plan_status=plan_status
    )

    plan = random.sample(PLAN_ITEMS, k=random.randint(2, 4))
    meds = random.sample(MEDICATIONS, k=random.randint(2, 5))

    follow_up_days = random.choice([7, 14, 21, 30, 60, 90])
    follow_up_date = doc_date + timedelta(days=follow_up_days)

    lines = [
        f"CASE NOTE — {doc_date.strftime('%B %d, %Y')}",
        f"Member: {member_name} (ID: {member_id})",
        "",
        "PRESENTING COMPLAINT:",
        complaint,
        "",
        "ASSESSMENT:",
        assessment,
        "",
        "CURRENT MEDICATIONS:",
        *[f"  - {m}" for m in meds],
        "",
        "PLAN:",
        *[f"  {i+1}. {p}" for i, p in enumerate(plan)],
        "",
        f"FOLLOW-UP: Scheduled for {follow_up_date.strftime('%B %d, %Y')}",
        f"Documenting Provider: Dr. {fake.last_name()}, {random.choice(['MD', 'DO', 'NP', 'PA'])}",
    ]
    return "\n".join(lines)


def _generate_call_transcript_text(
    member_id: str,
    member_name: str,
    doc_date: date,
) -> str:
    """Generate realistic outbound call transcript text."""
    caller = f"{fake.first_name()} {fake.last_name()}, {random.choice(['RN', 'LPN', 'CHW', 'SW'])}"
    duration = random.randint(5, 35)
    reason = random.choice(CALL_REASONS)
    outcome = random.choice(CALL_OUTCOMES)

    paragraphs = random.sample(
        CALL_TRANSCRIPT_PARAGRAPHS, k=random.randint(2, 4)
    )
    paragraphs = [
        p.format(symptom_status=random.choice(SYMPTOM_STATUSES))
        if "{symptom_status}" in p else p
        for p in paragraphs
    ]

    action_items = random.sample(PLAN_ITEMS, k=random.randint(1, 3))

    lines = [
        f"CALL TRANSCRIPT — {doc_date.strftime('%B %d, %Y')}",
        f"Member: {member_name} (ID: {member_id})",
        f"Caller: {caller}",
        f"Duration: {duration} minutes",
        f"Reason: {reason}",
        "",
        "TRANSCRIPT:",
        *[f"\n{p}" for p in paragraphs],
        "",
        f"OUTCOME: {outcome}",
        "",
        "ACTION ITEMS:",
        *[f"  - {a}" for a in action_items],
    ]
    return "\n".join(lines)


def _generate_claims_summary_text(
    member_id: str,
    member_name: str,
    member_claims: List[Dict],
    doc_date: date,
) -> str:
    """Generate a mini claims history summary report."""
    recent = sorted(member_claims, key=lambda c: c.get("service_from_date") or "", reverse=True)[:10]

    total_billed = sum(c.get("billed_amount") or 0 for c in recent)
    total_paid = sum(c.get("paid_amount") or 0 for c in recent)
    total_member_resp = sum(c.get("member_responsibility") or 0 for c in recent)

    lines = [
        f"CLAIMS SUMMARY REPORT — {doc_date.strftime('%B %d, %Y')}",
        f"Member: {member_name} (ID: {member_id})",
        f"Recent Claims Count: {len(recent)}",
        "",
        f"{'Date':<14} {'Type':<14} {'Procedure':<12} {'Diagnosis':<12} {'Billed':>10} {'Paid':>10}",
        "-" * 78,
    ]

    for c in recent:
        svc_date = (c.get("service_from_date") or "N/A")[:10]
        claim_type = (c.get("claim_type") or "N/A")[:12]
        proc = (c.get("procedure_code") or "N/A")[:10]
        dx = (c.get("primary_diagnosis_code") or "N/A")[:10]
        billed = f"${(c.get('billed_amount') or 0):,.2f}"
        paid = f"${(c.get('paid_amount') or 0):,.2f}"
        lines.append(f"{svc_date:<14} {claim_type:<14} {proc:<12} {dx:<12} {billed:>10} {paid:>10}")

    lines.extend([
        "-" * 78,
        f"{'TOTALS':<52} ${total_billed:>9,.2f} ${total_paid:>9,.2f}",
        f"Total Member Responsibility: ${total_member_resp:,.2f}",
    ])
    return "\n".join(lines)


def _sanitize_for_pdf(text: str) -> str:
    """Replace unicode characters that fpdf2 core fonts can't handle."""
    replacements = {
        "\u2014": "--",   # em dash
        "\u2013": "-",    # en dash
        "\u2018": "'",    # left single quote
        "\u2019": "'",    # right single quote
        "\u201c": '"',    # left double quote
        "\u201d": '"',    # right double quote
        "\u2026": "...",  # ellipsis
        "\u2022": "*",    # bullet
        "\u00a0": " ",    # non-breaking space
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    # Fallback: strip any remaining non-latin1 characters
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _build_pdf_bytes(text_content: str, title: str) -> bytes:
    """Build a simple PDF from text content using fpdf2."""
    from fpdf import FPDF

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Header
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(200, 16, 46)  # Databricks red
    pdf.cell(0, 10, "Red Bricks Insurance", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, _sanitize_for_pdf(title), new_x="LMARGIN", new_y="NEXT")
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(5)

    # Body
    pdf.set_font("Courier", "", 9)
    for line in _sanitize_for_pdf(text_content).split("\n"):
        pdf.cell(0, 4.5, line, new_x="LMARGIN", new_y="NEXT")

    return pdf.output()


def _generate_single_doc(args: tuple) -> Dict[str, Any]:
    """Generate a single document (text + PDF). Designed for parallel execution."""
    doc_id, mid, name, primary_dx, member_claims, doc_type, doc_date = args

    if doc_type == "case_note":
        text_content = _generate_case_note_text(mid, primary_dx, name, doc_date)
        title = "Clinical Case Note"
        author = f"Dr. {Faker().last_name()}"
    elif doc_type == "call_transcript":
        text_content = _generate_call_transcript_text(mid, name, doc_date)
        title = "Outreach Call Transcript"
        author = f"{Faker().first_name()} {Faker().last_name()}"
    else:
        text_content = _generate_claims_summary_text(mid, name, member_claims, doc_date)
        title = "Claims Summary Report"
        author = f"{Faker().first_name()} {Faker().last_name()}"

    pdf_bytes = _build_pdf_bytes(text_content, title)

    return {
        "document_id": doc_id,
        "member_id": mid,
        "document_type": doc_type,
        "title": title,
        "created_date": doc_date.isoformat(),
        "author": author,
        "full_text": text_content,
        "pdf_bytes": pdf_bytes,
        "file_name": f"{doc_id}_{mid}_{doc_type}.pdf",
    }


def generate_documents(
    members_data: List[Dict[str, Any]],
    encounters: List[Dict[str, Any]],
    claims_data: List[Dict[str, Any]],
    primary_dx_by_member: Dict[str, str],
    n_per_member: int = 3,
    max_workers: int = 8,
) -> List[Dict[str, Any]]:
    """Generate synthetic PDF documents and companion metadata for each member.

    Uses ThreadPoolExecutor for parallel PDF generation. Returns a list of
    metadata dicts including full text content and PDF bytes.

    Document types: case_note, call_transcript, claims_summary
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Build per-member claims lookup
    claims_by_member: Dict[str, List[Dict]] = {}
    for c in claims_data:
        mid = c.get("member_id")
        if mid:
            claims_by_member.setdefault(mid, []).append(c)

    # Build member name lookup
    member_names = {
        m["member_id"]: f"{m.get('first_name', 'Unknown')} {m.get('last_name', 'Unknown')}"
        for m in members_data
    }

    doc_types = ["case_note", "call_transcript", "claims_summary"]
    doc_type_weights = [0.50, 0.30, 0.20]  # case notes most frequent

    # Pre-generate all work items (deterministic ordering with pre-seeded random)
    work_items = []
    doc_counter = 0
    for member in members_data:
        mid = member["member_id"]
        name = member_names.get(mid, "Unknown Member")
        primary_dx = primary_dx_by_member.get(mid)
        member_claims = claims_by_member.get(mid, [])

        for _ in range(n_per_member):
            doc_type = weighted_choice(doc_types, doc_type_weights)
            doc_date = random_date_between(date(2024, 1, 1), date(2025, 12, 31))
            doc_id = f"DOC{100000 + doc_counter}"
            doc_counter += 1
            work_items.append((doc_id, mid, name, primary_dx, member_claims, doc_type, doc_date))

    # Generate documents in parallel
    documents: List[Dict[str, Any]] = [None] * len(work_items)  # type: ignore
    completed = 0
    total = len(work_items)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_generate_single_doc, item): idx
            for idx, item in enumerate(work_items)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            documents[idx] = future.result()
            completed += 1
            if completed % 2000 == 0:
                print(f"  Documents generated: {completed}/{total}")

    print(f"  Documents generated: {total}/{total} (complete)")
    return documents

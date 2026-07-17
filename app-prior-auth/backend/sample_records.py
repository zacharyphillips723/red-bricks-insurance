"""Synthetic medical-record PDF generator for the PA document-intake demo.

Produces downloadable, pre-populated (synthetic PHI) medical records so a demo
never stalls for lack of a real document. Three scenarios deliberately exercise
the different adjudication paths:

  approvable  — procedure + diagnosis both match a real policy, well documented
                -> Auto-Approve
  incomplete  — procedure matches a policy but clinical documentation is missing
                -> Needs Clinical Review
  non_covered — procedure code is not covered by any medical policy
                -> Auto-Deny

Codes are drawn from real rows in silver_medical_policy_rules so the downstream
Tier-1 matching produces a realistic, explainable result.
"""

import io

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

# Scenario definitions. Codes align with silver_medical_policy_rules.
SCENARIOS = {
    "approvable": {
        "title": "Auto-Approve scenario",
        "member_name": "Jane A. Doe",
        "member_id": "MBR-100238",
        "provider": "Dr. Emily Carter, Endocrinology",
        "provider_npi": "1093817465",
        "procedure": "95249 — Personal Continuous Glucose Monitor (CGM) setup",
        "diagnosis": "E11.65 — Type 2 diabetes mellitus with hyperglycemia",
        "clinical": (
            "62-year-old with type 2 diabetes mellitus, poorly controlled despite "
            "maximally tolerated oral therapy. Most recent HbA1c 9.1% (goal <7%). "
            "Patient has failed metformin 1000mg BID and glipizide 10mg daily over "
            "the past 9 months with documented adherence. Experiencing recurrent "
            "morning hyperglycemia and two nocturnal hypoglycemic episodes. "
            "Endocrinology recommends personal CGM to guide insulin titration and "
            "reduce hypoglycemia risk. Functional status: independent ADLs."
        ),
    },
    "incomplete": {
        "title": "Needs-Review scenario (missing documentation)",
        "member_name": "Robert K. Lee",
        "member_id": "MBR-100477",
        "provider": "Dr. Alan Pierce, Orthopedic Surgery",
        "provider_npi": "1548302976",
        "procedure": "27447 — Total knee arthroplasty",
        "diagnosis": "M17.11 — Unilateral primary osteoarthritis, right knee",
        # Intentionally sparse clinical detail -> fails the documentation gate.
        "clinical": "Knee pain. Requesting surgery.",
    },
    "non_covered": {
        "title": "Auto-Deny scenario (procedure not covered)",
        "member_name": "Maria S. Gomez",
        "member_id": "MBR-100913",
        "provider": "Dr. Susan Field, Dermatology",
        "provider_npi": "1730495862",
        # 15823 (blepharoplasty) is not in any Red Bricks PA policy.
        "procedure": "15823 — Blepharoplasty, upper eyelid (cosmetic)",
        "diagnosis": "H02.839 — Dermatochalasis, unspecified eyelid",
        "clinical": (
            "Patient requests upper eyelid procedure for cosmetic reasons. "
            "No documented visual-field impairment. Elective cosmetic request."
        ),
    },
}


def list_scenarios() -> list[dict]:
    return [
        {"scenario": key, "title": val["title"], "procedure": val["procedure"]}
        for key, val in SCENARIOS.items()
    ]


def generate_sample_pdf(scenario: str = "approvable") -> tuple[bytes, str]:
    """Render a synthetic PA medical record PDF for the given scenario.

    Returns (pdf_bytes, filename).
    """
    data = SCENARIOS.get(scenario, SCENARIOS["approvable"])
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    width, height = letter
    margin = 1 * inch
    y = height - margin

    def line(text: str, dy: int = 18, font: str = "Helvetica", size: int = 10):
        nonlocal y
        c.setFont(font, size)
        c.drawString(margin, y, text)
        y -= dy

    # Header
    c.setFont("Helvetica-Bold", 15)
    c.drawString(margin, y, "Red Bricks Insurance")
    y -= 22
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, y, "Prior Authorization Request — Medical Record")
    y -= 10
    c.setStrokeColorRGB(0.7, 0.1, 0.1)
    c.line(margin, y, width - margin, y)
    y -= 24

    line("SYNTHETIC RECORD — for demonstration only. Contains no real PHI.",
         dy=26, font="Helvetica-Oblique", size=8)

    line(f"Member Name:            {data['member_name']}")
    line(f"Member ID:              {data['member_id']}")
    line(f"Requesting Provider:    {data['provider']}")
    line(f"Provider NPI:           {data['provider_npi']}")
    y -= 6
    line(f"Requested Procedure:    {data['procedure']}", dy=20, font="Helvetica-Bold")
    line(f"Primary Diagnosis:      {data['diagnosis']}", dy=24, font="Helvetica-Bold")

    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, "Clinical Summary / Documentation")
    y -= 18

    # Wrap the clinical narrative.
    c.setFont("Helvetica", 10)
    words = data["clinical"].split()
    linebuf = ""
    max_chars = 90
    for w in words:
        if len(linebuf) + len(w) + 1 > max_chars:
            c.drawString(margin, y, linebuf)
            y -= 15
            linebuf = w
        else:
            linebuf = f"{linebuf} {w}".strip()
    if linebuf:
        c.drawString(margin, y, linebuf)
        y -= 15

    c.showPage()
    c.save()
    buf.seek(0)
    filename = f"sample_pa_record_{scenario}.pdf"
    return buf.read(), filename

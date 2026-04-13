# Red Bricks Insurance — medical policies domain: generates synthetic prior auth policy PDFs.

import os
import random
from datetime import date
from typing import Any, Dict, List

from fpdf import FPDF


# ---------------------------------------------------------------------------
# Policy definitions — each maps to a clinical domain with realistic criteria
# ---------------------------------------------------------------------------

POLICIES = [
    {
        "policy_id": "RBI-PA-2025-001",
        "policy_name": "Diabetes Management — CGMs, Insulin Pumps, and GLP-1 Receptor Agonists",
        "service_category": "diabetes_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-15",
        "purpose": (
            "This policy establishes clinical criteria for prior authorization of continuous glucose "
            "monitors (CGMs), insulin pump therapy, and GLP-1 receptor agonist medications for Red Bricks "
            "Insurance members with diabetes mellitus. The policy ensures appropriate utilization based on "
            "evidence-based clinical guidelines while maintaining access to medically necessary treatments."
        ),
        "covered_services": [
            {"code": "95249", "system": "CPT", "description": "CGM patient training & data interpretation", "cost_range": (200, 500)},
            {"code": "95250", "system": "CPT", "description": "CGM data analysis, physician review", "cost_range": (150, 400)},
            {"code": "95251", "system": "CPT", "description": "CGM interpretation and report", "cost_range": (100, 300)},
            {"code": "E0784", "system": "HCPCS", "description": "External insulin pump, programmable", "cost_range": (4000, 8000)},
            {"code": "A9274", "system": "HCPCS", "description": "External CGM sensor/transmitter", "cost_range": (200, 400)},
            {"code": "J3490", "system": "HCPCS", "description": "GLP-1 receptor agonist injection (semaglutide, dulaglutide, liraglutide)", "cost_range": (800, 1500)},
        ],
        "diagnosis_codes": [
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
            ("E10.9", "Type 1 diabetes mellitus without complications"),
            ("E10.65", "Type 1 diabetes mellitus with hyperglycemia"),
            ("E13.9", "Other specified diabetes mellitus without complications"),
        ],
        "clinical_criteria": [
            "Member must have a confirmed diagnosis of Type 1 or Type 2 diabetes mellitus",
            "For CGM: HbA1c >= 7.0% documented within the past 90 days, OR history of severe hypoglycemia (glucose < 54 mg/dL) within the past 6 months, OR pregnancy with pre-existing diabetes",
            "For insulin pump: Member must be on intensive insulin therapy (3+ daily injections) for >= 6 months with documented HbA1c > 7.5% despite compliance, OR frequent severe hypoglycemia (>= 2 episodes in 6 months)",
            "For GLP-1 agonists: Must have tried and failed (or have documented intolerance to) metformin at maximum tolerated dose for >= 3 months, AND must have tried at least one sulfonylurea or SGLT2 inhibitor before GLP-1 approval (step therapy requirement)",
            "BMI >= 25 required for GLP-1 approval (if primary indication is T2DM weight management component)",
            "Endocrinologist consultation required for insulin pump initiation (PCP referral acceptable for CGM and GLP-1)",
        ],
        "step_therapy": [
            "Step 1: Metformin monotherapy (minimum 3 months at maximum tolerated dose, up to 2000mg/day)",
            "Step 2: Add sulfonylurea (glimepiride, glipizide) OR SGLT2 inhibitor (empagliflozin, dapagliflozin) — minimum 3 months",
            "Step 3: If HbA1c remains > 7.0% after Steps 1-2, approve GLP-1 receptor agonist. Preferred agents: semaglutide (Ozempic), dulaglutide (Trulicity). Non-preferred: liraglutide (Victoza) — requires prior failure of preferred agent",
            "Step 4: If HbA1c remains > 8.0% despite GLP-1, consider insulin pump evaluation",
        ],
        "required_documentation": [
            "Most recent HbA1c lab result (within 90 days of request)",
            "Current medication list with start dates and dosages",
            "Documentation of step therapy compliance (pharmacy claims or provider attestation)",
            "For pumps: Insulin dose log showing 3+ daily injections for >= 6 months",
            "For CGM: Blood glucose log showing monitoring compliance (or documented barriers to finger-stick testing)",
            "Provider letter of medical necessity",
        ],
        "exclusions": [
            "Cosmetic or weight-loss-only use of GLP-1 agonists (no diabetes diagnosis)",
            "Investigational or experimental glucose monitoring devices not FDA-approved",
            "Duplicate CGM and traditional glucose monitoring supplies simultaneously",
            "Members not meeting step therapy requirements without documented clinical justification for exception",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-002",
        "policy_name": "Cardiovascular Procedures — Cardiac Catheterization, Stents, and Cardiac Rehabilitation",
        "service_category": "cardiovascular",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-20",
        "purpose": (
            "This policy defines prior authorization requirements for invasive and non-invasive cardiovascular "
            "procedures including cardiac catheterization, percutaneous coronary intervention (PCI/stent placement), "
            "cardiac MRI, echocardiography, and cardiac rehabilitation programs for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "93458", "system": "CPT", "description": "Left heart catheterization with angiography", "cost_range": (5000, 15000)},
            {"code": "92928", "system": "CPT", "description": "PCI with stent placement, single vessel", "cost_range": (15000, 40000)},
            {"code": "93306", "system": "CPT", "description": "Transthoracic echocardiography, complete", "cost_range": (400, 1200)},
            {"code": "75561", "system": "CPT", "description": "Cardiac MRI with contrast", "cost_range": (1500, 4000)},
            {"code": "93015", "system": "CPT", "description": "Cardiovascular stress test", "cost_range": (300, 800)},
            {"code": "93798", "system": "CPT", "description": "Cardiac rehabilitation, per session", "cost_range": (100, 300)},
        ],
        "diagnosis_codes": [
            ("I25.10", "Atherosclerotic heart disease of native coronary artery"),
            ("I50.9", "Heart failure, unspecified"),
            ("I21.9", "Acute myocardial infarction, unspecified"),
            ("I20.9", "Angina pectoris, unspecified"),
            ("I48.91", "Atrial fibrillation, unspecified"),
        ],
        "clinical_criteria": [
            "For cardiac catheterization: Positive or indeterminate stress test, OR acute coronary syndrome presentation, OR new-onset heart failure with EF < 40%, OR valvular heart disease requiring surgical evaluation",
            "For PCI/stent: Catheterization showing >= 70% stenosis in a major coronary artery (or >= 50% left main), AND symptoms refractory to optimal medical therapy (OMT) for >= 4 weeks, OR acute STEMI/NSTEMI presentation",
            "For cardiac MRI: Inconclusive echocardiogram, OR suspected infiltrative/inflammatory cardiomyopathy, OR assessment of myocardial viability pre-revascularization, OR complex congenital heart disease evaluation",
            "For echocardiography: New murmur, dyspnea of unclear etiology, suspected heart failure, pre-operative cardiac assessment for major non-cardiac surgery, OR follow-up of known valvular disease (maximum frequency: every 12 months for stable disease)",
            "For cardiac rehabilitation: Post-MI (within 12 months), post-CABG or PCI (within 6 months), stable heart failure with EF < 35%, OR stable angina — up to 36 sessions over 12 weeks",
        ],
        "step_therapy": [
            "Step 1: Non-invasive assessment (EKG, troponins, BNP) and trial of optimal medical therapy (beta-blocker + statin + antiplatelet + ACE inhibitor as appropriate)",
            "Step 2: Stress testing (exercise or pharmacologic) before proceeding to catheterization for stable presentations",
            "Step 3: Catheterization with intent to intervene only if stress test positive or high clinical suspicion",
            "Exception: Acute presentations (STEMI, NSTEMI, unstable angina) bypass step therapy — emergent catheterization authorized",
        ],
        "required_documentation": [
            "Recent stress test results (within 6 months) for elective catheterization",
            "Echocardiogram with documented ejection fraction",
            "Documentation of current cardiac medications and duration of medical therapy",
            "Cardiologist consultation note with clinical rationale",
            "For cardiac rehab: Discharge summary from qualifying event (MI, CABG, PCI)",
        ],
        "exclusions": [
            "Routine surveillance catheterization without clinical indication",
            "Repeat echocardiography within 12 months for stable valvular disease without symptom change",
            "Cardiac MRI for uncomplicated hypertension assessment",
            "Cardiac rehabilitation beyond 36 sessions without documented clinical necessity for extension",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-003",
        "policy_name": "Orthopedic and Musculoskeletal — Joint Replacement, Spinal Fusion, and Advanced Imaging",
        "service_category": "orthopedic",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-10",
        "purpose": (
            "This policy establishes prior authorization criteria for orthopedic procedures including total knee "
            "and hip arthroplasty, spinal fusion, arthroscopic surgery, and advanced musculoskeletal imaging (MRI) "
            "for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "27447", "system": "CPT", "description": "Total knee arthroplasty (TKR)", "cost_range": (18000, 45000)},
            {"code": "27130", "system": "CPT", "description": "Total hip arthroplasty (THR)", "cost_range": (20000, 50000)},
            {"code": "22612", "system": "CPT", "description": "Posterior lumbar interbody fusion, single level", "cost_range": (30000, 80000)},
            {"code": "29881", "system": "CPT", "description": "Knee arthroscopy with meniscectomy", "cost_range": (3000, 10000)},
            {"code": "73721", "system": "CPT", "description": "MRI lower extremity joint without contrast", "cost_range": (500, 2000)},
            {"code": "72148", "system": "CPT", "description": "MRI lumbar spine without contrast", "cost_range": (500, 2000)},
        ],
        "diagnosis_codes": [
            ("M17.11", "Primary osteoarthritis, right knee"),
            ("M16.11", "Primary osteoarthritis, right hip"),
            ("M54.5", "Low back pain"),
            ("M51.16", "Intervertebral disc disorders with radiculopathy, lumbar region"),
            ("M75.10", "Rotator cuff tear, unspecified shoulder"),
            ("M23.20", "Derangement of meniscus due to tear, unspecified knee"),
        ],
        "clinical_criteria": [
            "For TKR/THR: Radiographic evidence of moderate-to-severe joint degeneration (Kellgren-Lawrence Grade 3-4), AND failure of conservative management for >= 6 weeks including physical therapy (minimum 6 sessions), NSAIDs/analgesics, and at least one corticosteroid injection, AND functional limitation documented by validated score (WOMAC, KOOS, or Harris Hip Score)",
            "For spinal fusion: Documented structural instability (spondylolisthesis, fracture, tumor) OR failure of >= 12 weeks conservative therapy (PT, medications, epidural injections — minimum 2 injections) for degenerative disc disease, AND concordant imaging findings (MRI showing nerve compression correlating with symptoms), AND BMI < 40 (relative contraindication; requires additional documentation if BMI 40-50; absolute exclusion if BMI > 50)",
            "For knee arthroscopy: Mechanical symptoms (locking, catching, giving way) with MRI-confirmed meniscal tear or loose body, OR failed 4 weeks conservative management for meniscal tear",
            "For MRI: Clinical suspicion of internal derangement not adequately evaluated by X-ray, AND failure of initial conservative management (>= 4 weeks) for non-traumatic presentations. Acute trauma with suspected fracture, ligament tear, or spinal cord compression approved without wait period",
        ],
        "step_therapy": [
            "Step 1: Conservative management — physical therapy (6+ sessions), oral analgesics (NSAIDs, acetaminophen), activity modification",
            "Step 2: Interventional — corticosteroid injection(s), bracing/orthotics. For spine: epidural steroid injections (minimum 2, spaced 2+ weeks apart)",
            "Step 3: Advanced imaging (MRI) if symptoms persist after Steps 1-2",
            "Step 4: Surgical consultation and authorization if imaging confirms structural pathology and conservative measures exhausted",
        ],
        "required_documentation": [
            "Imaging reports (X-ray and/or MRI) with radiologist interpretation",
            "Physical therapy notes documenting sessions attended and progress",
            "Documentation of conservative treatments tried with dates and duration",
            "Validated functional outcome score (WOMAC, KOOS, ODI, or equivalent)",
            "Orthopedic surgeon consultation note with surgical recommendation",
            "For spinal fusion: BMI documented within 30 days of request",
        ],
        "exclusions": [
            "MRI for non-specific low back pain < 4 weeks duration without red flags",
            "Spinal fusion for non-specific back pain without structural imaging findings",
            "Repeat joint replacement within 5 years without documented mechanical failure",
            "Arthroscopic debridement for knee osteoarthritis (not supported by clinical evidence)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-004",
        "policy_name": "Behavioral Health — Inpatient Psychiatry, Residential Treatment, and Neuromodulation",
        "service_category": "behavioral_health",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-01",
        "purpose": (
            "This policy defines prior authorization criteria for behavioral health services including "
            "inpatient psychiatric hospitalization, residential treatment, intensive outpatient programs (IOP), "
            "electroconvulsive therapy (ECT), and transcranial magnetic stimulation (TMS) for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "99221", "system": "CPT", "description": "Initial inpatient psychiatric evaluation", "cost_range": (500, 1200)},
            {"code": "90837", "system": "CPT", "description": "Psychotherapy, 60 minutes", "cost_range": (120, 250)},
            {"code": "90847", "system": "CPT", "description": "Family psychotherapy with patient present", "cost_range": (130, 260)},
            {"code": "90870", "system": "CPT", "description": "Electroconvulsive therapy (ECT)", "cost_range": (500, 1500)},
            {"code": "90867", "system": "CPT", "description": "Transcranial magnetic stimulation (TMS), initial", "cost_range": (300, 600)},
            {"code": "H0015", "system": "HCPCS", "description": "Intensive outpatient program (IOP), per diem", "cost_range": (200, 500)},
        ],
        "diagnosis_codes": [
            ("F32.9", "Major depressive disorder, single episode, unspecified"),
            ("F33.2", "Major depressive disorder, recurrent, severe"),
            ("F31.9", "Bipolar disorder, unspecified"),
            ("F41.1", "Generalized anxiety disorder"),
            ("F43.10", "Post-traumatic stress disorder, unspecified"),
            ("F20.9", "Schizophrenia, unspecified"),
        ],
        "clinical_criteria": [
            "For inpatient psychiatry: Active suicidal ideation with plan or intent (Columbia Suicide Severity Rating Scale score >= 4), OR acute psychosis with inability to maintain safety, OR severe self-harm requiring medical intervention, OR acute mania with dangerous behavior. Must meet medical necessity for 24-hour supervised care that cannot be safely managed at a lower level",
            "For residential treatment: Failed two or more outpatient treatment attempts (documented), AND GAD-7 >= 15 or PHQ-9 >= 20, AND functional impairment preventing work/school/ADLs, AND clinical assessment indicates need for structured therapeutic environment without acute medical necessity for inpatient",
            "For IOP: Step-down from inpatient/residential, OR PHQ-9 >= 15 or GAD-7 >= 10 with functional impairment, AND outpatient therapy alone insufficient (documented by treating provider). Standard authorization: 4 weeks (12-16 sessions); extension requires clinical update",
            "For ECT: Treatment-resistant depression (failed >= 2 adequate antidepressant trials from different classes, each >= 8 weeks at therapeutic dose), OR catatonia, OR severe depression with psychotic features, OR acute suicidality requiring rapid response. Up to 12 acute sessions; maintenance ECT requires separate authorization",
            "For TMS: Treatment-resistant depression (failed >= 2 antidepressant trials), AND PHQ-9 >= 14 at time of request, AND no history of seizure disorder, AND no metallic implants in/near head. Standard course: 30-36 sessions over 6-9 weeks",
        ],
        "step_therapy": [
            "Step 1: Outpatient psychotherapy (CBT, DBT, or evidence-based modality) — minimum 8 sessions",
            "Step 2: Pharmacotherapy — adequate trial of first-line antidepressant (SSRI or SNRI) at therapeutic dose for >= 8 weeks",
            "Step 3: Augmentation or switch — second antidepressant trial (different class) or augmentation with atypical antipsychotic or lithium for >= 6 weeks",
            "Step 4: TMS or ECT if Steps 1-3 fail (TMS preferred unless acute safety concern warrants ECT)",
        ],
        "required_documentation": [
            "PHQ-9 and/or GAD-7 scores within 30 days of request",
            "Columbia Suicide Severity Rating Scale (C-SSRS) for inpatient requests",
            "Current medication list with dosages, start dates, and documented response",
            "Therapy attendance records and treatment progress notes",
            "Safety assessment and risk stratification",
            "For TMS/ECT: Documentation of failed medication trials with dates, doses, and reasons for discontinuation",
        ],
        "exclusions": [
            "Inpatient admission for substance use disorder alone (see separate SUD policy)",
            "TMS for conditions other than major depressive disorder (off-label use not covered)",
            "Residential treatment exceeding 90 days without quarterly clinical review and continued medical necessity documentation",
            "ECT maintenance beyond monthly sessions without documented relapse prevention justification",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-005",
        "policy_name": "Specialty Pharmacy — Biologic and Targeted Immune Modulators",
        "service_category": "specialty_pharmacy",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-01",
        "purpose": (
            "This policy establishes prior authorization and step therapy requirements for biologic and targeted "
            "immune modulator medications including TNF inhibitors, IL-17 inhibitors, IL-23 inhibitors, and JAK "
            "inhibitors used in the treatment of autoimmune and inflammatory conditions for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "J0135", "system": "HCPCS", "description": "Adalimumab (Humira/biosimilar), 20mg injection", "cost_range": (2000, 6000)},
            {"code": "J1745", "system": "HCPCS", "description": "Infliximab (Remicade/biosimilar), 10mg IV", "cost_range": (3000, 8000)},
            {"code": "J3357", "system": "HCPCS", "description": "Ustekinumab (Stelara), 1mg IV", "cost_range": (10000, 25000)},
            {"code": "J3590", "system": "HCPCS", "description": "Secukinumab (Cosentyx), injection", "cost_range": (4000, 7000)},
            {"code": "J3262", "system": "HCPCS", "description": "Tocilizumab (Actemra), 1mg IV", "cost_range": (1500, 4000)},
        ],
        "diagnosis_codes": [
            ("M05.79", "Rheumatoid arthritis with rheumatoid factor, unspecified site"),
            ("L40.50", "Arthropathic psoriasis, unspecified"),
            ("K50.90", "Crohn's disease, unspecified, without complications"),
            ("K51.90", "Ulcerative colitis, unspecified, without complications"),
            ("M45.9", "Ankylosing spondylitis, unspecified"),
            ("L40.0", "Psoriasis vulgaris"),
        ],
        "clinical_criteria": [
            "For TNF inhibitors (adalimumab, infliximab, etanercept): Must have tried and failed (or have contraindication to) at least one conventional DMARD (methotrexate preferred, minimum 3 months at 15-25mg/week) for RA/PsA. For IBD: failed conventional therapy (5-ASA + corticosteroids + immunomodulator). Biosimilar preferred: must use FDA-approved biosimilar unless documented clinical failure or adverse reaction",
            "For IL-17 inhibitors (secukinumab, ixekizumab): Must have failed at least one TNF inhibitor, OR have contraindication to TNF inhibitors (active TB, demyelinating disease, heart failure NYHA III-IV). For psoriasis: BSA >= 10% or DLQI >= 10, AND failed topical therapy + phototherapy or conventional systemic",
            "For IL-23 inhibitors (guselkumab, risankizumab): Must have failed at least one TNF inhibitor AND one IL-17 inhibitor, OR have documented adverse reactions to both classes",
            "For JAK inhibitors (tofacitinib, upadacitinib, baricitinib): Must have failed at least one biologic (TNF or IL-17), AND have no history of VTE, malignancy within 5 years, or active serious infection. Black box warning discussion must be documented",
        ],
        "step_therapy": [
            "Step 1: Conventional therapy — methotrexate (RA/PsA), 5-ASA (IBD), topicals + phototherapy (psoriasis) — minimum 3 months",
            "Step 2: TNF inhibitor (biosimilar preferred) — adalimumab biosimilar or infliximab biosimilar as first biologic",
            "Step 3: If TNF failure — IL-17 inhibitor (secukinumab) or switch TNF agent",
            "Step 4: If IL-17 failure — IL-23 inhibitor (guselkumab, risankizumab) or JAK inhibitor (with risk discussion documented)",
        ],
        "required_documentation": [
            "Confirmed diagnosis with specialist documentation (rheumatologist, dermatologist, or gastroenterologist)",
            "Documentation of conventional therapy trial with dates, doses, and reason for discontinuation",
            "For RA: DAS28 or CDAI disease activity score",
            "For psoriasis: BSA and DLQI scores",
            "For IBD: Endoscopy or imaging showing active disease",
            "TB screening (QuantiFERON or PPD) within 12 months for all biologic starts",
            "Hepatitis B/C screening for all biologic starts",
            "For JAK inhibitors: Documented risk-benefit discussion re: black box warnings (VTE, malignancy, cardiovascular events)",
        ],
        "exclusions": [
            "Concurrent use of two biologic agents",
            "Brand biologic when FDA-approved biosimilar is available (unless documented biosimilar failure)",
            "JAK inhibitors as first-line biologic therapy (must fail at least one biologic first)",
            "Biologic therapy for non-FDA-approved indications without peer-reviewed literature support",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-006",
        "policy_name": "High-Cost Diagnostic Imaging — MRI, CT with Contrast, and PET/CT",
        "service_category": "diagnostic_imaging",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-05",
        "purpose": (
            "This policy establishes prior authorization criteria for high-cost diagnostic imaging studies "
            "including MRI, CT with contrast, and PET/CT scans for Red Bricks Insurance members. The policy "
            "ensures imaging is clinically appropriate while reducing unnecessary radiation exposure and cost."
        ),
        "covered_services": [
            {"code": "70553", "system": "CPT", "description": "MRI brain with and without contrast", "cost_range": (1000, 3000)},
            {"code": "72148", "system": "CPT", "description": "MRI lumbar spine without contrast", "cost_range": (500, 2000)},
            {"code": "74178", "system": "CPT", "description": "CT abdomen and pelvis with contrast", "cost_range": (600, 2000)},
            {"code": "71260", "system": "CPT", "description": "CT chest with contrast", "cost_range": (500, 1500)},
            {"code": "78816", "system": "CPT", "description": "PET/CT whole body imaging", "cost_range": (3000, 8000)},
            {"code": "77067", "system": "CPT", "description": "Screening mammography, bilateral", "cost_range": (150, 400)},
        ],
        "diagnosis_codes": [
            ("R51.9", "Headache, unspecified"),
            ("M54.5", "Low back pain"),
            ("R10.9", "Unspecified abdominal pain"),
            ("C34.90", "Malignant neoplasm unspecified bronchus or lung"),
            ("C50.919", "Malignant neoplasm unspecified female breast"),
            ("R91.1", "Solitary pulmonary nodule"),
        ],
        "clinical_criteria": [
            "For MRI brain: New-onset seizure, focal neurological deficit, suspected intracranial mass, persistent headache > 4 weeks unresponsive to treatment, or post-concussion with red flag symptoms. NOT authorized for: routine headache evaluation < 4 weeks, pre-employment screening",
            "For MRI spine: Radiculopathy or myelopathy symptoms > 4 weeks with failed conservative management, suspected spinal cord compression (urgent — no wait required), post-operative evaluation with new symptoms, or suspected infection/tumor. NOT authorized for: non-specific back pain < 6 weeks without red flags",
            "For CT with contrast (abdomen/pelvis/chest): Acute abdominal pain with concerning exam findings, suspected appendicitis/diverticulitis, staging or surveillance of known malignancy, pulmonary embolism evaluation (CT angiography), or suspected aortic pathology. NOT authorized for: routine screening, non-specific abdominal complaints without clinical findings",
            "For PET/CT: Staging of newly diagnosed malignancy (lung, lymphoma, melanoma, head/neck, esophageal), treatment response assessment after >= 2 cycles of chemotherapy, evaluation of suspected recurrence with elevated tumor markers or equivocal conventional imaging. NOT authorized for: screening, surveillance < 12 months from last PET without clinical indication",
            "For screening mammography: Covered per USPSTF guidelines without prior authorization. Diagnostic mammography and breast MRI for high-risk screening require authorization if outside standard guidelines",
        ],
        "step_therapy": [
            "Step 1: Appropriate initial workup — X-ray, ultrasound, or lab studies as clinically indicated",
            "Step 2: Conservative management trial (4-6 weeks) for non-urgent musculoskeletal or pain presentations",
            "Step 3: Advanced imaging (MRI, CT) if initial workup inconclusive and conservative measures failed",
            "Exception: Red flag symptoms (suspected cancer, cord compression, acute neurological deficit, trauma) — advanced imaging authorized immediately without step therapy",
        ],
        "required_documentation": [
            "Clinical indication with relevant history and physical exam findings",
            "Results of initial workup (X-ray, labs, ultrasound) if applicable",
            "Documentation of conservative management trial and outcome (for non-urgent requests)",
            "For PET/CT: Pathology report confirming malignancy diagnosis, treatment history, and specific clinical question",
            "Ordering provider must document why the requested study is the most appropriate modality",
        ],
        "exclusions": [
            "Whole-body MRI or CT screening without clinical indication",
            "Repeat imaging within 90 days for the same indication without documented clinical change",
            "PET/CT for prostate cancer staging (per NCCN guidelines, limited utility in low-risk disease)",
            "CT for uncomplicated headache or back pain without red flag symptoms",
        ],
    },
]


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

def _sanitize(text: str) -> str:
    """Replace Unicode characters that Helvetica (latin-1) can't encode."""
    return (
        text
        .replace("\u2014", "--")   # em dash
        .replace("\u2013", "-")    # en dash
        .replace("\u2018", "'")    # left single quote
        .replace("\u2019", "'")    # right single quote
        .replace("\u201c", '"')    # left double quote
        .replace("\u201d", '"')    # right double quote
        .replace("\u2022", "-")    # bullet
        .replace("\u2026", "...")  # ellipsis
        .replace("\u00a0", " ")    # non-breaking space
    )


class _PolicyPDF(FPDF):
    """Custom PDF with Red Bricks branding."""

    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(180, 30, 30)
        self.cell(0, 8, "RED BRICKS INSURANCE -- MEDICAL POLICY", align="L")
        self.cell(0, 8, "CONFIDENTIAL", align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(180, 30, 30)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_heading(self, title: str):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(30, 30, 100)
        self.cell(0, 8, _sanitize(title), new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def sub_heading(self, title: str):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(60, 60, 60)
        self.cell(0, 7, _sanitize(title), new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text: str):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5, _sanitize(text))
        self.ln(2)

    def bullet_list(self, items: list):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(40, 40, 40)
        for item in items:
            self.cell(5)
            self.multi_cell(0, 5, f"  *  {_sanitize(item)}")
            self.ln(1)
        self.ln(2)

    def code_table(self, headers: list, rows: list):
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(230, 230, 240)
        col_widths = [25, 15, 100, 25]
        if len(headers) == 2:
            col_widths = [30, 160]
        elif len(headers) == 3:
            col_widths = [25, 120, 45]
        for i, h in enumerate(headers):
            w = col_widths[i] if i < len(col_widths) else 40
            self.cell(w, 6, h, border=1, fill=True)
        self.ln()
        self.set_font("Helvetica", "", 8)
        self.set_fill_color(255, 255, 255)
        for row in rows:
            max_h = 6
            for i, val in enumerate(row):
                w = col_widths[i] if i < len(col_widths) else 40
                self.cell(w, max_h, _sanitize(str(val)[:60]), border=1)
            self.ln()
        self.ln(3)


def _build_policy_pdf(policy: dict) -> bytes:
    """Build a single policy PDF and return raw bytes."""
    pdf = _PolicyPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(180, 30, 30)
    pdf.multi_cell(0, 8, _sanitize(policy["policy_name"]))
    pdf.ln(3)

    # Meta info
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 5, f"Policy ID: {policy['policy_id']}    |    Effective: {policy['effective_date']}    |    Last Reviewed: {policy['last_reviewed']}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    # Purpose
    pdf.section_heading("1. Purpose and Scope")
    pdf.body_text(policy["purpose"])

    # Covered services
    pdf.section_heading("2. Covered Services")
    headers = ["Code", "System", "Description", "Est. Cost"]
    rows = []
    for svc in policy["covered_services"]:
        cost = f"${svc['cost_range'][0]:,}-${svc['cost_range'][1]:,}"
        rows.append([svc["code"], svc["system"], svc["description"][:55], cost])
    pdf.code_table(headers, rows)

    # Diagnosis codes
    pdf.section_heading("3. Applicable Diagnosis Codes")
    headers = ["ICD-10", "Description"]
    rows = [[dx[0], dx[1][:80]] for dx in policy["diagnosis_codes"]]
    pdf.code_table(headers, rows)

    # Clinical criteria
    pdf.section_heading("4. Clinical Criteria for Authorization")
    pdf.bullet_list(policy["clinical_criteria"])

    # Step therapy
    pdf.section_heading("5. Step Therapy Requirements")
    pdf.bullet_list(policy["step_therapy"])

    # Required documentation
    pdf.section_heading("6. Required Documentation")
    pdf.bullet_list(policy["required_documentation"])

    # Exclusions
    pdf.section_heading("7. Exclusions and Limitations")
    pdf.bullet_list(policy["exclusions"])

    # Appeal process (standard across all policies)
    pdf.section_heading("8. Appeal Process")
    pdf.body_text(
        "Members and providers have the right to appeal any prior authorization denial. "
        "Appeals must be submitted within 60 days of the denial notification. Expedited appeals "
        "for urgent clinical situations will be reviewed within 72 hours. Standard appeals are "
        "reviewed within 30 calendar days. All appeals are reviewed by a physician reviewer who "
        "was not involved in the original determination. The appeal must include: (a) a written "
        "request specifying the basis for the appeal, (b) any additional clinical documentation "
        "supporting medical necessity, and (c) the treating provider's letter of support. "
        "External review by an Independent Review Organization (IRO) is available after exhaustion "
        "of internal appeals."
    )

    # References
    pdf.section_heading("9. References")
    pdf.body_text(
        "Clinical criteria in this policy are based on nationally recognized guidelines including: "
        "AMA CPT coding guidelines, CMS National Coverage Determinations (NCDs), relevant medical "
        "society position statements, peer-reviewed clinical literature, and FDA-approved indications. "
        "This policy is reviewed annually and updated as clinical evidence evolves."
    )

    return pdf.output()


def generate_medical_policy_pdfs(output_dir: str) -> List[Dict[str, Any]]:
    """
    Generate all medical policy PDFs and write to output_dir.
    Returns list of metadata dicts for pipeline ingestion.
    """
    os.makedirs(output_dir, exist_ok=True)
    metadata = []

    for policy in POLICIES:
        pdf_bytes = _build_policy_pdf(policy)
        filename = f"{policy['policy_id']}.pdf"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "wb") as f:
            f.write(pdf_bytes)

        metadata.append({
            "policy_id": policy["policy_id"],
            "policy_name": policy["policy_name"],
            "service_category": policy["service_category"],
            "effective_date": policy["effective_date"],
            "last_reviewed": policy["last_reviewed"],
            "file_name": filename,
            "file_path": filepath,
            "num_covered_services": len(policy["covered_services"]),
            "num_criteria": len(policy["clinical_criteria"]),
            "num_step_therapy_steps": len(policy["step_therapy"]),
        })

    return metadata


def get_policy_rules_flat() -> List[Dict[str, Any]]:
    """
    Return flattened structured rules for all policies (for CSV/Parquet ingestion).
    Each row is one rule from one policy — used for the bronze_medical_policies table.
    """
    rules = []
    rule_counter = 0
    for policy in POLICIES:
        # Clinical criteria as rules
        for criterion in policy["clinical_criteria"]:
            rule_counter += 1
            rules.append({
                "policy_id": policy["policy_id"],
                "policy_name": policy["policy_name"],
                "service_category": policy["service_category"],
                "rule_id": f"RULE-{rule_counter:04d}",
                "rule_type": "clinical_criteria",
                "rule_text": criterion,
                "procedure_codes": "|".join(s["code"] for s in policy["covered_services"]),
                "diagnosis_codes": "|".join(dx[0] for dx in policy["diagnosis_codes"]),
                "effective_date": policy["effective_date"],
            })
        # Step therapy as rules
        for step in policy["step_therapy"]:
            rule_counter += 1
            rules.append({
                "policy_id": policy["policy_id"],
                "policy_name": policy["policy_name"],
                "service_category": policy["service_category"],
                "rule_id": f"RULE-{rule_counter:04d}",
                "rule_type": "step_therapy",
                "rule_text": step,
                "procedure_codes": "|".join(s["code"] for s in policy["covered_services"]),
                "diagnosis_codes": "|".join(dx[0] for dx in policy["diagnosis_codes"]),
                "effective_date": policy["effective_date"],
            })
    return rules

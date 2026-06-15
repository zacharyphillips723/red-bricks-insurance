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
    {
        "policy_id": "RBI-PA-2025-007",
        "policy_name": "Pulmonary Medicine — COPD Management, Nebulizers, and Pulmonary Rehabilitation",
        "service_category": "pulmonary",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-15",
        "purpose": (
            "This policy establishes prior authorization criteria for pulmonary services including "
            "home nebulizer therapy, pulmonary rehabilitation programs, oxygen therapy, and advanced "
            "diagnostics for chronic obstructive pulmonary disease (COPD) and asthma management."
        ),
        "covered_services": [
            {"code": "94060", "system": "CPT", "description": "Bronchodilator responsiveness testing (spirometry pre/post)", "cost_range": (100, 300)},
            {"code": "E0570", "system": "HCPCS", "description": "Home nebulizer with compressor", "cost_range": (150, 400)},
            {"code": "94625", "system": "CPT", "description": "Pulmonary rehabilitation, per session", "cost_range": (75, 200)},
            {"code": "E1390", "system": "HCPCS", "description": "Oxygen concentrator, single delivery", "cost_range": (200, 600)},
            {"code": "94726", "system": "CPT", "description": "Plethysmography (lung volumes)", "cost_range": (150, 400)},
        ],
        "diagnosis_codes": [
            ("J44.1", "COPD with acute exacerbation"),
            ("J44.0", "COPD with acute lower respiratory infection"),
            ("J45.20", "Mild intermittent asthma, uncomplicated"),
            ("J45.50", "Severe persistent asthma, uncomplicated"),
            ("J96.10", "Chronic respiratory failure, unspecified"),
        ],
        "clinical_criteria": [
            "For home nebulizer: Documented inability to use metered-dose inhaler (MDI) with spacer due to cognitive impairment, severe arthritis, or pediatric age < 5, OR FEV1 < 50% predicted with >= 2 exacerbations in past 12 months requiring systemic corticosteroids",
            "For pulmonary rehabilitation: FEV1 < 80% predicted despite optimal pharmacotherapy, AND functional dyspnea (mMRC grade >= 2), AND ability to participate in exercise program. Standard course: 36 sessions over 12 weeks",
            "For oxygen therapy: Resting PaO2 <= 55 mmHg or SpO2 <= 88% on room air, OR PaO2 56-59 mmHg with cor pulmonale, polycythemia (Hct > 55%), or pulmonary hypertension. Arterial blood gas or pulse oximetry must be documented during stable state (not during acute exacerbation)",
            "For spirometry: Indicated for initial COPD/asthma diagnosis, response to treatment assessment (no more than quarterly), pre-operative evaluation for thoracic surgery, or disability evaluation",
        ],
        "step_therapy": [
            "Step 1: Short-acting bronchodilator (albuterol MDI with spacer) for mild intermittent symptoms",
            "Step 2: Add long-acting bronchodilator (LABA or LAMA) for moderate persistent disease",
            "Step 3: ICS/LABA combination (fluticasone/salmeterol, budesonide/formoterol) if persistent symptoms",
            "Step 4: Add LAMA triple therapy or consider biologic (if eosinophilic phenotype with blood eos >= 300)",
        ],
        "required_documentation": [
            "Spirometry results (FEV1, FVC, FEV1/FVC ratio) within 12 months",
            "Current medication list with inhaler technique assessment",
            "For oxygen: ABG or pulse oximetry during stable state (not acute exacerbation)",
            "Smoking cessation counseling documentation (active smokers)",
            "For pulmonary rehab: Functional assessment (6-minute walk test or equivalent)",
        ],
        "exclusions": [
            "Nebulizer for patients who can effectively use MDI with spacer",
            "Oxygen therapy based solely on acute exacerbation measurements",
            "Pulmonary rehabilitation for patients unable to participate in exercise (e.g., bedbound)",
            "Repeat spirometry more than quarterly without clinical indication for change",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-008",
        "policy_name": "Gastroenterology — Endoscopy, Colonoscopy, and IBD Management",
        "service_category": "gastroenterology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-20",
        "purpose": (
            "This policy defines coverage and prior authorization requirements for gastroenterology "
            "procedures including upper and lower endoscopy, capsule endoscopy, and management of "
            "inflammatory bowel disease and gastroesophageal reflux disease."
        ),
        "covered_services": [
            {"code": "43239", "system": "CPT", "description": "Upper GI endoscopy (EGD) with biopsy", "cost_range": (1800, 5000)},
            {"code": "45380", "system": "CPT", "description": "Colonoscopy with biopsy", "cost_range": (1500, 4000)},
            {"code": "45378", "system": "CPT", "description": "Diagnostic colonoscopy", "cost_range": (1200, 3500)},
            {"code": "91110", "system": "CPT", "description": "Capsule endoscopy, small bowel", "cost_range": (1500, 4000)},
            {"code": "43257", "system": "CPT", "description": "Upper GI endoscopy with Bravo pH capsule", "cost_range": (2000, 5500)},
        ],
        "diagnosis_codes": [
            ("K21.0", "GERD with esophagitis"),
            ("K21.9", "GERD without esophagitis"),
            ("K50.90", "Crohn's disease, unspecified"),
            ("K51.90", "Ulcerative colitis, unspecified"),
            ("K57.30", "Diverticulosis of large intestine without hemorrhage"),
            ("K92.1", "Melena (GI bleeding)"),
        ],
        "clinical_criteria": [
            "For screening colonoscopy: Covered per USPSTF guidelines (age 45-75) without prior authorization. High-risk screening (family history of CRC in first-degree relative before age 60, personal history of adenomatous polyps, Lynch syndrome) may begin at age 40 or 10 years before youngest affected relative",
            "For diagnostic colonoscopy: Iron deficiency anemia with suspected GI source, positive FIT/FOBT, unexplained rectal bleeding, change in bowel habits > 6 weeks in patients > 50, surveillance for prior polyps per guideline intervals",
            "For EGD: Dysphagia, odynophagia, refractory GERD (failed 8 weeks PPI therapy), suspected Barrett's esophagus surveillance, upper GI bleeding, unexplained weight loss with upper GI symptoms",
            "For capsule endoscopy: Obscure GI bleeding (negative upper and lower endoscopy), suspected small bowel Crohn's disease, small bowel tumor surveillance in polyposis syndromes",
        ],
        "step_therapy": [
            "Step 1: For GERD — lifestyle modification + PPI trial (8 weeks at standard dose) before EGD",
            "Step 2: For persistent symptoms — dose escalation or switch PPI, add H2 blocker at bedtime",
            "Step 3: EGD if refractory to 8+ weeks optimal PPI therapy or alarm symptoms (dysphagia, weight loss, anemia)",
            "Step 4: Anti-reflux surgery (fundoplication) only after documented objective GERD (pH study or impedance) and failed medical management",
        ],
        "required_documentation": [
            "Indication for procedure with relevant symptoms and duration",
            "Prior endoscopy reports (if repeat procedure) with pathology results",
            "For GERD: Documentation of PPI trial duration, dose, and response",
            "For IBD surveillance: Disease extent, duration, and prior dysplasia findings",
            "Gastroenterologist consultation note",
        ],
        "exclusions": [
            "Screening colonoscopy before age 45 without high-risk indication",
            "EGD for uncomplicated GERD without PPI trial",
            "Repeat colonoscopy within guideline intervals without new indication",
            "Capsule endoscopy without prior standard upper and lower endoscopy",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-009",
        "policy_name": "Chronic Kidney Disease — Dialysis Access, Nephrology Management, and Renal Transplant Evaluation",
        "service_category": "nephrology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-10",
        "purpose": (
            "This policy establishes coverage criteria for nephrology services including dialysis access "
            "procedures, erythropoiesis-stimulating agents (ESAs), phosphate binders, and renal transplant "
            "evaluation for Red Bricks Insurance members with chronic kidney disease."
        ),
        "covered_services": [
            {"code": "36821", "system": "CPT", "description": "AV fistula creation for hemodialysis", "cost_range": (5000, 15000)},
            {"code": "90935", "system": "CPT", "description": "Hemodialysis, single treatment", "cost_range": (300, 800)},
            {"code": "90945", "system": "CPT", "description": "Peritoneal dialysis, single treatment", "cost_range": (200, 500)},
            {"code": "J0881", "system": "HCPCS", "description": "Darbepoetin alfa (Aranesp), 1mcg", "cost_range": (200, 600)},
            {"code": "50360", "system": "CPT", "description": "Renal transplant, allograft", "cost_range": (80000, 200000)},
        ],
        "diagnosis_codes": [
            ("N18.3", "Chronic kidney disease, stage 3"),
            ("N18.4", "Chronic kidney disease, stage 4"),
            ("N18.5", "Chronic kidney disease, stage 5"),
            ("N18.6", "End stage renal disease"),
            ("D63.1", "Anemia in chronic kidney disease"),
        ],
        "clinical_criteria": [
            "For dialysis access (AV fistula): eGFR < 20 mL/min or expected to start dialysis within 6 months, nephrology referral documenting CKD progression and dialysis planning. Fistula preferred over graft per KDOQI guidelines",
            "For ESAs: Hemoglobin < 10 g/dL in CKD stage 3-5 or ESRD, iron stores repleted (TSAT >= 20%, ferritin >= 100 ng/mL), and no active malignancy or uncontrolled hypertension. Target Hgb 10-11.5 g/dL — do not exceed 13 g/dL",
            "For transplant evaluation: eGFR < 20 mL/min or on dialysis, age-appropriate cancer screening current, cardiac clearance obtained, no active substance abuse, BMI < 40 (relative), and psychosocial evaluation completed",
            "For phosphate binders: Serum phosphorus > 5.5 mg/dL in CKD stage 3-5 or ESRD, dietary phosphorus restriction attempted",
        ],
        "step_therapy": [
            "Step 1: Dietary counseling (renal diet: low sodium, potassium, phosphorus restriction)",
            "Step 2: ACE inhibitor or ARB for proteinuria reduction (unless contraindicated)",
            "Step 3: Iron supplementation before ESA initiation (oral first, IV if oral intolerance or inadequate response)",
            "Step 4: ESA therapy if anemia persists despite iron repletion",
        ],
        "required_documentation": [
            "eGFR and serum creatinine trends over 6+ months",
            "Nephrology consultation note with CKD staging and management plan",
            "For ESAs: CBC, iron studies (TSAT, ferritin), reticulocyte count",
            "For transplant: Cardiac clearance, cancer screening, psychosocial eval, insurance verification",
            "Documentation of dietary counseling and medication compliance",
        ],
        "exclusions": [
            "ESA therapy with hemoglobin > 13 g/dL",
            "Dialysis access placement when eGFR > 25 mL/min without documented rapid decline",
            "Transplant evaluation for patients with active malignancy (except non-melanoma skin cancer)",
            "IV iron as first-line without trial of oral iron (unless documented GI intolerance)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-010",
        "policy_name": "Hypertension Management — Ambulatory Blood Pressure Monitoring and Combination Therapy",
        "service_category": "hypertension",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-15",
        "purpose": (
            "This policy defines coverage for hypertension management services including ambulatory "
            "blood pressure monitoring (ABPM), renal artery duplex ultrasound, and guidelines for "
            "antihypertensive medication management for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "93784", "system": "CPT", "description": "Ambulatory blood pressure monitoring, 24-hour", "cost_range": (150, 400)},
            {"code": "93786", "system": "CPT", "description": "ABPM recording and interpretation", "cost_range": (100, 300)},
            {"code": "93975", "system": "CPT", "description": "Duplex scan of renal arteries", "cost_range": (300, 800)},
            {"code": "99214", "system": "CPT", "description": "Office visit, established patient, level 4 (HTN management)", "cost_range": (140, 260)},
        ],
        "diagnosis_codes": [
            ("I10", "Essential (primary) hypertension"),
            ("I11.9", "Hypertensive heart disease without heart failure"),
            ("I12.9", "Hypertensive chronic kidney disease, stage 1-4"),
            ("I13.10", "Hypertensive heart and CKD without heart failure"),
        ],
        "clinical_criteria": [
            "For ABPM: Suspected white coat hypertension (office BP >= 140/90 but patient reports normal home readings), suspected masked hypertension, assessment of nocturnal dipping pattern, or evaluation of resistant hypertension (uncontrolled on 3+ agents including diuretic)",
            "For renal artery duplex: Resistant hypertension (on 3+ agents at optimal doses), new-onset hypertension age < 30 or > 55, asymmetric kidney size > 1.5cm on imaging, or flash pulmonary edema with hypertension",
            "For combination antihypertensive therapy: BP remains above goal (< 130/80 per ACC/AHA) despite 4+ weeks of monotherapy at adequate dose. Preferred combinations: ACE/ARB + CCB, ACE/ARB + thiazide diuretic",
        ],
        "step_therapy": [
            "Step 1: Lifestyle modification (DASH diet, sodium < 2300mg/day, exercise 150 min/week, weight management) for 3 months if Stage 1 HTN without ASCVD risk",
            "Step 2: Monotherapy — ACE inhibitor or ARB preferred (lisinopril, losartan). CCB (amlodipine) if ACE/ARB contraindicated",
            "Step 3: Add second agent from different class (ACE/ARB + CCB or ACE/ARB + thiazide) if BP not at goal after 4 weeks",
            "Step 4: Triple therapy or consider secondary causes workup (ABPM, renal artery imaging, aldosterone/renin ratio)",
        ],
        "required_documentation": [
            "Office BP readings (minimum 2 readings, separated by 1+ minutes) on 2+ visits",
            "Home blood pressure log (if available)",
            "Current antihypertensive regimen with doses and duration",
            "Renal function (BMP with creatinine, eGFR) within 12 months",
            "For ABPM: Clinical rationale for suspected white coat or masked HTN",
        ],
        "exclusions": [
            "ABPM as routine screening tool (only for specific clinical questions listed above)",
            "Renal artery imaging without documented resistant hypertension or clinical suspicion of renovascular disease",
            "Combination therapy as initial treatment without trial of monotherapy (exception: Stage 2 HTN with BP >= 160/100 may start dual therapy)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-011",
        "policy_name": "Lipid Management — PCSK9 Inhibitors and Advanced Lipid Testing",
        "service_category": "lipid_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-05",
        "purpose": (
            "This policy establishes prior authorization requirements for advanced lipid-lowering "
            "therapies including PCSK9 inhibitors (evolocumab, alirocumab), icosapent ethyl (Vascepa), "
            "bempedoic acid, and advanced lipid testing for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "PCSK9 inhibitor injection (evolocumab, alirocumab)", "cost_range": (400, 800)},
            {"code": "83704", "system": "CPT", "description": "Lipoprotein(a) quantitative", "cost_range": (50, 150)},
            {"code": "83721", "system": "CPT", "description": "LDL direct measurement", "cost_range": (30, 80)},
            {"code": "82172", "system": "CPT", "description": "Apolipoprotein B measurement", "cost_range": (40, 120)},
        ],
        "diagnosis_codes": [
            ("E78.5", "Hyperlipidemia, unspecified"),
            ("E78.01", "Familial hypercholesterolemia"),
            ("E78.2", "Mixed hyperlipidemia"),
            ("I25.10", "Atherosclerotic heart disease of native coronary artery"),
        ],
        "clinical_criteria": [
            "For PCSK9 inhibitors: LDL-C remains >= 70 mg/dL (ASCVD patients) or >= 100 mg/dL (primary prevention with FH) despite maximum tolerated statin + ezetimibe for >= 8 weeks each, OR documented statin intolerance (failed >= 2 statins at lowest dose, or rhabdomyolysis/CK > 10x ULN on any statin)",
            "For icosapent ethyl (Vascepa): Fasting triglycerides 150-499 mg/dL despite statin therapy, AND established ASCVD or diabetes with >= 2 additional risk factors",
            "For bempedoic acid: Statin intolerance (documented as above), LDL-C >= 70 mg/dL on ezetimibe alone",
            "For Lp(a) testing: First-degree relative with premature ASCVD (male < 55, female < 65), personal history of ASCVD without traditional risk factors, or familial hypercholesterolemia evaluation",
        ],
        "step_therapy": [
            "Step 1: High-intensity statin therapy (atorvastatin 40-80mg or rosuvastatin 20-40mg) — minimum 8 weeks",
            "Step 2: Add ezetimibe 10mg if LDL-C remains above goal — minimum 8 weeks",
            "Step 3: If LDL-C still above goal after Steps 1-2, approve PCSK9 inhibitor. If statin-intolerant, bempedoic acid + ezetimibe before PCSK9",
            "Step 4: PCSK9 inhibitor approved if bempedoic acid + ezetimibe insufficient and LDL-C remains >= 70 mg/dL (ASCVD) or >= 100 mg/dL (FH)",
        ],
        "required_documentation": [
            "Fasting lipid panel within 8 weeks showing current LDL-C level",
            "Documentation of statin therapy trial (drug, dose, duration, LDL response or reason for discontinuation)",
            "For statin intolerance: Documentation of >= 2 statin trials with adverse effects",
            "For PCSK9: Documentation of ezetimibe trial (or contraindication)",
            "10-year ASCVD risk score or documentation of clinical ASCVD",
        ],
        "exclusions": [
            "PCSK9 inhibitors as first-line therapy without statin/ezetimibe trial",
            "Advanced lipid testing (Lp(a), apoB, NMR) as routine screening without clinical indication",
            "Icosapent ethyl for triglycerides < 150 mg/dL or >= 500 mg/dL (different treatment pathway for severe hypertriglyceridemia)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-012",
        "policy_name": "Thyroid Disorders — Thyroid Ultrasound, Fine Needle Aspiration, and Thyroid Surgery",
        "service_category": "endocrinology_thyroid",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-01",
        "purpose": (
            "This policy defines coverage criteria for thyroid disorder evaluation and management "
            "including thyroid ultrasound, fine needle aspiration biopsy, thyroid function monitoring, "
            "and thyroid surgery for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "76536", "system": "CPT", "description": "Ultrasound, thyroid, with soft tissues of neck", "cost_range": (200, 600)},
            {"code": "60100", "system": "CPT", "description": "Fine needle aspiration biopsy, thyroid", "cost_range": (300, 900)},
            {"code": "60240", "system": "CPT", "description": "Thyroidectomy, total or subtotal", "cost_range": (8000, 20000)},
            {"code": "84443", "system": "CPT", "description": "TSH (thyroid stimulating hormone)", "cost_range": (20, 60)},
            {"code": "84436", "system": "CPT", "description": "Free thyroxine (Free T4)", "cost_range": (20, 60)},
        ],
        "diagnosis_codes": [
            ("E03.9", "Hypothyroidism, unspecified"),
            ("E05.90", "Thyrotoxicosis, unspecified"),
            ("E04.1", "Nontoxic single thyroid nodule"),
            ("C73", "Malignant neoplasm of thyroid gland"),
        ],
        "clinical_criteria": [
            "For thyroid ultrasound: Palpable thyroid nodule, abnormal thyroid on physical exam, incidental thyroid finding on other imaging, surveillance of known nodule per ATA guidelines, or evaluation of cervical lymphadenopathy with suspected thyroid origin",
            "For FNA biopsy: Thyroid nodule >= 1.0 cm with suspicious ultrasound features (hypoechoic, irregular margins, microcalcifications, taller-than-wide), or nodule >= 1.5 cm with intermediate features, or any size with highly suspicious features per ACR TI-RADS 4-5",
            "For thyroidectomy: Confirmed or suspected thyroid malignancy (Bethesda V-VI on FNA), symptomatic goiter causing compressive symptoms, or Graves disease refractory to anti-thyroid medications and not candidate for radioactive iodine",
            "For TSH/Free T4 monitoring: Initial diagnosis workup, dose adjustment monitoring (6-8 weeks after levothyroxine change), annual monitoring for stable hypothyroidism on levothyroxine",
        ],
        "step_therapy": [
            "Step 1: TSH screening for suspected thyroid dysfunction; if abnormal, add Free T4 and Free T3",
            "Step 2: Thyroid ultrasound if nodule palpated or TSH suppressed (evaluate for autonomous nodule)",
            "Step 3: FNA biopsy based on ultrasound risk stratification (ACR TI-RADS)",
            "Step 4: Surgery or radioactive iodine based on pathology and clinical presentation",
        ],
        "required_documentation": [
            "TSH and Free T4 results within 3 months",
            "Thyroid ultrasound report with TI-RADS classification",
            "For FNA: Ultrasound showing nodule size and features per TI-RADS criteria",
            "For surgery: FNA pathology report (Bethesda classification)",
            "Endocrinology or ENT consultation note",
        ],
        "exclusions": [
            "Thyroid ultrasound screening without clinical indication (palpable nodule, abnormal TSH, or incidental finding)",
            "FNA of nodules < 1.0 cm without highly suspicious features",
            "Repeat ultrasound within 12 months for TI-RADS 1-2 nodules",
            "Thyroidectomy for benign nodules without compressive symptoms",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-013",
        "policy_name": "Sleep Medicine — Polysomnography, CPAP Therapy, and Oral Appliances",
        "service_category": "sleep_medicine",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-15",
        "purpose": (
            "This policy establishes coverage criteria for sleep disorder diagnosis and treatment "
            "including polysomnography, home sleep apnea testing, CPAP/BiPAP therapy, and oral "
            "appliance therapy for obstructive sleep apnea."
        ),
        "covered_services": [
            {"code": "95810", "system": "CPT", "description": "Polysomnography, attended, with CPAP titration", "cost_range": (1500, 4000)},
            {"code": "95806", "system": "CPT", "description": "Home sleep apnea test (HSAT)", "cost_range": (200, 500)},
            {"code": "E0601", "system": "HCPCS", "description": "CPAP device", "cost_range": (500, 1500)},
            {"code": "E0470", "system": "HCPCS", "description": "BiPAP device without backup rate", "cost_range": (800, 2000)},
            {"code": "E0486", "system": "HCPCS", "description": "Oral appliance for sleep apnea", "cost_range": (1500, 3000)},
        ],
        "diagnosis_codes": [
            ("G47.33", "Obstructive sleep apnea"),
            ("G47.30", "Sleep apnea, unspecified"),
            ("G47.31", "Primary central sleep apnea"),
            ("R06.83", "Snoring"),
        ],
        "clinical_criteria": [
            "For HSAT: Suspected moderate-to-severe OSA in adults without significant comorbidities (no CHF, COPD, neuromuscular disease). Epworth Sleepiness Scale (ESS) >= 10 or STOP-Bang >= 3",
            "For in-lab polysomnography: Suspected OSA with significant cardiopulmonary comorbidity, suspected central sleep apnea, negative or inconclusive HSAT with high clinical suspicion, or pediatric patients (< 18 years)",
            "For CPAP: AHI >= 15 events/hour, OR AHI 5-14 with symptoms (excessive daytime sleepiness, impaired cognition) or comorbidities (hypertension, cardiovascular disease, stroke). Compliance review at 90 days: must demonstrate >= 4 hours use per night on >= 70% of nights",
            "For BiPAP: CPAP intolerance documented (unable to tolerate CPAP at adequate pressure >= 15 cmH2O, or complex sleep apnea, or obesity hypoventilation syndrome with BMI >= 35)",
            "For oral appliance: Mild-moderate OSA (AHI 5-30) who prefer oral appliance, OR CPAP intolerance documented",
        ],
        "step_therapy": [
            "Step 1: Clinical screening (ESS, STOP-Bang) and sleep medicine referral",
            "Step 2: Diagnostic testing — HSAT preferred; in-lab PSG if HSAT inconclusive or comorbidities",
            "Step 3: CPAP trial (minimum 90 days) for moderate-severe OSA",
            "Step 4: BiPAP or oral appliance only after documented CPAP failure/intolerance",
        ],
        "required_documentation": [
            "Sleep study report (HSAT or PSG) with AHI, oxygen desaturation index, and minimum SpO2",
            "Epworth Sleepiness Scale score",
            "For CPAP renewal: Compliance data download showing usage >= 4 hrs/night on >= 70% nights",
            "For BiPAP: Documentation of CPAP intolerance with pressures tried",
            "Sleep medicine consultation note",
        ],
        "exclusions": [
            "In-lab PSG as first-line for uncomplicated suspected OSA without comorbidities",
            "CPAP/BiPAP for snoring alone without documented AHI >= 5",
            "CPAP replacement within 5 years without documented device malfunction",
            "Oral appliance for severe OSA (AHI > 30) as first-line without CPAP trial",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-014",
        "policy_name": "Evaluation and Management — Office Visit Coding Standards and Medical Necessity",
        "service_category": "evaluation_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-10",
        "purpose": (
            "This policy establishes coding standards and medical necessity criteria for Evaluation "
            "and Management (E/M) services to ensure appropriate code-level selection. It defines "
            "expected documentation requirements for each E/M level and outlines the medical decision "
            "making (MDM) framework used for claims review and FWA detection."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit, established patient, low MDM complexity", "cost_range": (95, 180)},
            {"code": "99214", "system": "CPT", "description": "Office visit, established patient, moderate MDM complexity", "cost_range": (140, 260)},
            {"code": "99215", "system": "CPT", "description": "Office visit, established patient, high MDM complexity", "cost_range": (200, 380)},
            {"code": "99203", "system": "CPT", "description": "Office visit, new patient, low MDM complexity", "cost_range": (130, 250)},
            {"code": "99204", "system": "CPT", "description": "Office visit, new patient, moderate MDM complexity", "cost_range": (200, 350)},
            {"code": "99205", "system": "CPT", "description": "Office visit, new patient, high MDM complexity", "cost_range": (260, 450)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("Z23", "Encounter for immunization"),
            ("R10.9", "Unspecified abdominal pain"),
            ("R51.9", "Headache, unspecified"),
            ("M54.5", "Low back pain"),
        ],
        "clinical_criteria": [
            "99213 (Low complexity): Self-limited or minor problem (1+ conditions); minimal or no data to review; low risk of morbidity from diagnostic testing/treatment. Expected provider distribution: 40-50% of established office visits",
            "99214 (Moderate complexity): 1+ chronic illness with mild exacerbation, OR 2+ stable chronic conditions, OR 1 new undiagnosed problem with uncertain prognosis; moderate amount of data reviewed (ordering/reviewing tests, obtaining records); moderate risk of morbidity (prescription drug management). Expected provider distribution: 30-40% of established visits",
            "99215 (High complexity): 1+ chronic illness with severe exacerbation or side effects of treatment, OR 1 acute/chronic condition posing threat to life or bodily function; extensive data reviewed (independent interpretation of tests, discussion of management with external physician); high risk of morbidity (decision regarding hospitalization, drugs requiring intensive monitoring). Expected: 10-20% of established visits. Provider billing >= 40% at 99215 triggers review",
            "For new patient visits (99203-99205): Same MDM complexity thresholds as above. New patient visits inherently involve more data gathering; code level reflects MDM complexity, not time spent",
            "Modifier 25 (significant, separately identifiable E/M service): May be appended only when a significant and separately identifiable E/M service is performed on the same day as a minor procedure. The E/M must be above and beyond the usual pre/post-operative care of the procedure. Documentation must clearly show two distinct services",
        ],
        "step_therapy": [
            "Step 1: Provider selects E/M code based on medical decision making (MDM) complexity per AMA 2021 guidelines",
            "Step 2: Documentation must support the MDM level selected — number/complexity of problems, data reviewed, risk of management",
            "Step 3: Claims with code levels consistently above specialty benchmarks (>= 2 standard deviations) are flagged for retrospective review",
            "Step 4: Providers with > 40% of E/M visits at 99215 level will receive educational outreach; persistent outliers referred for focused audit",
        ],
        "required_documentation": [
            "Chief complaint and clinical indication for the visit",
            "Number and complexity of problems addressed (with status updates for chronic conditions)",
            "Data reviewed: labs, imaging, records obtained, and interpretation",
            "Risk assessment: medications prescribed, procedures planned, diagnoses considered",
            "Time-based billing (if applicable): total time on date of encounter, with activities documented",
        ],
        "exclusions": [
            "Billing 99215 for routine follow-up of stable chronic conditions (e.g., stable HTN medication refill with no changes)",
            "Modifier 25 with a minor procedure unless documentation clearly supports a separately identifiable E/M service",
            "Billing new patient codes (99203-99205) for patients seen within the past 3 years by the same provider or same specialty in the same group",
            "Level 5 coding for encounters under 40 minutes without documented high-complexity MDM",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-015",
        "policy_name": "Preventive Care — Annual Wellness Visits, Cancer Screenings, and Immunizations",
        "service_category": "preventive_care",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-01",
        "purpose": (
            "This policy defines coverage for preventive care services covered without prior authorization "
            "under the Affordable Care Act and USPSTF guidelines, including annual wellness visits, cancer "
            "screenings, immunizations, and routine laboratory testing."
        ),
        "covered_services": [
            {"code": "99395", "system": "CPT", "description": "Preventive visit, established patient, 18-39 years", "cost_range": (150, 280)},
            {"code": "99396", "system": "CPT", "description": "Preventive visit, established patient, 40-64 years", "cost_range": (165, 310)},
            {"code": "99397", "system": "CPT", "description": "Preventive visit, established patient, 65+ years", "cost_range": (175, 330)},
            {"code": "77067", "system": "CPT", "description": "Screening mammography, bilateral", "cost_range": (150, 400)},
            {"code": "45378", "system": "CPT", "description": "Screening colonoscopy", "cost_range": (1200, 3500)},
            {"code": "90471", "system": "CPT", "description": "Immunization administration", "cost_range": (15, 40)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("Z00.01", "Adult medical examination with abnormal findings"),
            ("Z23", "Encounter for immunization"),
            ("Z12.31", "Encounter for screening mammogram"),
            ("Z12.11", "Encounter for screening for malignant neoplasm of colon"),
        ],
        "clinical_criteria": [
            "Annual wellness visit: Covered once per calendar year for all members. Includes health risk assessment, review of functional ability and safety, detection of cognitive impairment, personalized prevention plan, and screening schedule review",
            "Screening mammography: Covered annually for women age 40+ per ACS guidelines, or biannually age 50-74 per USPSTF. High-risk screening (BRCA carriers, chest radiation history) may begin at age 30 with breast MRI",
            "Screening colonoscopy: Covered beginning at age 45, every 10 years for average risk. FIT/FOBT annually is an acceptable alternative. High-risk screening at earlier age and shorter intervals per gastroenterologist recommendation",
            "Immunizations: All ACIP-recommended vaccines covered without cost-sharing — influenza (annual), COVID-19, Tdap, zoster (age 50+), pneumococcal (age 65+ or high-risk), HPV (through age 26)",
            "Routine labs: Lipid panel (every 5 years age 20+, annually if on statin), fasting glucose or HbA1c (every 3 years age 35+, annually if prediabetes), CBC and CMP per provider clinical judgment during wellness visit",
        ],
        "step_therapy": [
            "No step therapy required for preventive care services — covered per ACA mandates without prior authorization",
            "If abnormal findings are identified during preventive visit, diagnostic workup follows standard clinical pathways and may require authorization per applicable disease-specific policies",
            "Preventive visit coding: Bill preventive code (99395-99397) for the wellness visit. If a separately identifiable problem is addressed, an E/M code with modifier 25 may be billed additionally",
        ],
        "required_documentation": [
            "Age-appropriate screening schedule reviewed and documented",
            "Health risk assessment completed",
            "Current immunization status reviewed and updated",
            "For screening colonoscopy: Risk assessment (average vs. high-risk) documented",
            "Shared decision-making documented for lung cancer and prostate cancer screening",
        ],
        "exclusions": [
            "More than one annual wellness visit per calendar year",
            "Diagnostic testing during preventive visit billed as preventive (diagnostic tests require separate authorization per policy)",
            "Screening services not recommended by USPSTF (e.g., PSA screening without shared decision-making documentation)",
            "Non-ACIP-recommended vaccines (travel vaccines billed separately)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-016",
        "policy_name": "Clinical Laboratory Services — Comprehensive Metabolic Panel, CBC, and Urinalysis Standards",
        "service_category": "laboratory",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-07-20",
        "purpose": (
            "This policy establishes medical necessity and frequency guidelines for routine and specialty "
            "clinical laboratory testing including comprehensive metabolic panels, complete blood counts, "
            "urinalysis, and common chemistry panels ordered in outpatient settings."
        ),
        "covered_services": [
            {"code": "80053", "system": "CPT", "description": "Comprehensive metabolic panel (CMP)", "cost_range": (15, 45)},
            {"code": "85025", "system": "CPT", "description": "Complete blood count (CBC) with differential", "cost_range": (12, 35)},
            {"code": "81001", "system": "CPT", "description": "Urinalysis, automated with microscopy", "cost_range": (8, 20)},
            {"code": "36415", "system": "CPT", "description": "Venipuncture for specimen collection", "cost_range": (10, 25)},
            {"code": "83036", "system": "CPT", "description": "Hemoglobin A1c (HbA1c)", "cost_range": (20, 60)},
            {"code": "80061", "system": "CPT", "description": "Lipid panel (total cholesterol, HDL, LDL, triglycerides)", "cost_range": (20, 50)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("E78.5", "Hyperlipidemia, unspecified"),
            ("I10", "Essential (primary) hypertension"),
            ("N18.3", "Chronic kidney disease, stage 3"),
        ],
        "clinical_criteria": [
            "CMP: Medically necessary for initial evaluation of metabolic disorders, monitoring patients on medications affecting liver/renal function (statins, ACE inhibitors, diuretics, metformin), CKD monitoring, diabetes management, and pre-operative assessment. Maximum frequency: quarterly for chronic disease monitoring, annually for routine screening",
            "CBC with differential: Indicated for evaluation of anemia, infection, bleeding disorders, monitoring patients on immunosuppressants or chemotherapy, and pre-operative assessment. Maximum frequency: quarterly for stable chronic conditions, more frequent for active treatment monitoring",
            "Urinalysis: Annual screening for diabetes and CKD patients, evaluation of UTI symptoms, pre-operative assessment, and proteinuria monitoring in CKD",
            "HbA1c: Every 3 months for diabetic patients not at goal, every 6 months for stable diabetic patients at goal, once for prediabetes screening in at-risk adults",
            "Lipid panel: Every 5 years for average-risk adults, annually for patients on statin therapy, 4-12 weeks after statin initiation or dose change",
        ],
        "step_therapy": [
            "Step 1: Order targeted testing based on clinical indication — avoid panel ordering when individual tests suffice",
            "Step 2: Review prior results before re-ordering — duplicate testing within guideline intervals is not covered",
            "Step 3: Specialty panels (autoimmune, coagulation, tumor markers) require clinical justification beyond routine screening",
        ],
        "required_documentation": [
            "Clinical indication for each test ordered (diagnosis code must support medical necessity)",
            "Date of most recent prior test of same type (to verify frequency compliance)",
            "For specialty panels: Clinical rationale beyond routine screening",
        ],
        "exclusions": [
            "CMP at every office visit without clinical indication (e.g., routine follow-up for stable conditions tested within 3 months)",
            "Duplicate testing within guideline frequency intervals without documented clinical change",
            "Standing lab orders without periodic clinical reassessment (maximum standing order duration: 12 months)",
            "Screening panels for asymptomatic patients beyond USPSTF-recommended intervals",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-017",
        "policy_name": "Emergency Department Services — Triage, Observation, and Prudent Layperson Standard",
        "service_category": "emergency_services",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-20",
        "purpose": (
            "This policy defines coverage for emergency department services including ED visit levels, "
            "observation stays, and the prudent layperson standard that governs ED visit coverage "
            "regardless of final diagnosis. It also addresses appropriate ED utilization and avoidable ED visits."
        ),
        "covered_services": [
            {"code": "99281", "system": "CPT", "description": "ED visit, level 1 — self-limited or minor problem", "cost_range": (80, 250)},
            {"code": "99282", "system": "CPT", "description": "ED visit, level 2 — low to moderate severity", "cost_range": (150, 450)},
            {"code": "99283", "system": "CPT", "description": "ED visit, level 3 — moderate severity", "cost_range": (250, 750)},
            {"code": "99284", "system": "CPT", "description": "ED visit, level 4 — high severity, urgent evaluation", "cost_range": (450, 1300)},
            {"code": "99285", "system": "CPT", "description": "ED visit, level 5 — high severity, immediate threat to life", "cost_range": (700, 2200)},
            {"code": "99234", "system": "CPT", "description": "Observation care same-day admit and discharge", "cost_range": (400, 1000)},
        ],
        "diagnosis_codes": [
            ("R10.9", "Unspecified abdominal pain"),
            ("R07.9", "Chest pain, unspecified"),
            ("S72.001A", "Fracture of neck of right femur, initial"),
            ("I21.9", "Acute myocardial infarction, unspecified"),
            ("J18.9", "Pneumonia, unspecified organism"),
        ],
        "clinical_criteria": [
            "Prudent layperson standard: ED visits are covered when a prudent layperson with average knowledge of health and medicine would reasonably believe that the symptoms require emergency care to prevent serious jeopardy to health, serious impairment to bodily functions, or serious dysfunction of any body part. Coverage applies regardless of the final diagnosis",
            "ED visit level selection must reflect acuity: Level 1-2 for minor/low-severity presentations, Level 3 for moderate, Level 4-5 for high severity. Provider billing > 70% at Level 4-5 triggers utilization review",
            "Observation status: Appropriate for patients requiring 6-24 hours of monitoring to determine need for inpatient admission. Common indications: chest pain rule-out, asthma exacerbation, syncope evaluation, abdominal pain observation",
            "Avoidable ED utilization: Members with >= 4 ED visits in 12 months for non-emergent conditions will be referred to care management for PCP access and chronic disease management coordination",
        ],
        "step_therapy": [
            "No prior authorization required for emergency services — covered under the prudent layperson standard",
            "Post-stabilization: Once the emergency condition is stabilized, prior authorization may be required for continued inpatient stay or transfer to specialized facility",
            "Non-emergent ED use: Members are encouraged to use urgent care, telehealth, or PCP for non-emergent conditions. Lower cost-sharing for urgent care visits vs. ED",
        ],
        "required_documentation": [
            "Triage assessment with presenting complaint and vital signs",
            "Medical screening examination (MSE) per EMTALA requirements",
            "ED level selection supported by MDM complexity (number of diagnoses, data reviewed, risk)",
            "For observation: Documented clinical decision to observe with expected observation period and criteria for admission vs. discharge",
        ],
        "exclusions": [
            "Retrospective denial of ED visit based on final diagnosis alone (prohibited by prudent layperson standard in most states)",
            "Observation stays exceeding 48 hours without conversion to inpatient admission (requires clinical review)",
            "Routine or scheduled testing in the ED (lab draws, imaging for non-emergent indications)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-018",
        "policy_name": "General Surgery — Appendectomy and Cholecystectomy Authorization Standards",
        "service_category": "general_surgery",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-30",
        "purpose": (
            "This policy establishes prior authorization and medical necessity criteria for common "
            "general surgical procedures including appendectomy, cholecystectomy, and hernia repair "
            "for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "44950", "system": "CPT", "description": "Appendectomy, open", "cost_range": (5000, 15000)},
            {"code": "44970", "system": "CPT", "description": "Appendectomy, laparoscopic", "cost_range": (6000, 18000)},
            {"code": "47562", "system": "CPT", "description": "Laparoscopic cholecystectomy", "cost_range": (5000, 15000)},
            {"code": "47563", "system": "CPT", "description": "Laparoscopic cholecystectomy with cholangiography", "cost_range": (6000, 18000)},
            {"code": "49505", "system": "CPT", "description": "Inguinal hernia repair, initial", "cost_range": (3000, 10000)},
        ],
        "diagnosis_codes": [
            ("K35.80", "Acute appendicitis, unspecified"),
            ("K80.20", "Calculus of gallbladder without cholecystitis"),
            ("K80.00", "Calculus of gallbladder with acute cholecystitis"),
            ("K40.90", "Unilateral inguinal hernia without obstruction"),
            ("K80.10", "Calculus of gallbladder with chronic cholecystitis"),
        ],
        "clinical_criteria": [
            "For appendectomy (emergent): Acute appendicitis confirmed by clinical presentation (RLQ pain, fever, elevated WBC) and/or imaging (CT with appendiceal diameter > 6mm, periappendiceal fat stranding). No prior authorization required for emergent surgery",
            "For cholecystectomy (elective): Symptomatic cholelithiasis with >= 2 episodes of biliary colic, OR complicated gallstone disease (cholecystitis, pancreatitis, choledocholithiasis), OR gallbladder polyp >= 10mm. Laparoscopic approach preferred unless contraindicated",
            "For cholecystectomy (emergent): Acute cholecystitis — no prior authorization required. Surgery within 72 hours of admission preferred per Tokyo Guidelines",
            "For hernia repair: Symptomatic inguinal hernia (pain, enlarging, or risk of incarceration), OR incarcerated/strangulated hernia (emergent — no PA required). Watchful waiting acceptable for asymptomatic reducible hernias in elderly or high surgical risk patients",
        ],
        "step_therapy": [
            "Step 1: For biliary colic — dietary modification (low-fat diet), symptom management with analgesics",
            "Step 2: If recurrent biliary colic (>= 2 episodes) or complicated disease — surgical consultation",
            "Step 3: Cholecystectomy authorized if surgical criteria met. Same-day or 23-hour observation preferred for uncomplicated laparoscopic cholecystectomy",
            "Exception: Emergent presentations (acute appendicitis, acute cholecystitis, incarcerated hernia) bypass step therapy",
        ],
        "required_documentation": [
            "Imaging results (CT for appendicitis, ultrasound for gallstones)",
            "Laboratory findings (WBC, LFTs, lipase/amylase as applicable)",
            "Documentation of symptom episodes for elective cholecystectomy",
            "Surgical consultation note with planned approach (laparoscopic vs. open)",
            "For hernia: Physical exam documenting hernia type, reducibility, and symptoms",
        ],
        "exclusions": [
            "Prophylactic cholecystectomy for asymptomatic gallstones (unless concurrent with other abdominal surgery)",
            "Elective appendectomy without acute or recurrent appendicitis",
            "Hernia repair for asymptomatic, reducible inguinal hernia without patient preference for surgery",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-019",
        "policy_name": "Obstetric Services — Prenatal Care, Delivery, and Postpartum Coverage",
        "service_category": "obstetrics",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-30",
        "purpose": (
            "This policy defines coverage for obstetric services including prenatal care, labor and "
            "delivery, cesarean section, and postpartum care for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "59400", "system": "CPT", "description": "Routine obstetric care, vaginal delivery (global)", "cost_range": (3000, 8000)},
            {"code": "59510", "system": "CPT", "description": "Routine obstetric care, cesarean delivery (global)", "cost_range": (5000, 15000)},
            {"code": "59025", "system": "CPT", "description": "Fetal non-stress test (NST)", "cost_range": (100, 300)},
            {"code": "76801", "system": "CPT", "description": "Obstetric ultrasound, first trimester", "cost_range": (200, 600)},
            {"code": "76805", "system": "CPT", "description": "Obstetric ultrasound, detailed anatomy (18-22 weeks)", "cost_range": (300, 800)},
        ],
        "diagnosis_codes": [
            ("O80", "Encounter for full-term uncomplicated delivery"),
            ("O82", "Encounter for cesarean delivery without indication"),
            ("O24.419", "Gestational diabetes mellitus in pregnancy, unspecified"),
            ("O13.9", "Gestational hypertension, unspecified trimester"),
            ("O36.5990", "Maternal care for other known or suspected poor fetal growth"),
        ],
        "clinical_criteria": [
            "Prenatal care (global package): Covered for all pregnancies. Includes initial and subsequent history, physical exams, recording of weight/BP/fetal heart tones, routine labs, and all antepartum care per ACOG guidelines. No prior authorization required",
            "For cesarean delivery: Medically indicated for malpresentation (breech), placenta previa, prior classical cesarean, failed trial of labor after cesarean (TOLAC), fetal distress, cord prolapse, or obstructed labor. Elective primary cesarean without medical indication is not covered under standard benefits",
            "For additional ultrasounds beyond standard schedule: Medically indicated for growth restriction, placenta previa, multiple gestation, or high-risk pregnancy monitoring. Standard schedule: first trimester dating scan + 18-22 week anatomy scan",
            "For fetal NST: Indicated for decreased fetal movement, gestational diabetes on insulin, hypertensive disorders, post-dates (>= 41 weeks), or IUGR. Typically weekly or biweekly in third trimester for high-risk pregnancies",
        ],
        "step_therapy": [
            "Standard prenatal care pathway per ACOG guidelines — no step therapy required for routine obstetric care",
            "For gestational diabetes: Nutritional counseling and glucose monitoring first; insulin therapy if glucose targets not met after 2 weeks of dietary management",
            "For gestational hypertension: Increased monitoring frequency, antihypertensives if BP >= 160/110; delivery at 37 weeks if severe features",
        ],
        "required_documentation": [
            "Estimated date of delivery (EDD) with dating criteria",
            "Prenatal labs (blood type, Rh, CBC, UA, GBS screening, glucose challenge)",
            "For cesarean: Documented medical indication per ACOG criteria",
            "For additional ultrasounds: Clinical indication beyond standard screening",
            "Postpartum visit documentation within 3-8 weeks of delivery",
        ],
        "exclusions": [
            "Elective primary cesarean delivery without documented medical indication",
            "3D/4D ultrasound for non-medical purposes (keepsake imaging)",
            "Elective induction before 39 weeks without medical indication per ACOG guidelines",
            "Doula services (not covered under standard medical benefits; may be covered under supplemental wellness benefit)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-020",
        "policy_name": "Ophthalmology — Cataract Surgery, Glaucoma Management, and Retinal Services",
        "service_category": "ophthalmology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-05",
        "purpose": (
            "This policy establishes prior authorization criteria for ophthalmology services including "
            "cataract surgery with intraocular lens implantation, glaucoma management, and retinal "
            "procedures for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "66984", "system": "CPT", "description": "Cataract surgery with IOL implantation", "cost_range": (2500, 6000)},
            {"code": "66821", "system": "CPT", "description": "YAG laser capsulotomy, after cataract surgery", "cost_range": (400, 1000)},
            {"code": "65855", "system": "CPT", "description": "Laser trabeculoplasty (SLT) for glaucoma", "cost_range": (500, 1500)},
            {"code": "67228", "system": "CPT", "description": "Retinal laser photocoagulation (PRP or focal)", "cost_range": (800, 2500)},
            {"code": "67028", "system": "CPT", "description": "Intravitreal injection (anti-VEGF)", "cost_range": (1500, 3000)},
        ],
        "diagnosis_codes": [
            ("H25.10", "Age-related nuclear cataract, unspecified eye"),
            ("H40.11", "Primary open-angle glaucoma"),
            ("H35.30", "Unspecified macular degeneration"),
            ("E11.311", "Type 2 DM with unspecified diabetic retinopathy"),
            ("H33.001", "Retinal detachment with retinal break, right eye"),
        ],
        "clinical_criteria": [
            "For cataract surgery: Best-corrected visual acuity (BCVA) <= 20/50 in the operative eye, OR visually significant cataract causing functional impairment despite optimal refraction (documented glare, contrast sensitivity loss, or difficulty with ADLs). No minimum acuity threshold if functional impairment documented",
            "For glaucoma laser (SLT): First-line or adjunct therapy for open-angle glaucoma with IOP above target despite or in lieu of topical medications. May be repeated once if initial response was >= 20% IOP reduction but effect has waned",
            "For anti-VEGF injections: Wet age-related macular degeneration, diabetic macular edema, retinal vein occlusion with macular edema, or macular edema secondary to uveitis. Treat-and-extend protocol preferred over fixed-interval dosing",
            "For retinal laser: Proliferative diabetic retinopathy, retinal tears with risk of detachment, or focal/grid laser for diabetic macular edema not responsive to anti-VEGF",
        ],
        "step_therapy": [
            "Step 1: For cataract — updated refraction to confirm cataract is the primary visual limitant (not uncorrected refractive error)",
            "Step 2: For glaucoma — topical medication trial (prostaglandin analog preferred) OR SLT as initial therapy (both acceptable per AAO Preferred Practice Pattern)",
            "Step 3: For DME — anti-VEGF injection series (initial 3-6 monthly injections) before considering focal laser",
            "Step 4: Surgical intervention (trabeculectomy, MIGS for glaucoma; vitrectomy for advanced retinal disease) if conservative/laser measures insufficient",
        ],
        "required_documentation": [
            "Visual acuity (BCVA) and refraction",
            "Slit-lamp exam findings documenting cataract grade or other pathology",
            "For glaucoma: IOP measurements, visual field testing, OCT RNFL thickness",
            "For retinal injections: OCT showing macular edema thickness, fluorescein angiography if applicable",
            "Ophthalmologist consultation note with treatment plan",
        ],
        "exclusions": [
            "Premium IOL upgrade (multifocal, toric, accommodating) — standard monofocal IOL covered; premium upgrade is patient responsibility",
            "Cataract surgery for visual acuity > 20/40 without documented functional impairment",
            "Anti-VEGF for dry AMD (no approved anti-VEGF indication for non-neovascular AMD)",
            "Cosmetic blepharoplasty billed as medically necessary without documented visual field obstruction",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-021",
        "policy_name": "Oncology — Chemotherapy, Radiation Therapy, and PET/CT Surveillance",
        "service_category": "oncology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-05",
        "purpose": (
            "This policy establishes prior authorization criteria for oncology services including "
            "chemotherapy regimens, radiation therapy, immunotherapy, PET/CT surveillance imaging, "
            "and genetic counseling for Red Bricks Insurance members with confirmed malignancies."
        ),
        "covered_services": [
            {"code": "96413", "system": "CPT", "description": "Chemotherapy IV infusion, first hour", "cost_range": (500, 2000)},
            {"code": "96415", "system": "CPT", "description": "Chemotherapy IV infusion, each additional hour", "cost_range": (200, 800)},
            {"code": "77385", "system": "CPT", "description": "IMRT delivery, simple", "cost_range": (500, 1500)},
            {"code": "77386", "system": "CPT", "description": "IMRT delivery, complex", "cost_range": (800, 2500)},
            {"code": "78816", "system": "CPT", "description": "PET/CT whole body for oncologic restaging", "cost_range": (3000, 8000)},
            {"code": "96401", "system": "CPT", "description": "Chemotherapy SC/IM injection, non-hormonal", "cost_range": (100, 500)},
        ],
        "diagnosis_codes": [
            ("C50.919", "Malignant neoplasm unspecified female breast"),
            ("C34.90", "Malignant neoplasm unspecified bronchus or lung"),
            ("C18.9", "Malignant neoplasm of colon, unspecified"),
            ("C61", "Malignant neoplasm of prostate"),
            ("C83.30", "Diffuse large B-cell lymphoma, unspecified site"),
        ],
        "clinical_criteria": [
            "Chemotherapy authorization requires: confirmed tissue diagnosis (pathology report), staging evaluation complete (imaging + labs), treatment plan consistent with NCCN guidelines for the cancer type and stage, and ECOG performance status 0-2 (or documentation of why treatment is appropriate for PS 3)",
            "Radiation therapy: Treatment plan from board-certified radiation oncologist, simulation CT/MRI completed, dose-volume constraints documented. IMRT preferred over 3D conformal for head/neck, prostate, and select thoracic tumors where OAR sparing required",
            "Immunotherapy (pembrolizumab, nivolumab, atezolizumab): PD-L1 testing (IHC or CPS score) required for NSCLC and bladder cancer. MSI-H/dMMR testing for colorectal and other solid tumors. Companion diagnostic result must be documented",
            "PET/CT surveillance: After completion of curative-intent therapy, PET/CT covered per NCCN surveillance schedule (typically every 6-12 months for 2-3 years). Not indicated for routine surveillance of treated prostate cancer (PSA monitoring preferred)",
        ],
        "step_therapy": [
            "Step 1: Tissue diagnosis with pathology confirmation and molecular/genomic testing where applicable (EGFR, ALK, ROS1 for NSCLC; HER2 for breast; KRAS/BRAF for CRC)",
            "Step 2: Staging with appropriate imaging (CT, PET/CT, MRI as indicated by cancer type)",
            "Step 3: Multidisciplinary tumor board review for complex cases (recommended, not required)",
            "Step 4: Treatment per NCCN guidelines — first-line regimen must be attempted before second-line authorization",
        ],
        "required_documentation": [
            "Pathology report with histologic diagnosis and relevant biomarkers",
            "Staging evaluation (TNM staging with imaging results)",
            "NCCN guideline-concordant treatment plan",
            "ECOG performance status",
            "For immunotherapy: PD-L1 or MSI testing results",
            "Prior treatment history for second-line and beyond requests",
        ],
        "exclusions": [
            "Chemotherapy for ECOG performance status 4 without documented palliative intent and patient/family goals-of-care discussion",
            "PET/CT surveillance beyond NCCN-recommended intervals without clinical indication for recurrence",
            "Proton beam therapy when IMRT achieves equivalent outcomes (exceptions: pediatric tumors, base of skull, ocular melanoma)",
            "Off-label chemotherapy regimens without peer-reviewed clinical evidence or NCCN compendia listing",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-022",
        "policy_name": "Urology — Benign Prostatic Hyperplasia, Prostate Cancer Screening, and Urologic Procedures",
        "service_category": "urology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-10",
        "purpose": (
            "This policy establishes coverage criteria for urologic services including BPH management, "
            "prostate cancer screening, cystoscopy, and minimally invasive urologic procedures."
        ),
        "covered_services": [
            {"code": "52000", "system": "CPT", "description": "Cystoscopy, diagnostic", "cost_range": (500, 1500)},
            {"code": "52601", "system": "CPT", "description": "TURP (transurethral resection of prostate)", "cost_range": (5000, 15000)},
            {"code": "55700", "system": "CPT", "description": "Prostate biopsy, needle, transrectal", "cost_range": (800, 2500)},
            {"code": "84153", "system": "CPT", "description": "PSA (prostate-specific antigen)", "cost_range": (30, 80)},
            {"code": "52441", "system": "CPT", "description": "UroLift (prostatic urethral lift)", "cost_range": (3000, 8000)},
        ],
        "diagnosis_codes": [
            ("N40.0", "Benign prostatic hyperplasia without lower urinary tract symptoms"),
            ("N40.1", "BPH with lower urinary tract symptoms"),
            ("C61", "Malignant neoplasm of prostate"),
            ("R33.8", "Other retention of urine"),
            ("N30.00", "Acute cystitis without hematuria"),
        ],
        "clinical_criteria": [
            "For BPH surgical intervention (TURP, UroLift, Rezum): Failed medical therapy (alpha-blocker + 5-alpha reductase inhibitor) for >= 3 months, AND IPSS >= 8 (moderate-severe symptoms), AND documented impact on quality of life. Prostate size assessment (TRUS or MRI) required for procedure selection",
            "For PSA screening: Shared decision-making required per USPSTF. Covered annually for men 55-69 who elect screening after informed discussion. Men 40-54 with high risk (African American, family history) may begin earlier. Not recommended for men 70+ or < 10-year life expectancy",
            "For prostate biopsy: PSA >= 4.0 ng/mL (or >= 2.5 ng/mL with high-risk factors), OR abnormal digital rectal exam, OR rising PSA velocity > 0.75 ng/mL/year. MRI-guided biopsy preferred over systematic biopsy when available",
            "For cystoscopy: Hematuria evaluation (gross or persistent microscopic), recurrent UTIs (>= 3 in 12 months), lower urinary tract symptoms with suspected structural abnormality, or bladder cancer surveillance",
        ],
        "step_therapy": [
            "Step 1: For BPH — behavioral management (fluid restriction, bladder training) + alpha-blocker (tamsulosin) for 4+ weeks",
            "Step 2: Add 5-alpha reductase inhibitor (finasteride) for prostate > 30g or inadequate alpha-blocker response",
            "Step 3: If dual medical therapy fails after 3+ months — minimally invasive procedure (UroLift for prostate 30-80g, Rezum) before TURP",
            "Step 4: TURP or surgical enucleation for large prostates (> 80g) or failed minimally invasive procedures",
        ],
        "required_documentation": [
            "IPSS score and quality of life assessment",
            "Prostate size measurement (TRUS, MRI, or digital estimate)",
            "PSA level within 6 months",
            "Medication trial documentation with doses and duration",
            "Urologist consultation note",
        ],
        "exclusions": [
            "PSA screening for men 70+ without shared decision-making documentation",
            "TURP as first-line treatment without trial of medical therapy (unless acute urinary retention or recurrent UTIs due to BPH)",
            "Repeat cystoscopy within 6 months without new clinical indication",
            "Robotic prostatectomy as first intervention for low-risk prostate cancer when active surveillance is appropriate",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-023",
        "policy_name": "Dermatology — Biologic Therapy for Psoriasis, Skin Biopsy, and Phototherapy",
        "service_category": "dermatology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-25",
        "purpose": (
            "This policy defines coverage for dermatology services including biologic therapy for "
            "moderate-to-severe psoriasis, skin biopsy, phototherapy, and Mohs micrographic surgery."
        ),
        "covered_services": [
            {"code": "96920", "system": "CPT", "description": "Phototherapy (NB-UVB), per session", "cost_range": (50, 150)},
            {"code": "11102", "system": "CPT", "description": "Skin biopsy, tangential (shave)", "cost_range": (100, 300)},
            {"code": "11104", "system": "CPT", "description": "Skin biopsy, punch", "cost_range": (150, 400)},
            {"code": "17311", "system": "CPT", "description": "Mohs micrographic surgery, first stage", "cost_range": (800, 2000)},
            {"code": "J3590", "system": "HCPCS", "description": "Biologic injection for psoriasis (secukinumab, guselkumab)", "cost_range": (4000, 8000)},
        ],
        "diagnosis_codes": [
            ("L40.0", "Psoriasis vulgaris"),
            ("L40.50", "Arthropathic psoriasis, unspecified"),
            ("C44.319", "Basal cell carcinoma of skin of other parts of face"),
            ("L20.9", "Atopic dermatitis, unspecified"),
            ("D22.9", "Melanocytic nevi, unspecified"),
        ],
        "clinical_criteria": [
            "For biologic therapy (psoriasis): BSA >= 10% OR DLQI >= 10, AND failed topical therapy (corticosteroids + vitamin D analog) for >= 8 weeks, AND failed phototherapy or conventional systemic (methotrexate, cyclosporine, acitretin) for >= 3 months. Biosimilar preferred as first biologic",
            "For phototherapy (NB-UVB): Moderate psoriasis (BSA 3-10%) or atopic dermatitis not controlled with topicals. Standard course: 3 sessions/week for 8-12 weeks (24-36 sessions). Home phototherapy unit may be authorized after demonstrating compliance with in-office treatments",
            "For Mohs surgery: Indicated for BCC or SCC in high-risk anatomic locations (face, ears, hands, feet, genitalia), recurrent tumors, tumors with aggressive histology (morpheaform, perineural invasion), or tumors with ill-defined clinical borders. Standard excision preferred for low-risk tumors in non-critical locations",
            "For skin biopsy: Clinically indicated for suspected malignancy (changing mole, non-healing lesion), inflammatory dermatosis not responding to empiric treatment, or vesiculobullous disease requiring direct immunofluorescence",
        ],
        "step_therapy": [
            "Step 1: Topical therapy — corticosteroids, calcipotriene, tazarotene for 8+ weeks",
            "Step 2: Phototherapy (NB-UVB preferred) or conventional systemic (methotrexate for PsA, acitretin for pustular psoriasis)",
            "Step 3: Biologic therapy if Steps 1-2 fail. Start with TNF inhibitor biosimilar (adalimumab biosimilar) or IL-17 inhibitor",
            "Step 4: IL-23 inhibitor or JAK inhibitor if TNF and IL-17 inhibitors fail",
        ],
        "required_documentation": [
            "BSA calculation and DLQI score (for biologic requests)",
            "Photographic documentation of disease extent",
            "Documentation of topical therapy trials with duration and response",
            "For Mohs: Biopsy-proven diagnosis with histologic subtype, tumor location, and size",
            "Dermatologist consultation note",
        ],
        "exclusions": [
            "Biologic therapy for mild psoriasis (BSA < 3%) without documented special circumstances (face, genitals, nails affecting function)",
            "Mohs surgery for low-risk BCC on trunk or extremities where standard excision is appropriate",
            "Cosmetic dermatology procedures (laser resurfacing, chemical peels for cosmetic purposes)",
            "Phototherapy beyond 36 sessions without documented continued response",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-024",
        "policy_name": "Pain Management — Epidural Injections, Nerve Blocks, and Opioid Prescribing Standards",
        "service_category": "pain_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-25",
        "purpose": (
            "This policy establishes prior authorization criteria for interventional pain management "
            "procedures and opioid prescribing guidelines to ensure appropriate pain management while "
            "mitigating opioid-related risks for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "62323", "system": "CPT", "description": "Lumbar epidural steroid injection", "cost_range": (700, 2500)},
            {"code": "64483", "system": "CPT", "description": "Transforaminal epidural injection, lumbar", "cost_range": (800, 2800)},
            {"code": "64493", "system": "CPT", "description": "Facet joint nerve block, lumbar", "cost_range": (600, 2000)},
            {"code": "20610", "system": "CPT", "description": "Joint injection, major joint", "cost_range": (200, 600)},
            {"code": "64635", "system": "CPT", "description": "Radiofrequency ablation, facet nerve, lumbar", "cost_range": (1500, 4000)},
        ],
        "diagnosis_codes": [
            ("M54.5", "Low back pain"),
            ("M54.41", "Lumbago with sciatica, right side"),
            ("M47.816", "Spondylosis without myelopathy, lumbar region"),
            ("G89.29", "Other chronic pain"),
            ("M51.16", "Intervertebral disc disorders with radiculopathy, lumbar"),
        ],
        "clinical_criteria": [
            "For epidural steroid injections: Radicular pain correlating with imaging findings (MRI showing disc herniation or spinal stenosis), AND failed >= 4 weeks conservative therapy (physical therapy, NSAIDs, activity modification). Maximum: 3 injections per region per 12 months. Image guidance (fluoroscopy or CT) required for all spinal injections",
            "For facet joint injections/nerve blocks: Axial spine pain with facet loading signs on exam, AND failed conservative therapy >= 4 weeks. Diagnostic blocks must precede radiofrequency ablation (>= 80% pain relief from diagnostic block required before RFA authorization)",
            "For radiofrequency ablation: Documented >= 80% pain relief from diagnostic medial branch block, pain duration >= 3 months, and imaging ruling out surgical pathology. May be repeated once per region per 12 months if initial procedure provided >= 6 months relief",
            "Opioid prescribing: Acute pain limited to 7-day supply (post-surgical: 14-day). Chronic opioid therapy (> 90 days) requires pain management agreement, urine drug screening every 6 months, PDMP check at each prescribing, and functional goals documentation. Maximum MME: 90 MME/day without prior authorization; > 90 MME requires pain specialist consultation",
        ],
        "step_therapy": [
            "Step 1: Non-pharmacologic therapy — physical therapy, exercise, cognitive behavioral therapy for pain, weight management",
            "Step 2: Non-opioid medications — NSAIDs, acetaminophen, duloxetine, gabapentin as appropriate for pain type",
            "Step 3: Interventional procedures — epidural or facet injections if imaging correlates with symptoms",
            "Step 4: Opioid therapy only when Steps 1-3 insufficient; start low dose, titrate slowly, with functional goals",
        ],
        "required_documentation": [
            "Pain assessment (NRS score, functional impact, treatment history)",
            "MRI or CT showing pathology correlating with pain distribution",
            "Physical therapy documentation (sessions attended, response)",
            "For opioid prescribing: Pain agreement, PDMP check, urine drug screen",
            "For RFA: Diagnostic block results showing >= 80% pain relief",
        ],
        "exclusions": [
            "Epidural injections without correlating imaging findings",
            "More than 3 epidural injections per region per 12 months",
            "RFA without prior diagnostic medial branch block showing >= 80% relief",
            "Opioid prescribing > 90 MME/day without pain specialist involvement",
            "Trigger point injections > 4 sessions per month without documented improvement",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-025",
        "policy_name": "Physical Therapy and Rehabilitation — Outpatient PT/OT/ST Standards",
        "service_category": "rehabilitation",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-20",
        "purpose": (
            "This policy defines coverage and utilization management criteria for outpatient physical "
            "therapy, occupational therapy, and speech therapy services for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "97110", "system": "CPT", "description": "Therapeutic exercises, each 15 min", "cost_range": (30, 80)},
            {"code": "97140", "system": "CPT", "description": "Manual therapy techniques, each 15 min", "cost_range": (30, 80)},
            {"code": "97530", "system": "CPT", "description": "Therapeutic activities, each 15 min", "cost_range": (30, 80)},
            {"code": "97161", "system": "CPT", "description": "PT evaluation, low complexity", "cost_range": (80, 200)},
            {"code": "97163", "system": "CPT", "description": "PT evaluation, high complexity", "cost_range": (100, 280)},
            {"code": "92507", "system": "CPT", "description": "Speech-language pathology treatment", "cost_range": (80, 200)},
        ],
        "diagnosis_codes": [
            ("M54.5", "Low back pain"),
            ("M17.11", "Primary osteoarthritis, right knee"),
            ("S83.511A", "Sprain of ACL of right knee, initial"),
            ("I63.9", "Cerebral infarction, unspecified (post-stroke rehab)"),
            ("M75.10", "Rotator cuff tear, unspecified shoulder"),
        ],
        "clinical_criteria": [
            "Initial authorization: 12 visits over 8 weeks for most musculoskeletal conditions. Treatment plan must include measurable functional goals, baseline measurements, and expected discharge criteria",
            "Extension authorization: Additional visits approved if documented progress toward functional goals (>= 10% improvement on validated outcome measure), ongoing medical necessity, and updated treatment plan. Maximum 36 visits per condition per calendar year without medical director review",
            "Post-surgical rehabilitation: 12-24 visits depending on procedure (TKR: 18-24, rotator cuff repair: 18-24, ACL reconstruction: 24-36, spinal fusion: 12-18). Begins within 2 weeks of surgery or as directed by surgeon",
            "Speech therapy: Covered for documented speech/language/swallowing disorder secondary to medical condition (stroke, TBI, head/neck cancer, developmental delay). Cognitive rehabilitation for TBI covered with documented functional deficits",
        ],
        "step_therapy": [
            "Step 1: Initial PT evaluation with functional baseline assessment and goal-setting",
            "Step 2: Active treatment phase (therapeutic exercises, manual therapy) — progress reassessed every 4-6 visits",
            "Step 3: If plateau reached (< 10% improvement over 4 consecutive visits), discharge from skilled PT with home exercise program",
            "Step 4: Maintenance therapy (patient can perform independently) is not covered as skilled PT",
        ],
        "required_documentation": [
            "PT evaluation with diagnosis, functional limitations, and measurable goals",
            "Progress notes every 10 visits or 30 days (whichever is sooner)",
            "Validated outcome measures (DASH, LEFS, ODI, or equivalent) at baseline and reassessment",
            "Discharge summary with goals met/not met and home exercise program",
        ],
        "exclusions": [
            "Maintenance therapy that does not require skilled PT intervention (can be performed independently with HEP)",
            "PT for general conditioning or fitness without specific medical diagnosis",
            "More than 2 therapy disciplines concurrently without documented medical necessity for each",
            "Therapy visits exceeding 36 per condition per year without medical director authorization",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-026",
        "policy_name": "Durable Medical Equipment — Wheelchairs, Walkers, Hospital Beds, and Supply Standards",
        "service_category": "durable_medical_equipment",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-25",
        "purpose": (
            "This policy establishes prior authorization criteria for durable medical equipment (DME) "
            "including wheelchairs, walkers, hospital beds, and orthotic/prosthetic devices to ensure "
            "appropriate utilization and prevent DME fraud."
        ),
        "covered_services": [
            {"code": "K0001", "system": "HCPCS", "description": "Standard wheelchair", "cost_range": (200, 600)},
            {"code": "K0823", "system": "HCPCS", "description": "Power wheelchair, Group 2, standard", "cost_range": (3000, 8000)},
            {"code": "E0143", "system": "HCPCS", "description": "Walker, folding, wheeled, with seat", "cost_range": (80, 250)},
            {"code": "E0260", "system": "HCPCS", "description": "Hospital bed, semi-electric", "cost_range": (1000, 3000)},
            {"code": "L1843", "system": "HCPCS", "description": "Knee orthosis (KO), knee brace", "cost_range": (200, 800)},
        ],
        "diagnosis_codes": [
            ("M62.81", "Muscle weakness (generalized)"),
            ("G20", "Parkinson disease"),
            ("G35", "Multiple sclerosis"),
            ("I69.351", "Hemiplegia following cerebral infarction"),
            ("M17.11", "Primary osteoarthritis, right knee"),
        ],
        "clinical_criteria": [
            "For manual wheelchair: Mobility limitation in the home that prevents safe ambulation, AND member cannot use a cane/walker safely, AND has sufficient upper body strength to self-propel. Face-to-face examination by treating provider required within 45 days before order",
            "For power wheelchair: Meets manual wheelchair criteria AND cannot self-propel manual wheelchair due to upper extremity impairment, neurological condition, or cardiopulmonary limitation. In-home assessment by PT/OT recommended. Must demonstrate ability to operate safely",
            "For hospital bed: Medical condition requiring positioning that cannot be achieved in a regular bed (e.g., congestive heart failure requiring elevation > 30 degrees, traction, body casts, or repositioning for pressure ulcer prevention). Must be used in member's home",
            "For knee brace: Post-surgical protection (ACL repair, meniscal repair), ligament instability documented on exam, or degenerative joint disease with valgus/varus instability. Off-the-shelf preferred; custom only if off-the-shelf does not fit or is insufficient",
            "DME supplier must be enrolled with Red Bricks Insurance and meet DMEPOS supplier standards. Competitive bidding applies for standard items",
        ],
        "step_therapy": [
            "Step 1: Trial of less costly alternatives (cane before walker, walker before wheelchair)",
            "Step 2: Standard/off-the-shelf equipment before custom equipment",
            "Step 3: Manual wheelchair before power wheelchair (if upper body function adequate)",
            "Step 4: Rental before purchase for items expected to be needed < 13 months",
        ],
        "required_documentation": [
            "Face-to-face examination within 45 days of order (by treating physician, NP, or PA)",
            "Detailed written order (DWO) signed by prescribing provider",
            "Documentation of mobility limitation in the home setting",
            "For power wheelchair: PT/OT in-home assessment documenting functional need and ability to operate",
            "For hospital bed: Clinical justification for positioning beyond standard bed capabilities",
        ],
        "exclusions": [
            "Power wheelchair when manual wheelchair is appropriate and member can self-propel",
            "DME for convenience rather than medical necessity",
            "Duplicate DME items (e.g., wheelchair + scooter for same member without distinct clinical need)",
            "Premium/upgraded equipment beyond what is medically necessary (member pays upgrade difference)",
            "DME suppliers not enrolled with Red Bricks Insurance (must be contracted DMEPOS supplier)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-027",
        "policy_name": "Telehealth Services — Virtual Visit Coverage and Billing Standards",
        "service_category": "telehealth",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-15",
        "purpose": (
            "This policy establishes coverage for telehealth services including synchronous audio-video "
            "visits, audio-only visits, remote patient monitoring, and asynchronous store-and-forward "
            "consultations for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Telehealth office visit, established patient, level 3 (modifier 95)", "cost_range": (95, 180)},
            {"code": "99214", "system": "CPT", "description": "Telehealth office visit, established patient, level 4 (modifier 95)", "cost_range": (140, 260)},
            {"code": "90837", "system": "CPT", "description": "Telehealth psychotherapy, 60 min (modifier 95)", "cost_range": (120, 250)},
            {"code": "99457", "system": "CPT", "description": "Remote physiologic monitoring, 20 min clinical staff", "cost_range": (50, 120)},
            {"code": "99458", "system": "CPT", "description": "RPM, each additional 20 min", "cost_range": (40, 100)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("F32.9", "Major depressive disorder, single episode"),
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("I10", "Essential (primary) hypertension"),
            ("J45.20", "Mild intermittent asthma, uncomplicated"),
        ],
        "clinical_criteria": [
            "Synchronous audio-video visits: Covered at payment parity with in-person visits for all clinical services where the provider determines the visit can be safely conducted via telehealth. Modifier 95 required on claims. Provider must be licensed in the state where the patient is located",
            "Audio-only visits: Covered for established patients when audio-video is not available due to technology barriers. Limited to E/M levels 99212-99213 and behavioral health services. Modifier 93 required",
            "Remote patient monitoring (RPM): Covered for chronic conditions requiring frequent monitoring (diabetes, CHF, COPD, hypertension). Requires FDA-cleared monitoring device, minimum 16 days of data transmission per 30-day period, and clinical review documented",
            "Store-and-forward: Covered for dermatology, ophthalmology, and radiology consultations where asynchronous review of images/data is clinically appropriate",
        ],
        "step_therapy": [
            "No step therapy required — telehealth is a modality, not a distinct service. Same medical necessity criteria apply as for in-person visits",
            "For RPM: Initial in-person or telehealth visit required to establish patient-provider relationship and prescribe monitoring device before RPM billing begins",
        ],
        "required_documentation": [
            "Documentation that visit was conducted via telehealth (modality documented in note)",
            "Informed consent for telehealth on file",
            "For audio-only: Documentation of why audio-video was not feasible",
            "For RPM: Device prescribed, data transmission log, clinical review of transmitted data",
        ],
        "exclusions": [
            "Telehealth for services requiring physical examination that cannot be deferred (e.g., suspected fracture, acute abdomen)",
            "Audio-only visits at E/M level 99214-99215 (requires audio-video for higher complexity)",
            "RPM with fewer than 16 days of data transmission in a 30-day period",
            "Telehealth visits from providers not licensed in the patient's state of residence at time of service",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-028",
        "policy_name": "Substance Use Disorder — Detoxification, MAT, and Residential Treatment",
        "service_category": "substance_use",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-30",
        "purpose": (
            "This policy establishes coverage criteria for substance use disorder (SUD) treatment "
            "including medical detoxification, medication-assisted treatment (MAT), intensive outpatient "
            "programs, and residential treatment facilities."
        ),
        "covered_services": [
            {"code": "H0010", "system": "HCPCS", "description": "SUD services, sub-acute detoxification, per diem", "cost_range": (500, 1500)},
            {"code": "H0020", "system": "HCPCS", "description": "Alcohol and/or drug services, methadone administration", "cost_range": (15, 40)},
            {"code": "J2315", "system": "HCPCS", "description": "Naltrexone injection (Vivitrol), per injection", "cost_range": (1200, 1800)},
            {"code": "H0015", "system": "HCPCS", "description": "SUD intensive outpatient program, per diem", "cost_range": (200, 500)},
            {"code": "H2036", "system": "HCPCS", "description": "SUD residential treatment, per diem", "cost_range": (400, 1200)},
        ],
        "diagnosis_codes": [
            ("F10.20", "Alcohol dependence, uncomplicated"),
            ("F11.20", "Opioid dependence, uncomplicated"),
            ("F14.20", "Cocaine dependence, uncomplicated"),
            ("F15.20", "Other stimulant dependence, uncomplicated"),
            ("F19.20", "Other psychoactive substance dependence, uncomplicated"),
        ],
        "clinical_criteria": [
            "For medical detoxification: CIWA-Ar >= 10 (alcohol) or COWS >= 13 (opioid), OR history of withdrawal seizures/delirium tremens, OR hemodynamic instability during withdrawal. Inpatient detox for high-risk; outpatient for mild-moderate withdrawal",
            "For MAT (buprenorphine, methadone, naltrexone): First-line treatment for opioid use disorder per SAMHSA guidelines. No prior authorization required for buprenorphine induction. Methadone through licensed OTP only. Naltrexone (Vivitrol) covered for opioid and alcohol use disorders after complete detoxification",
            "For IOP: Minimum 9 hours/week of structured programming. Indicated as step-down from residential/inpatient or when outpatient alone insufficient. ASAM criteria Level 2.1",
            "For residential treatment: Meets ASAM criteria Level 3.1-3.5. Failed lower levels of care, OR imminent risk to self/others, OR unstable living environment undermining recovery. Initial authorization: 30 days; extensions in 14-day increments with clinical update",
        ],
        "step_therapy": [
            "Step 1: Assessment using ASAM criteria for appropriate level of care placement",
            "Step 2: Outpatient treatment + MAT (for opioid use disorder) as first-line unless ASAM criteria indicate higher level needed",
            "Step 3: IOP or partial hospitalization if outpatient insufficient",
            "Step 4: Residential treatment only when ASAM criteria indicate Level 3+ need",
        ],
        "required_documentation": [
            "ASAM criteria multidimensional assessment",
            "Substance use history and current use pattern",
            "Prior treatment history and outcomes",
            "For detox: CIWA-Ar or COWS scores",
            "For residential: Documentation of failed lower level of care or imminent safety risk",
            "Urine drug screen at admission",
        ],
        "exclusions": [
            "Prior authorization as barrier to MAT initiation (buprenorphine induction covered without PA per Mental Health Parity)",
            "Residential treatment exceeding 90 days without quarterly clinical review",
            "Detoxification without connection to ongoing SUD treatment (detox alone is insufficient)",
            "SUD treatment at facilities not licensed or accredited by state and/or CARF/Joint Commission",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-029",
        "policy_name": "Anticoagulation Therapy — DOACs, Warfarin Monitoring, and Bridging Standards",
        "service_category": "anticoagulation",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-30",
        "purpose": (
            "This policy defines coverage for anticoagulation therapy including direct oral anticoagulants "
            "(DOACs), warfarin management with INR monitoring, and periprocedural bridging anticoagulation."
        ),
        "covered_services": [
            {"code": "85610", "system": "CPT", "description": "Prothrombin time (PT/INR)", "cost_range": (10, 30)},
            {"code": "93793", "system": "CPT", "description": "Anticoagulation management, per month (warfarin)", "cost_range": (20, 50)},
            {"code": "J1644", "system": "HCPCS", "description": "Heparin sodium injection, per 1000 units", "cost_range": (5, 20)},
            {"code": "99211", "system": "CPT", "description": "INR clinic visit (nurse-led)", "cost_range": (20, 50)},
        ],
        "diagnosis_codes": [
            ("I48.91", "Atrial fibrillation, unspecified"),
            ("I26.99", "Other pulmonary embolism without acute cor pulmonale"),
            ("I82.401", "Acute DVT of unspecified deep vein of right lower extremity"),
            ("Z79.01", "Long-term current use of anticoagulants"),
        ],
        "clinical_criteria": [
            "For DOAC therapy (Eliquis, Xarelto, Pradaxa): First-line for non-valvular atrial fibrillation (CHA2DS2-VASc >= 2 in men, >= 3 in women), acute VTE treatment and secondary prevention, and post-orthopedic thromboprophylaxis. Eliquis (apixaban) is preferred formulary agent due to safety profile",
            "For warfarin: Indicated for mechanical heart valves (DOACs contraindicated), severe renal impairment (CrCl < 15 mL/min for most DOACs), antiphospholipid syndrome, or patient preference with documented INR monitoring compliance",
            "INR monitoring: Weekly during initiation and dose adjustments, then every 4 weeks for stable patients. Home INR testing (CoaguChek) covered for patients on chronic warfarin with demonstrated competency",
            "Bridging anticoagulation: LMWH bridging for high-risk patients (mechanical valve, recent VTE < 3 months, CHA2DS2-VASc >= 7) undergoing procedures requiring warfarin interruption. DOACs do not require bridging (stop 2-3 days pre-procedure, resume 1-2 days post-procedure)",
        ],
        "step_therapy": [
            "Step 1: Risk assessment (CHA2DS2-VASc for AF, Wells score for VTE)",
            "Step 2: DOAC preferred over warfarin for most indications (except mechanical valves, severe CKD, APS)",
            "Step 3: Preferred DOAC: apixaban (Eliquis). Alternative: rivaroxaban if once-daily dosing preferred",
            "Step 4: Warfarin with structured INR monitoring program if DOAC contraindicated",
        ],
        "required_documentation": [
            "Indication for anticoagulation with supporting diagnosis",
            "CHA2DS2-VASc score (for AF) or Wells score / imaging (for VTE)",
            "Renal function (CrCl/eGFR) within 6 months — required for DOAC dosing",
            "For warfarin: INR monitoring frequency and time-in-therapeutic-range (TTR)",
            "Bleeding risk assessment (HAS-BLED score)",
        ],
        "exclusions": [
            "DOAC therapy for mechanical heart valves (warfarin required)",
            "Brand-name DOAC when therapeutic generic equivalent is available",
            "INR monitoring more frequently than weekly for stable patients on warfarin",
            "Bridging anticoagulation for low-risk patients (CHA2DS2-VASc <= 4, no recent VTE)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-030",
        "policy_name": "Antihypertensive Medications — ACE Inhibitors, ARBs, and Calcium Channel Blockers",
        "service_category": "antihypertensive_pharmacy",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-07-15",
        "purpose": (
            "This policy defines the formulary tier structure, step therapy requirements, and quantity "
            "limits for antihypertensive medications including ACE inhibitors, angiotensin receptor "
            "blockers, and calcium channel blockers."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Lisinopril oral tablet (generic ACE inhibitor)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Losartan oral tablet (generic ARB)", "cost_range": (8, 25)},
            {"code": "J3490", "system": "HCPCS", "description": "Amlodipine oral tablet (generic CCB)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Hydrochlorothiazide oral tablet (generic thiazide)", "cost_range": (4, 10)},
        ],
        "diagnosis_codes": [
            ("I10", "Essential (primary) hypertension"),
            ("I11.9", "Hypertensive heart disease without heart failure"),
            ("N18.3", "Chronic kidney disease, stage 3 (with proteinuria)"),
            ("E11.9", "Type 2 diabetes mellitus (nephroprotection)"),
        ],
        "clinical_criteria": [
            "ACE inhibitors (lisinopril, enalapril, ramipril): First-line for hypertension with diabetes, CKD with proteinuria, heart failure with reduced EF, or post-MI. Generic required; brand-name only with documented generic allergy/intolerance",
            "ARBs (losartan, valsartan, irbesartan): Second-line when ACE inhibitor not tolerated (cough, angioedema). Generic required. Do NOT use ACE + ARB concurrently (dual RAAS blockade increases adverse events)",
            "CCBs (amlodipine, nifedipine ER): First-line for hypertension in African American patients per JNC8, or add-on to ACE/ARB. Amlodipine preferred generic. Non-dihydropyridine CCBs (diltiazem, verapamil) for rate control in AF",
            "Thiazide diuretics (HCTZ, chlorthalidone): First-line per ALLHAT data. Chlorthalidone preferred over HCTZ for superior 24-hour BP control (ALLHAT, SPRINT). Generic required",
        ],
        "step_therapy": [
            "Step 1: Generic ACE inhibitor (lisinopril 10-40mg) or generic thiazide (chlorthalidone 12.5-25mg) as monotherapy",
            "Step 2: If not at goal after 4 weeks, add second agent from different class (ACE + CCB or ACE + thiazide)",
            "Step 3: If ACE intolerant, switch to generic ARB (losartan). If persistent cough or angioedema on ACE, ARB approved without step through ACE",
            "Step 4: Triple therapy (ACE/ARB + CCB + thiazide) if dual therapy insufficient. Consider spironolactone 25-50mg for resistant hypertension",
        ],
        "required_documentation": [
            "BP readings showing above goal (>= 130/80 per ACC/AHA) on current regimen",
            "Current medication regimen with doses and duration",
            "For ARB: Documentation of ACE inhibitor intolerance (cough, angioedema, hyperkalemia)",
            "Renal function (BMP) within 12 months — required before starting ACE/ARB",
            "Potassium level monitoring for ACE/ARB + spironolactone combination",
        ],
        "exclusions": [
            "Brand-name antihypertensives when generic equivalent is available (Tier 1 generics preferred)",
            "ACE + ARB concurrent use (dual RAAS blockade — contraindicated per ONTARGET trial)",
            "ARB as first-line without ACE inhibitor trial or documented contraindication",
            "More than 90-day supply per fill for controlled substances (not applicable to antihypertensives but included for formulary consistency)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-031",
        "policy_name": "Gastrointestinal Pharmacotherapy — Proton Pump Inhibitors and H2 Blockers",
        "service_category": "gi_pharmacy",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-10",
        "purpose": (
            "This policy establishes formulary management and step therapy requirements for acid "
            "suppression therapy including proton pump inhibitors and H2 receptor antagonists, with "
            "attention to appropriate duration of therapy and deprescribing."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Omeprazole capsule (generic PPI)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Pantoprazole tablet (generic PPI)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Famotidine tablet (generic H2 blocker)", "cost_range": (4, 12)},
            {"code": "J3490", "system": "HCPCS", "description": "Sucralfate tablet (mucosal protectant)", "cost_range": (10, 30)},
        ],
        "diagnosis_codes": [
            ("K21.0", "GERD with esophagitis"),
            ("K21.9", "GERD without esophagitis"),
            ("K25.9", "Gastric ulcer, unspecified"),
            ("K27.9", "Peptic ulcer, unspecified"),
        ],
        "clinical_criteria": [
            "PPI therapy (omeprazole, pantoprazole, lansoprazole): Covered for GERD, peptic ulcer disease, H. pylori eradication (triple therapy), NSAID gastroprophylaxis (high-risk patients on chronic NSAIDs), and Zollinger-Ellison syndrome. Initial authorization: 8 weeks for GERD; indefinite for Barrett's esophagus, ZE syndrome, or chronic NSAID use with risk factors",
            "PPI deprescribing: After 8 weeks of uncomplicated GERD treatment, attempt step-down to H2 blocker or PPI discontinuation with on-demand use. Chronic PPI use > 12 months requires annual reassessment of indication",
            "H2 blockers (famotidine): First-line for mild GERD, nocturnal acid breakthrough (as add-on to PPI), and stress ulcer prophylaxis in non-ICU settings",
            "Long-term PPI monitoring: Annual assessment of calcium, magnesium, and vitamin B12 levels for patients on PPI > 12 months (risk of hypomagnesemia, fracture, B12 deficiency)",
        ],
        "step_therapy": [
            "Step 1: Lifestyle modification (weight loss, elevation of HOB, avoid triggers) + antacid PRN for mild GERD",
            "Step 2: H2 blocker (famotidine 20mg BID) for 4 weeks for mild-moderate GERD",
            "Step 3: Generic PPI (omeprazole 20mg daily) if H2 blocker insufficient — 8-week course",
            "Step 4: Double-dose PPI or PPI + H2 blocker at bedtime if standard-dose PPI insufficient. EGD referral if refractory",
        ],
        "required_documentation": [
            "Documented GERD symptoms and duration",
            "For chronic PPI (> 8 weeks): Clinical rationale for continued use (Barrett's, chronic NSAID with risk factors, erosive esophagitis Los Angeles grade C-D)",
            "For PPI > 12 months: Annual reassessment note documenting continued indication and deprescribing assessment",
            "H. pylori test result (if tested)",
        ],
        "exclusions": [
            "Brand-name PPI when generic equivalent available (omeprazole, pantoprazole, lansoprazole generics all on Tier 1)",
            "Chronic PPI > 8 weeks for uncomplicated GERD without documented reassessment and deprescribing attempt",
            "PPI prophylaxis for patients on low-dose aspirin alone without additional GI risk factors",
            "Concurrent use of two PPIs",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-032",
        "policy_name": "Antidepressant and Anxiolytic Medications — SSRIs, SNRIs, and Benzodiazepine Standards",
        "service_category": "psychopharmacology",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-01",
        "purpose": (
            "This policy establishes formulary management, step therapy, and safety monitoring "
            "requirements for antidepressant and anxiolytic medications including SSRIs, SNRIs, "
            "and benzodiazepines for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Escitalopram tablet (generic SSRI)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Sertraline tablet (generic SSRI)", "cost_range": (4, 15)},
            {"code": "J3490", "system": "HCPCS", "description": "Duloxetine capsule (generic SNRI)", "cost_range": (8, 25)},
            {"code": "J3490", "system": "HCPCS", "description": "Venlafaxine ER capsule (generic SNRI)", "cost_range": (10, 30)},
            {"code": "J3490", "system": "HCPCS", "description": "Bupropion XL tablet (generic)", "cost_range": (10, 30)},
        ],
        "diagnosis_codes": [
            ("F32.9", "Major depressive disorder, single episode, unspecified"),
            ("F33.0", "Major depressive disorder, recurrent, mild"),
            ("F41.1", "Generalized anxiety disorder"),
            ("F41.0", "Panic disorder"),
            ("F43.10", "Post-traumatic stress disorder, unspecified"),
        ],
        "clinical_criteria": [
            "SSRI first-line: Generic escitalopram or sertraline as first-line for MDD, GAD, panic disorder, and PTSD. Adequate trial defined as 8 weeks at therapeutic dose before switching or augmenting",
            "SNRI second-line: Duloxetine or venlafaxine ER if SSRI inadequate response or not tolerated. Duloxetine preferred for comorbid chronic pain or diabetic neuropathy. Adequate trial: 8 weeks at therapeutic dose",
            "Bupropion: Preferred for MDD with fatigue, weight gain concerns, or sexual dysfunction on SSRI/SNRI. Contraindicated in seizure disorders and eating disorders",
            "Benzodiazepines: Short-term use only (maximum 4-week supply per fill for new prescriptions). Chronic use (> 90 days) requires documented taper plan or treatment-resistant anxiety with documented SSRI/SNRI failures. Quantity limits: alprazolam 60 tabs/30 days, lorazepam 60 tabs/30 days. Concurrent benzodiazepine + opioid prescribing triggers safety alert",
        ],
        "step_therapy": [
            "Step 1: Generic SSRI (escitalopram 10-20mg or sertraline 50-200mg) — 8-week trial",
            "Step 2: Switch SSRI or trial generic SNRI (duloxetine 60mg or venlafaxine ER 75-225mg) — 8-week trial",
            "Step 3: Augmentation (add bupropion, buspirone, or low-dose atypical antipsychotic) if partial response",
            "Step 4: Treatment-resistant pathway — TMS, ketamine/esketamine, or ECT referral (see Behavioral Health policy RBI-PA-2025-004)",
        ],
        "required_documentation": [
            "PHQ-9 (depression) and/or GAD-7 (anxiety) scores at initiation and follow-up",
            "Documentation of medication trial(s) — drug, dose, duration, response, and reason for change",
            "For benzodiazepines: Clinical justification, short-term treatment plan, and taper schedule if > 4 weeks",
            "Suicidality screening (C-SSRS or equivalent) at initiation and dose changes for patients < 25 years (FDA black box warning monitoring)",
        ],
        "exclusions": [
            "Brand-name antidepressants when generic equivalent available",
            "Benzodiazepine fills > 30-day supply per fill (encourages regular reassessment)",
            "Concurrent prescribing of two SSRIs or two SNRIs",
            "Benzodiazepine + opioid concurrent prescribing without documented clinical justification and naloxone co-prescribing",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-033",
        "policy_name": "Trauma and Fracture Management — Hip Fracture Surgery, Fixation, and Post-Surgical Care",
        "service_category": "trauma_orthopedic",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-15",
        "purpose": (
            "This policy defines coverage for acute fracture management including surgical fixation, "
            "hip fracture arthroplasty, and post-surgical rehabilitation for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "27236", "system": "CPT", "description": "Open treatment of femoral neck fracture, internal fixation", "cost_range": (10000, 30000)},
            {"code": "27245", "system": "CPT", "description": "Treatment of intertrochanteric femur fracture, with plate/screws", "cost_range": (12000, 35000)},
            {"code": "27130", "system": "CPT", "description": "Total hip arthroplasty for femoral neck fracture", "cost_range": (15000, 45000)},
            {"code": "27500", "system": "CPT", "description": "Closed treatment of femoral shaft fracture", "cost_range": (2000, 8000)},
            {"code": "27235", "system": "CPT", "description": "Percutaneous fixation of femoral neck fracture", "cost_range": (8000, 25000)},
        ],
        "diagnosis_codes": [
            ("S72.001A", "Fracture of neck of right femur, initial encounter"),
            ("S72.009A", "Fracture of unspecified part of neck of femur, initial"),
            ("S72.101A", "Unspecified trochanteric fracture of right femur, initial"),
            ("S72.301A", "Unspecified fracture of shaft of right femur, initial"),
            ("M80.08XA", "Age-related osteoporosis with pathological fracture, vertebrae"),
        ],
        "clinical_criteria": [
            "For hip fracture surgery: Emergent/urgent — no prior authorization required. Surgery should be performed within 24-48 hours of presentation per ACS TQIP guidelines. Delayed surgery (> 48 hours) requires documented medical reason for delay (anticoagulation reversal, cardiac optimization)",
            "Procedure selection for femoral neck fracture: Displaced fracture in patient > 65 or low functional demand — hemiarthroplasty or THA. Non-displaced fracture or physiologically young patient — internal fixation with cannulated screws. Decision documented by orthopedic surgeon",
            "Post-surgical care: Inpatient rehabilitation (IRF) covered for patients meeting CMS criteria (can participate in 3 hours/day therapy, medical complexity requiring physician supervision). Skilled nursing facility (SNF) for those not meeting IRF criteria. Home PT for low-acuity patients",
            "Osteoporosis workup: DEXA scan and metabolic bone disease evaluation recommended for all fragility fracture patients (fracture from fall from standing height or less). Bisphosphonate or denosumab therapy initiation documented in discharge plan",
        ],
        "step_therapy": [
            "Step 1: Emergency stabilization — imaging (X-ray, CT if needed), pain management, medical optimization",
            "Step 2: Surgical fixation or arthroplasty within 24-48 hours (no step therapy — emergent procedure)",
            "Step 3: Post-surgical VTE prophylaxis (LMWH or DOAC) for 35 days per AAOS guidelines",
            "Step 4: Rehabilitation placement per functional status; osteoporosis treatment initiation",
        ],
        "required_documentation": [
            "Imaging confirming fracture type and displacement",
            "Pre-operative medical clearance (cardiac risk assessment, anticoagulation status)",
            "Surgical report with procedure performed and implant details",
            "Post-operative VTE prophylaxis plan",
            "Rehabilitation disposition and osteoporosis management plan",
        ],
        "exclusions": [
            "Elective delay of hip fracture surgery beyond 48 hours without medical justification",
            "IRF admission for patients not meeting CMS 3-hour rule or medical complexity criteria",
            "Non-operative management of displaced femoral neck fracture in ambulatory patients (surgery is standard of care)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-034",
        "policy_name": "Sepsis and Critical Care — ICU Management, Blood Cultures, and Antimicrobial Stewardship",
        "service_category": "critical_care",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-15",
        "purpose": (
            "This policy defines coverage for critical care services including ICU-level care, "
            "sepsis bundle implementation, blood culture and susceptibility testing, and antimicrobial "
            "stewardship requirements for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "99291", "system": "CPT", "description": "Critical care, first 30-74 minutes", "cost_range": (500, 1500)},
            {"code": "99292", "system": "CPT", "description": "Critical care, each additional 30 min", "cost_range": (200, 600)},
            {"code": "87040", "system": "CPT", "description": "Blood culture for bacteria", "cost_range": (20, 60)},
            {"code": "87186", "system": "CPT", "description": "Antimicrobial susceptibility testing (MIC)", "cost_range": (15, 40)},
            {"code": "36556", "system": "CPT", "description": "Central venous catheter insertion", "cost_range": (500, 1500)},
        ],
        "diagnosis_codes": [
            ("A41.9", "Sepsis, unspecified organism"),
            ("R65.20", "Severe sepsis without septic shock"),
            ("R65.21", "Severe sepsis with septic shock"),
            ("J18.9", "Pneumonia, unspecified organism"),
            ("N39.0", "Urinary tract infection, site not specified"),
        ],
        "clinical_criteria": [
            "Critical care billing: Time-based, requiring the provider's full attention on the critically ill patient for life-threatening conditions. Documented bedside time and critical care activities required. Not solely based on ICU location — patient must meet clinical criteria for critical illness",
            "Sepsis bundle (SEP-1): Blood cultures before antibiotics, broad-spectrum antibiotics within 1 hour of recognition, lactate measurement, 30 mL/kg crystalloid for hypotension or lactate >= 4 mmol/L, vasopressors for persistent hypotension after fluids. Bundle compliance is a quality metric — deviations require documented clinical rationale",
            "Antimicrobial stewardship: Broad-spectrum antibiotics must be de-escalated within 48-72 hours based on culture results. Duration of therapy per IDSA guidelines (e.g., CAP: 5-7 days; UTI: 3-7 days; uncomplicated cellulitis: 5-7 days). IV-to-oral switch when clinically stable",
            "ICU admission criteria: Hemodynamic instability requiring vasopressors, respiratory failure requiring mechanical ventilation, multi-organ dysfunction, post-operative monitoring for high-risk procedures, or close monitoring for rapidly evolving conditions (DKA, status epilepticus)",
        ],
        "step_therapy": [
            "No step therapy for critical care — emergent services covered without prior authorization",
            "Antimicrobial stewardship pathway: Empiric broad-spectrum -> culture results -> targeted narrow-spectrum -> shortest effective duration",
            "ICU step-down criteria: Hemodynamically stable off vasopressors >= 12 hours, extubated or on stable low-level respiratory support, end-organ function improving — transfer to step-down or medical floor",
        ],
        "required_documentation": [
            "Critical care time documented (start/stop or total time with activities performed)",
            "Sepsis bundle elements with times (culture draw time, antibiotic administration time, fluid volume and rate)",
            "For prolonged ICU stay (> 7 days): Daily reassessment of ICU-level care necessity",
            "Antimicrobial stewardship: Culture results, de-escalation decision, and planned antibiotic duration",
        ],
        "exclusions": [
            "Critical care billing for patients who do not meet clinical criteria for critical illness (regardless of ICU bed placement)",
            "ICU admission for monitoring only when step-down unit is appropriate and available",
            "Broad-spectrum antibiotics beyond 72 hours without culture-driven de-escalation or documented clinical rationale",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-035",
        "policy_name": "Bronchodilator and Inhaler Therapy — Albuterol, ICS/LABA, and Asthma Action Plans",
        "service_category": "respiratory_pharmacy",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-09-10",
        "purpose": (
            "This policy defines formulary coverage and step therapy for inhaled medications used in "
            "asthma and COPD management including short-acting bronchodilators, inhaled corticosteroids, "
            "and combination ICS/LABA inhalers."
        ),
        "covered_services": [
            {"code": "J7611", "system": "HCPCS", "description": "Albuterol inhalation solution (nebulizer)", "cost_range": (10, 30)},
            {"code": "J7613", "system": "HCPCS", "description": "Albuterol MDI (metered dose inhaler)", "cost_range": (25, 60)},
            {"code": "J7626", "system": "HCPCS", "description": "Budesonide/formoterol inhaler (ICS/LABA)", "cost_range": (200, 400)},
            {"code": "J7631", "system": "HCPCS", "description": "Fluticasone/salmeterol inhaler (ICS/LABA)", "cost_range": (250, 450)},
            {"code": "J7644", "system": "HCPCS", "description": "Ipratropium/albuterol nebulizer solution", "cost_range": (15, 40)},
        ],
        "diagnosis_codes": [
            ("J45.20", "Mild intermittent asthma, uncomplicated"),
            ("J45.30", "Mild persistent asthma, uncomplicated"),
            ("J45.40", "Moderate persistent asthma, uncomplicated"),
            ("J45.50", "Severe persistent asthma, uncomplicated"),
            ("J44.1", "COPD with acute exacerbation"),
        ],
        "clinical_criteria": [
            "Albuterol rescue inhaler: Covered without step therapy for all asthma and COPD patients. Preferred generic albuterol MDI with spacer. Usage of > 2 canisters/month suggests uncontrolled disease requiring controller medication review",
            "ICS monotherapy (budesonide, fluticasone): Step 2 controller for mild persistent asthma. Low-dose ICS reduces exacerbation risk by 50% vs. SABA-only. Generic preferred",
            "ICS/LABA combination (budesonide/formoterol, fluticasone/salmeterol): Step 3-4 controller for moderate-severe persistent asthma or COPD with frequent exacerbations (>= 2/year). LABA must NOT be used as monotherapy without ICS (FDA black box warning). Preferred agent: generic budesonide/formoterol",
            "LAMA add-on (tiotropium): Step 4-5 for uncontrolled asthma on medium-dose ICS/LABA, or first-line maintenance for COPD. Triple therapy (ICS/LABA/LAMA) for COPD with ongoing exacerbations despite dual therapy",
        ],
        "step_therapy": [
            "Step 1 (Intermittent): SABA PRN (albuterol) — no daily controller needed if symptoms <= 2 days/week",
            "Step 2 (Mild Persistent): Low-dose ICS daily (budesonide 200mcg or fluticasone 88mcg BID)",
            "Step 3 (Moderate Persistent): Medium-dose ICS OR low-dose ICS/LABA combination",
            "Step 4 (Severe Persistent): Medium-high dose ICS/LABA; add LAMA or LTRA if needed",
            "Step 5: High-dose ICS/LABA + LAMA + consider biologic (omalizumab, mepolizumab, dupilumab) based on phenotype",
        ],
        "required_documentation": [
            "Asthma severity classification (intermittent, mild/moderate/severe persistent)",
            "Current controller and rescue medication usage",
            "Spirometry (FEV1) within 12 months for asthma and COPD",
            "Asthma action plan documented for all asthma patients",
            "For biologic add-on: Eosinophil count, IgE level, exacerbation history",
        ],
        "exclusions": [
            "LABA monotherapy without ICS (FDA black box — increased asthma-related death risk)",
            "Brand-name inhalers when therapeutic generic equivalent is available",
            "Nebulizer solution for patients who can effectively use MDI with spacer",
            "Biologic therapy without documented failure of high-dose ICS/LABA + LAMA and appropriate biomarker testing",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-036",
        "policy_name": "Claims Billing Compliance — Duplicate Claims Detection and Prevention",
        "service_category": "billing_compliance",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-10",
        "purpose": (
            "This policy establishes Red Bricks Insurance standards for identifying and preventing "
            "duplicate claim submissions. Duplicate billing is one of the most common forms of healthcare "
            "fraud and waste, accounting for an estimated 5-10% of total claims expenditure."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit — duplicate detection applies to same member/date/code", "cost_range": (95, 180)},
            {"code": "99214", "system": "CPT", "description": "Office visit — level 4 (common duplicate target)", "cost_range": (140, 260)},
            {"code": "99215", "system": "CPT", "description": "Office visit — level 5 (high-value duplicate target)", "cost_range": (200, 380)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("I10", "Essential (primary) hypertension"),
            ("E11.9", "Type 2 diabetes mellitus without complications"),
        ],
        "clinical_criteria": [
            "Exact duplicate: Same member ID, same date of service, same procedure code, same provider NPI — automatically denied as duplicate. Provider may resubmit with corrected claim (frequency type 7) if original claim was in error",
            "Near-duplicate: Same member, same date, same procedure code but different provider NPI — flagged for review. May be legitimate (e.g., patient seen by two providers same day) or may indicate billing error. Supporting documentation required within 30 days",
            "Same-day same-service: Same member, same date, same procedure code, same provider but different units or modifiers — requires clinical documentation justifying medical necessity for the additional service (e.g., bilateral procedure with modifier 50, or distinct services with modifier 59)",
            "Cross-facility duplicate: Same member, same date, same procedure code submitted by both facility and professional claims — review for appropriate split-billing vs. duplicate",
        ],
        "step_therapy": [
            "Step 1: Automated pre-payment duplicate edit — exact duplicates rejected at intake",
            "Step 2: Near-duplicate flagging — claims pended for manual review",
            "Step 3: Provider notification — letter requesting supporting documentation within 30 days",
            "Step 4: If documentation not received — claim denied as duplicate. Provider may appeal with documentation",
        ],
        "required_documentation": [
            "For near-duplicates: Clinical rationale for same-day same-service claims",
            "For cross-facility claims: Documentation of distinct facility and professional components",
            "Corrected claim submission (frequency type 7) for true billing errors on original claim",
            "Modifier documentation (50, 59, 76, 77) when same-day same-code billing is clinically appropriate",
        ],
        "exclusions": [
            "Duplicate claims that cannot be substantiated with clinical documentation will be denied",
            "Providers with duplicate claim rate > 5% of total submissions will be subject to prepayment review",
            "Retroactive duplicate adjustments within 365 days of original payment — overpayment recovery initiated",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-037",
        "policy_name": "CPT Bundling and Unbundling — Correct Coding Initiative (CCI) Edit Standards",
        "service_category": "coding_compliance",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-20",
        "purpose": (
            "This policy establishes Red Bricks Insurance adherence to CMS Correct Coding Initiative "
            "(CCI) edits and National Correct Coding Initiative (NCCI) bundling rules. Unbundling — "
            "billing separately for services that should be billed as a single bundled code — is a "
            "common form of billing abuse that inflates costs by 20-40% above appropriate payment."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit (may be bundled with minor procedure)", "cost_range": (95, 180)},
            {"code": "36415", "system": "CPT", "description": "Venipuncture (bundled into many lab panels)", "cost_range": (10, 25)},
            {"code": "80053", "system": "CPT", "description": "CMP (includes individual chemistry components)", "cost_range": (15, 45)},
            {"code": "85025", "system": "CPT", "description": "CBC (includes individual hematology components)", "cost_range": (12, 35)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("I10", "Essential (primary) hypertension"),
        ],
        "clinical_criteria": [
            "CCI Column 1/Column 2 edits: When Column 1 (comprehensive) and Column 2 (component) codes are billed on the same date by the same provider for the same patient, only the Column 1 code is payable. Column 2 code is bundled unless a valid modifier indicates a separately identifiable service",
            "Mutually exclusive edits: Certain procedure pairs cannot reasonably be performed on the same patient on the same date (e.g., open and laparoscopic approach to same anatomic site). Both claims denied unless documentation proves medical necessity for both",
            "Laboratory panel unbundling: Ordering individual chemistry tests (sodium, potassium, glucose, BUN, creatinine) that constitute a CMP (80053) triggers rebundling edit — paid at CMP rate, not sum of individual components. Provider education letter issued",
            "Surgical unbundling: Billing separately for incision, exploration, debridement, closure, and other components inherent in a surgical procedure — these are included in the global surgical package. Modifier 59 (distinct procedural service) must be supported by documentation of truly separate anatomic site, separate session, or separate encounter",
        ],
        "step_therapy": [
            "Step 1: Pre-payment CCI edit applied — bundled components automatically denied or repriced",
            "Step 2: Modifier review — if modifier 59/XE/XS/XP/XU appended, documentation review required to validate distinct service",
            "Step 3: Provider education outreach for first occurrence of unbundling pattern",
            "Step 4: Prepayment review for providers with systematic unbundling patterns (> 10% of surgical claims with modifier 59)",
        ],
        "required_documentation": [
            "For modifier 59: Operative report documenting distinct anatomic site, separate session, or separate encounter",
            "For modifier 25: Documentation of separately identifiable E/M service distinct from the minor procedure performed",
            "For laboratory rebundling appeals: Clinical rationale for ordering individual components instead of panel",
        ],
        "exclusions": [
            "Component billing that is properly bundled under CCI edits — only comprehensive code payable",
            "Modifier 59 used without documentation of distinct procedural service — claim denied",
            "Systematic laboratory unbundling (> 20% of lab claims failing rebundling edits) — provider placed on prepayment review",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-038",
        "policy_name": "Provider Billing Frequency Standards — Visit Limits and Impossible Day Detection",
        "service_category": "utilization_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-05",
        "purpose": (
            "This policy defines utilization benchmarks and billing frequency standards to identify "
            "providers billing at patterns inconsistent with clinical practice. It includes detection "
            "of impossible day billing (> 24 hours of services or > 50 unique patients per day) and "
            "excessive visit frequency for individual patients."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit, established, level 3 (benchmark: 16-24 per day)", "cost_range": (95, 180)},
            {"code": "99214", "system": "CPT", "description": "Office visit, established, level 4 (benchmark: 12-18 per day)", "cost_range": (140, 260)},
            {"code": "99215", "system": "CPT", "description": "Office visit, established, level 5 (benchmark: 8-12 per day)", "cost_range": (200, 380)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("I10", "Essential (primary) hypertension"),
            ("M54.5", "Low back pain"),
        ],
        "clinical_criteria": [
            "Daily patient volume thresholds: Providers billing > 50 unique patients per day trigger automatic review. By specialty — PCP: 20-30 patients/day is normal; Specialist: 15-25 patients/day is normal; Physical therapist: 8-12 patients/day is normal. Volumes > 2 standard deviations above specialty mean flagged",
            "Impossible day billing: Claims totaling > 24 hours of services per provider per day (e.g., 48 units of 15-min codes = 12 hours; if combined with office visits totaling 12+ hours, exceeds 24-hour day). System automatically flags and pends for review",
            "Excessive visit frequency per patient: Same patient, same provider, same diagnosis code seen > 2x/week (except for PT/OT with active treatment authorization, dialysis, or chemotherapy infusion) flagged for medical necessity review",
            "After-hours billing patterns: Providers billing > 30% of claims on weekends, holidays, or outside normal business hours (without documented emergency, hospital, or shift-based practice) flagged for pattern analysis",
        ],
        "step_therapy": [
            "Step 1: Automated utilization report identifies outlier providers (monthly analysis)",
            "Step 2: Peer comparison report — provider billed volume compared to specialty-specific benchmarks",
            "Step 3: Educational outreach letter for first-time outlier identification",
            "Step 4: Prepayment review or desk audit for persistent outliers (3+ consecutive months above threshold)",
        ],
        "required_documentation": [
            "Daily schedule documentation supporting billed patient volume",
            "For high-volume days: Documentation of each patient encounter with arrival/departure times",
            "For after-hours billing: Documentation of clinical reason for non-standard hours",
            "For frequent visits: Treatment plan justifying visit frequency per patient",
        ],
        "exclusions": [
            "Providers exceeding daily volume thresholds without clinical documentation are subject to claim recoupment for unsupported services",
            "Impossible day billing without acceptable explanation (e.g., group practice billing under single NPI in error) — overpayment recovery initiated",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-039",
        "policy_name": "Modifier 25 and Modifier 59 — Separate Service Documentation Requirements",
        "service_category": "modifier_compliance",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-05",
        "purpose": (
            "This policy establishes documentation requirements for Modifiers 25 and 59, the two "
            "most commonly misused billing modifiers. Improper modifier usage is a significant source "
            "of billing abuse, with estimates suggesting 30-40% of modifier 25 claims lack sufficient "
            "documentation for a separately identifiable service."
        ),
        "covered_services": [
            {"code": "99213-25", "system": "CPT", "description": "Office visit with modifier 25 (separate E/M with minor procedure)", "cost_range": (95, 180)},
            {"code": "99214-25", "system": "CPT", "description": "Office visit with modifier 25 (moderate complexity + procedure)", "cost_range": (140, 260)},
            {"code": "11102-59", "system": "CPT", "description": "Skin biopsy with modifier 59 (distinct procedural service)", "cost_range": (100, 300)},
        ],
        "diagnosis_codes": [
            ("L82.1", "Other seborrheic keratosis"),
            ("D22.9", "Melanocytic nevi, unspecified"),
            ("L57.0", "Actinic keratosis"),
            ("I10", "Essential (primary) hypertension"),
        ],
        "clinical_criteria": [
            "Modifier 25: The E/M service must be significant and separately identifiable from the procedure performed. The E/M cannot be the work-up leading to the procedure decision — it must address a separate clinical issue OR a significantly more complex evaluation than the typical pre-procedure assessment. The documentation must clearly show two distinct services in the medical record",
            "Modifier 25 appended at every visit with a minor procedure: Providers appending modifier 25 on > 75% of visits that include a minor procedure trigger focused audit. Expected appropriate rate: 30-50% depending on specialty",
            "Modifier 59 (Distinct Procedural Service): Used only when procedures not normally reported together are performed in distinct anatomic sites, separate encounters, or separate specimens. Preferred X-modifiers (XE, XS, XP, XU) should be used when they more precisely describe the relationship",
            "Modifier 59 overuse: Providers using modifier 59 on > 15% of surgical claims (excluding legitimate high-frequency scenarios like dermatology multiple lesion destruction) flagged for pattern review",
        ],
        "step_therapy": [
            "Step 1: Pre-payment edit identifies claims with modifier 25 or 59",
            "Step 2: High-volume modifier users (> 75% of visits with modifier 25) flagged for documentation audit",
            "Step 3: Random 10% sample of modifier claims audited quarterly for documentation compliance",
            "Step 4: Providers failing audit (< 70% documentation compliance) placed on prepayment review for modifier claims",
        ],
        "required_documentation": [
            "For modifier 25: Separate and distinct documentation of the E/M service beyond the procedure note — must show different chief complaint, different HPI, or additional clinical decision-making unrelated to the procedure",
            "For modifier 59: Operative report or procedure note documenting distinct anatomic site, separate session, or separate surgical field",
            "For X-modifiers: Specific documentation of which distinct relationship applies (XE=separate encounter, XS=separate structure, XP=separate practitioner, XU=unusual non-overlapping service)",
        ],
        "exclusions": [
            "Modifier 25 for routine pre-procedure evaluation that is inherent in the procedure (e.g., examining a lesion before biopsy without addressing a separate problem)",
            "Modifier 59 without documentation of distinct service — claim denied for the modifier-bearing code",
            "Blanket modifier appending (modifier added to all claims regardless of clinical circumstances) — constitutes billing abuse",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-040",
        "policy_name": "Pharmacy Quantity Limits — Days Supply, Refill Frequency, and Short Refill Detection",
        "service_category": "pharmacy_quantity",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-20",
        "purpose": (
            "This policy establishes quantity limits and refill frequency standards for prescription "
            "medications to prevent over-dispensing, short refills, and drug diversion."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Prescription medication, various (subject to quantity limits)", "cost_range": (5, 6000)},
        ],
        "diagnosis_codes": [
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("I10", "Essential (primary) hypertension"),
            ("F32.9", "Major depressive disorder, single episode"),
            ("G89.29", "Other chronic pain"),
        ],
        "clinical_criteria": [
            "Standard days supply: 30-day supply for maintenance medications, 90-day supply for mail-order pharmacy. Acute medications (antibiotics, short-term pain): 7-14 day supply based on clinical indication",
            "Early refill threshold: Refill not allowed before 75% of days supply has elapsed (e.g., 30-day supply cannot be refilled before day 23). Exceptions: documented dose change, lost/stolen medication (one-time override with police report for controlled substances), or travel supply (documented, maximum 90-day advance)",
            "Short refill detection: Pharmacy fill where next fill occurs before 75% of days_supply has elapsed — flagged for review. Patterns of short refills across multiple pharmacies for the same controlled substance trigger doctor shopping investigation",
            "Opioid quantity limits: 7-day maximum for acute prescriptions (post-surgical exception: 14 days). Chronic opioid > 90 MME/day requires prior authorization. Concurrent benzodiazepine + opioid prescribing triggers pharmacist intervention and prescriber notification",
            "Controlled substance monitoring: All Schedule II-V prescriptions checked against state PDMP at point of dispensing. Multiple prescriber / multiple pharmacy patterns for same controlled substance flagged for member care coordination outreach",
        ],
        "step_therapy": [
            "Step 1: Pharmacist point-of-sale edit — early refill rejected at pharmacy",
            "Step 2: Override request sent to prescriber for clinical justification (dose change, etc.)",
            "Step 3: If pattern of early refills — member referred to care management for medication adherence support",
            "Step 4: For controlled substances — lock-in program (single prescriber, single pharmacy) for members with documented misuse patterns",
        ],
        "required_documentation": [
            "For early refill override: Prescriber attestation of clinical justification (dose change, lost medication, travel)",
            "For opioid > 90 MME: Pain management agreement, PDMP check, urine drug screen",
            "For concurrent opioid + benzodiazepine: Clinical justification and naloxone co-prescribing",
            "For lock-in program: Documentation of misuse pattern and care coordination plan",
        ],
        "exclusions": [
            "Early refills without clinical justification — rejected at point of sale",
            "Controlled substance fills from > 3 prescribers or > 3 pharmacies in 90 days triggers mandatory lock-in program evaluation",
            "Opioid prescriptions > 7 days for acute pain without surgical justification",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-041",
        "policy_name": "Pediatric Services — Well-Child Visits, Developmental Screening, and Childhood Immunizations",
        "service_category": "pediatrics",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-08-15",
        "purpose": (
            "This policy defines coverage for pediatric preventive services, developmental screening, "
            "and childhood immunization schedules per AAP Bright Futures guidelines and ACIP recommendations."
        ),
        "covered_services": [
            {"code": "99381", "system": "CPT", "description": "Preventive visit, new patient, infant (< 1 year)", "cost_range": (150, 300)},
            {"code": "99391", "system": "CPT", "description": "Preventive visit, established patient, infant (< 1 year)", "cost_range": (120, 250)},
            {"code": "99392", "system": "CPT", "description": "Preventive visit, established patient, 1-4 years", "cost_range": (130, 260)},
            {"code": "99393", "system": "CPT", "description": "Preventive visit, established patient, 5-11 years", "cost_range": (130, 260)},
            {"code": "99394", "system": "CPT", "description": "Preventive visit, established patient, 12-17 years", "cost_range": (140, 270)},
            {"code": "96110", "system": "CPT", "description": "Developmental screening (e.g., ASQ)", "cost_range": (15, 40)},
        ],
        "diagnosis_codes": [
            ("Z00.129", "Encounter for routine child health examination with abnormal findings"),
            ("Z00.110", "Health examination for newborn under 8 days old"),
            ("Z23", "Encounter for immunization"),
            ("F84.0", "Autistic disorder"),
            ("F80.9", "Developmental disorder of speech and language, unspecified"),
        ],
        "clinical_criteria": [
            "Well-child visit schedule per AAP Bright Futures: Newborn, 1 month, 2 months, 4 months, 6 months, 9 months, 12 months, 15 months, 18 months, 24 months, 30 months, then annually 3-21 years. All visits covered without prior authorization and without member cost-sharing per ACA",
            "Developmental screening: ASQ or equivalent at 9, 18, and 30 months. Autism-specific screening (M-CHAT-R/F) at 18 and 24 months. Additional screening at any visit if developmental concern raised by parent or provider",
            "Childhood immunizations: All ACIP-recommended vaccines covered without cost-sharing — DTaP, IPV, MMR, Hib, HepB, PCV13, varicella, rotavirus, HepA, HPV (age 11-12), meningococcal, influenza (annual). Catch-up schedules for under-immunized children covered",
            "Lead screening: Blood lead level at 12 and 24 months for all children, or at 36-72 months if not previously tested. Medicaid-eligible children must be tested per state requirements",
        ],
        "step_therapy": [
            "No step therapy for pediatric preventive services — covered per ACA mandates",
            "If developmental delay identified: referral to Early Intervention (0-3 years) or school-based services (3+) with concurrent pediatric subspecialty evaluation as indicated",
        ],
        "required_documentation": [
            "Growth chart (height, weight, BMI percentile) documented at each visit",
            "Developmental milestone assessment per Bright Futures guidelines",
            "Immunization record updated per ACIP schedule",
            "For developmental screening: Standardized tool used and score documented",
            "Anticipatory guidance topics covered (safety, nutrition, sleep, screen time)",
        ],
        "exclusions": [
            "Well-child visit frequency exceeding AAP Bright Futures schedule without documented clinical indication",
            "Non-ACIP-recommended vaccines (travel vaccines billed separately, not covered under pediatric preventive)",
            "Developmental screening tools not validated per AAP guidelines",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-042",
        "policy_name": "Network Adequacy and Out-of-Network Authorization — Emergency and Non-Emergency OON Standards",
        "service_category": "network_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-01",
        "purpose": (
            "This policy defines Red Bricks Insurance network adequacy standards and the process for "
            "authorizing out-of-network (OON) services when in-network providers are not available "
            "within access standards, per No Surprises Act and state balance billing protections."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit — OON authorization when INN not available", "cost_range": (95, 180)},
            {"code": "99285", "system": "CPT", "description": "ED visit — OON emergency covered at INN rate per NSA", "cost_range": (700, 2200)},
            {"code": "99223", "system": "CPT", "description": "Inpatient admission — OON at INN facility covered per NSA", "cost_range": (400, 1000)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Adult medical examination without abnormal findings"),
            ("R07.9", "Chest pain, unspecified"),
            ("I21.9", "Acute myocardial infarction, unspecified"),
        ],
        "clinical_criteria": [
            "Emergency OON: Covered at in-network cost-sharing level per No Surprises Act. Member cannot be balance-billed for OON emergency services. Post-stabilization services at OON facility covered at INN rate until safe transfer is possible or member consents to OON continued care",
            "Non-emergency OON: Prior authorization required. Approved when: (a) no INN provider of required specialty within access standard (PCP: 30 min/15 miles; specialist: 60 min/30 miles; hospital: 30 min/30 miles), (b) INN provider wait time exceeds access standard (PCP: 10 business days; specialist: 15 business days; urgent: 48 hours), or (c) clinical continuity of care during network transition",
            "Ancillary OON at INN facility: No Surprises Act protects members from surprise bills from OON anesthesiologists, pathologists, radiologists, and assistant surgeons at INN facilities. Member pays INN cost-sharing; payer and OON provider resolve payment through IDR process if needed",
            "OON gap exception: If member demonstrates no INN provider available for needed service, OON services authorized at INN benefit level. Member must obtain gap exception approval BEFORE receiving non-emergency OON services",
        ],
        "step_therapy": [
            "Step 1: Member or provider contacts Red Bricks to verify INN provider availability for needed service",
            "Step 2: If no INN provider within access standards — gap exception authorized, OON provider paid at INN rate",
            "Step 3: Member receives OON services at INN cost-sharing level",
            "Step 4: Red Bricks resolves payment with OON provider (QPA-based payment or IDR process per NSA)",
        ],
        "required_documentation": [
            "Provider directory search documentation showing no INN provider available",
            "For gap exception: Clinical rationale and documentation of INN provider search",
            "For continuity of care: Treatment history and clinical need for continuity with specific provider",
            "For emergency OON: ED report documenting emergency medical condition",
        ],
        "exclusions": [
            "OON services without prior gap exception authorization (except emergency and ancillary at INN facility)",
            "Member-elected OON services when adequate INN providers are available (member pays OON cost-sharing)",
            "Balance billing by OON providers for services covered under the No Surprises Act",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-043",
        "policy_name": "Prior Authorization Process — Timelines, Criteria, and Transparency Standards",
        "service_category": "prior_auth_process",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-15",
        "purpose": (
            "This policy establishes Red Bricks Insurance prior authorization process standards "
            "including response timelines, clinical review criteria, transparency requirements per "
            "CMS-0057-F (Interoperability and Prior Authorization final rule), and provider/member "
            "notification requirements."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit (not subject to PA)", "cost_range": (95, 180)},
            {"code": "27447", "system": "CPT", "description": "Total knee arthroplasty (requires PA)", "cost_range": (18000, 45000)},
            {"code": "78816", "system": "CPT", "description": "PET/CT (requires PA)", "cost_range": (3000, 8000)},
        ],
        "diagnosis_codes": [
            ("M17.11", "Primary osteoarthritis, right knee"),
            ("C34.90", "Malignant neoplasm unspecified bronchus or lung"),
        ],
        "clinical_criteria": [
            "Standard PA determination: Within 7 calendar days of receiving complete request. If additional information needed, request issued within 3 business days; provider has 14 calendar days to respond",
            "Urgent/expedited PA determination: Within 72 hours when standard timeline could jeopardize life, health, or ability to regain maximum function. Expedited requests may be initiated by provider or member",
            "Retrospective PA (emergency/urgent services): Authorization reviewed within 30 days of claim submission. Emergency services that met prudent layperson standard are not denied retroactively based on final diagnosis",
            "Gold card/prior authorization exemption: Providers with >= 90% PA approval rate over 12 months for a specific service category may qualify for PA exemption for that category. Exemption reviewed annually",
            "CMS-0057-F compliance: PA decisions must include specific reason for denial with policy citation, applicable clinical criteria not met, and how to appeal. PA status available via FHIR API (Patient Access API) starting plan year 2026",
        ],
        "step_therapy": [
            "Step 1: Provider submits PA request via portal (electronic preferred) or fax with clinical documentation",
            "Step 2: Administrative review — is the request complete? If not, additional information requested within 3 business days",
            "Step 3: Clinical review by appropriately qualified reviewer (peer-to-peer available for any denial)",
            "Step 4: Determination issued — approval (valid 60-90 days), denial (with specific reason and appeal rights), or pend (additional information needed)",
        ],
        "required_documentation": [
            "Member demographics and insurance information",
            "Requesting provider NPI and contact information",
            "CPT/HCPCS code(s) and ICD-10 diagnosis code(s)",
            "Clinical documentation supporting medical necessity per applicable disease-specific policy",
            "For urgent requests: Clinical justification for expedited review",
        ],
        "exclusions": [
            "PA not required for: preventive services (ACA-mandated), emergency services, office visits (E/M codes 99211-99215 except when billed with specific high-cost procedures), laboratory tests at standard frequency, and pharmacy Tier 1 generics",
            "PA determination delayed beyond regulatory timelines (7 days standard, 72 hours urgent) — request deemed approved per applicable state regulations",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-044",
        "policy_name": "Appeals and Grievances — Internal Appeal Process and External Review Standards",
        "service_category": "appeals_process",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-10",
        "purpose": (
            "This policy defines the appeals and grievance process for Red Bricks Insurance members "
            "and providers to challenge adverse coverage determinations, including internal appeal "
            "procedures and external Independent Review Organization (IRO) processes."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Any denied service may be appealed", "cost_range": (95, 180)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Applies to any diagnosis associated with a denied service"),
        ],
        "clinical_criteria": [
            "Level 1 Internal Appeal: Member or provider submits written appeal within 180 days of adverse determination. Review by clinical reviewer not involved in original decision. Decision within 30 calendar days (standard) or 72 hours (expedited). Reviewer must be appropriate specialist for the clinical issue",
            "Level 2 Internal Appeal (if applicable by state): Second-level review by medical director or designee. Available if Level 1 upholds denial. Decision within 30 calendar days",
            "External Review (IRO): Available after exhaustion of internal appeals (or concurrently for urgent cases). Independent Review Organization reviews de novo. IRO decision is binding on the plan. Member has 4 months to request external review after final internal appeal decision",
            "Expedited appeal: Available when standard timeline could seriously jeopardize life, health, or ability to regain maximum function. 72-hour determination for both internal and external expedited appeals. May be requested by provider or member",
        ],
        "step_therapy": [
            "Step 1: Adverse determination issued with specific denial reason, policy citation, and appeal instructions",
            "Step 2: Member/provider files appeal with additional clinical documentation supporting medical necessity",
            "Step 3: Internal appeal review by independent clinical reviewer (peer-to-peer available for physician reviewers)",
            "Step 4: If internal appeal denied — member may request external IRO review",
        ],
        "required_documentation": [
            "Written appeal request identifying the adverse determination being appealed",
            "Additional clinical documentation not available at time of original determination",
            "Provider letter of medical necessity with specific clinical rationale",
            "For expedited appeal: Clinical justification for urgency",
            "Member authorization (if provider is filing on behalf of member)",
        ],
        "exclusions": [
            "Appeals filed after 180 days from adverse determination (time limit strictly enforced unless good cause shown)",
            "Appeals of coverage decisions that are not adverse benefit determinations (e.g., billing disputes, claim processing errors — these follow the grievance process, not clinical appeal)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-045",
        "policy_name": "Coordination of Benefits — Primary/Secondary Payer Determination and Subrogation",
        "service_category": "coordination_benefits",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-07-30",
        "purpose": (
            "This policy establishes Red Bricks Insurance coordination of benefits (COB) procedures "
            "for members with multiple health insurance coverages, including primary/secondary payer "
            "determination rules and third-party liability/subrogation standards."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "Office visit (COB applies to all covered services)", "cost_range": (95, 180)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "COB applies to all diagnoses — payer order determination"),
        ],
        "clinical_criteria": [
            "Birthday rule: When a dependent child is covered by both parents' plans, the plan of the parent whose birthday falls earlier in the calendar year is primary. If same birthday, the plan that has covered the parent longer is primary",
            "Active employee rule: For members with coverage through active employment and COBRA/retiree coverage, the active employment plan is primary. COBRA/retiree plan is secondary",
            "Medicare secondary payer: For members under 65 with group health plan through active employment (employer with 20+ employees), group health plan is primary and Medicare is secondary. For members 65+ with employer plan (employer with 20+ employees), employer plan is primary",
            "Third-party liability/subrogation: When a member's medical expenses result from another party's negligence (auto accident, workers comp, premises liability), Red Bricks has subrogation rights to recover from the liable party's insurer. Member must notify Red Bricks of third-party claims",
        ],
        "step_therapy": [
            "Step 1: COB questionnaire completed at enrollment and annually at renewal",
            "Step 2: Primary payer processes claim first; explanation of benefits (EOB) sent to secondary payer",
            "Step 3: Secondary payer processes remaining balance up to the plan's allowed amount (no duplication of benefits — total payment from all sources cannot exceed total charges)",
            "Step 4: Member receives single combined EOB showing both primary and secondary payments",
        ],
        "required_documentation": [
            "COB questionnaire with other insurance information (carrier name, ID, group number, subscriber relationship)",
            "Primary payer EOB for secondary claims processing",
            "For third-party liability: Incident report, other party's insurance information, attorney information if applicable",
            "Annual COB verification during open enrollment",
        ],
        "exclusions": [
            "Duplication of benefits — total payment from all insurers cannot exceed total billed charges",
            "Failure to disclose other coverage may result in claim denial or recoupment of overpayment",
            "Members who fail to cooperate with subrogation may be responsible for reimbursement of benefits paid",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-046",
        "policy_name": "Inpatient Admission and DRG-Based Payment — Medical Necessity and Length of Stay Standards",
        "service_category": "inpatient_management",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-01",
        "purpose": (
            "This policy establishes medical necessity criteria for inpatient admission and "
            "length of stay management under DRG-based payment methodology for Red Bricks Insurance members."
        ),
        "covered_services": [
            {"code": "99223", "system": "CPT", "description": "Initial hospital care, high complexity", "cost_range": (400, 1000)},
            {"code": "99232", "system": "CPT", "description": "Subsequent hospital care, moderate complexity", "cost_range": (150, 400)},
            {"code": "99238", "system": "CPT", "description": "Hospital discharge day management, 30 min or less", "cost_range": (100, 250)},
        ],
        "diagnosis_codes": [
            ("A41.9", "Sepsis, unspecified organism"),
            ("I50.9", "Heart failure, unspecified"),
            ("J18.9", "Pneumonia, unspecified organism"),
            ("I21.9", "Acute myocardial infarction, unspecified"),
            ("K35.80", "Acute appendicitis, unspecified"),
        ],
        "clinical_criteria": [
            "Inpatient admission medical necessity: The patient requires 24-hour nursing care and physician oversight that cannot be provided in an outpatient, observation, or skilled nursing setting. InterQual or MCG criteria applied for admission review. Severity of illness (SI) and intensity of service (IS) must both be met",
            "DRG payment: Inpatient stays paid per DRG (diagnosis-related group) which bundles all facility charges. Outlier payments for exceptionally high-cost stays (fixed loss threshold + 80% of costs above threshold). DRG assignment based on principal diagnosis, procedures performed, complications/comorbidities, and discharge status",
            "Length of stay management: Concurrent review at geometric mean LOS (GMLOS) for assigned DRG. Extension authorized if medical necessity criteria still met. Discharge planning begins at admission — anticipated discharge date documented within 24 hours of admission",
            "Observation vs. inpatient: Patients expected to require < 48 hours of hospital care should be placed in observation status. Two-midnight rule: if the admitting physician expects the patient to require hospital care spanning at least two midnights, inpatient admission is appropriate",
        ],
        "step_therapy": [
            "Step 1: Pre-admission review (elective admissions) — clinical criteria validated before admission date",
            "Step 2: Concurrent review — clinical criteria validated during stay at GMLOS and at regular intervals",
            "Step 3: Discharge planning — safe disposition arranged (home, SNF, IRF, LTACH as appropriate)",
            "Step 4: Retrospective review for emergency admissions — medical necessity validated post-discharge",
        ],
        "required_documentation": [
            "Admission H&P with admitting diagnosis, severity of illness, and treatment plan",
            "Daily progress notes documenting continued inpatient-level medical necessity",
            "Discharge summary with principal and secondary diagnoses, procedures, and discharge disposition",
            "For extended stays: Documentation of clinical reasons for LOS beyond GMLOS",
        ],
        "exclusions": [
            "Inpatient admission for conditions manageable in observation status (< 2 midnights expected)",
            "Social admissions (homelessness, awaiting placement) without concurrent medical necessity",
            "Elective admissions without pre-authorization (except emergency and urgent admissions)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-047",
        "policy_name": "Genetic Testing — Hereditary Cancer Panels, Pharmacogenomics, and Prenatal Screening",
        "service_category": "genetic_testing",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-10-30",
        "purpose": (
            "This policy defines coverage for genetic testing including hereditary cancer susceptibility "
            "panels, pharmacogenomic testing, and prenatal genetic screening."
        ),
        "covered_services": [
            {"code": "81162", "system": "CPT", "description": "BRCA1/BRCA2 full sequence analysis", "cost_range": (500, 3000)},
            {"code": "81432", "system": "CPT", "description": "Hereditary breast cancer gene panel (multi-gene)", "cost_range": (800, 4000)},
            {"code": "81225", "system": "CPT", "description": "CYP2D6 pharmacogenomic testing", "cost_range": (200, 600)},
            {"code": "81420", "system": "CPT", "description": "Fetal chromosomal aneuploidy (cell-free DNA / NIPT)", "cost_range": (500, 1500)},
            {"code": "81228", "system": "CPT", "description": "Cytogenomic microarray, constitutional", "cost_range": (800, 2500)},
        ],
        "diagnosis_codes": [
            ("Z15.01", "Genetic susceptibility to malignant neoplasm of breast"),
            ("Z15.04", "Genetic susceptibility to malignant neoplasm of prostate"),
            ("Z36.0", "Encounter for antenatal screening for chromosomal anomalies"),
            ("Z80.3", "Family history of malignant neoplasm of breast"),
        ],
        "clinical_criteria": [
            "BRCA1/BRCA2 testing: Personal history of breast cancer < 50, triple-negative breast cancer < 60, ovarian cancer at any age, male breast cancer, pancreatic cancer with family history, or first-degree relative with known BRCA mutation. Genetic counseling required before and after testing",
            "Multi-gene hereditary cancer panel: Meets NCCN criteria for hereditary cancer syndrome evaluation. Gene panel selected based on personal/family history (breast/ovarian panel, colorectal panel, or comprehensive). Single-site testing for known familial mutation is first-line for at-risk relatives",
            "Pharmacogenomic testing: Covered for CYP2D6 (tamoxifen metabolism), CYP2C19 (clopidogrel), DPYD (fluoropyrimidine toxicity), UGT1A1 (irinotecan), and HLA-B*5701 (abacavir hypersensitivity). Testing must be ordered by prescribing provider with intent to use results for prescribing decisions",
            "Prenatal NIPT (cell-free DNA): Covered for all pregnancies per ACOG 2020 guidelines. First-line screen for trisomy 21, 18, 13 and sex chromosome aneuploidies. Diagnostic testing (CVS or amniocentesis) offered if NIPT positive",
        ],
        "step_therapy": [
            "Step 1: Genetic counseling assessment (genetic counselor or trained provider) before hereditary cancer testing",
            "Step 2: Targeted single-gene or single-site testing if known familial mutation exists",
            "Step 3: Multi-gene panel if no known familial mutation and clinical criteria met",
            "Step 4: Results disclosure with genetic counseling and management recommendations",
        ],
        "required_documentation": [
            "Genetic counseling note documenting family history assessment and test indication",
            "Three-generation pedigree for hereditary cancer testing",
            "For pharmacogenomics: Specific medication and clinical decision the test will inform",
            "For prenatal: Gestational age and indication for screening",
        ],
        "exclusions": [
            "Direct-to-consumer genetic testing results not ordered by licensed provider — not covered and not used for clinical decision-making",
            "Whole genome/exome sequencing without specific clinical indication",
            "Repeat genetic testing for the same gene/mutation already resulted",
            "Genetic testing for non-medical purposes (ancestry, paternity)",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-048",
        "policy_name": "Provider Credentialing and Network Integrity — FWA Provider Monitoring Standards",
        "service_category": "provider_integrity",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-01",
        "purpose": (
            "This policy establishes Red Bricks Insurance provider credentialing, monitoring, and "
            "network integrity standards for detecting and preventing provider-level fraud, waste, and "
            "abuse including provider ring detection and exclusion monitoring."
        ),
        "covered_services": [
            {"code": "99213", "system": "CPT", "description": "All services subject to provider integrity monitoring", "cost_range": (95, 180)},
        ],
        "diagnosis_codes": [
            ("Z00.00", "Provider integrity applies to all diagnoses and service types"),
        ],
        "clinical_criteria": [
            "Provider credentialing: All network providers must complete initial credentialing and re-credentialing every 3 years per NCQA standards. Verification includes: active license, board certification (if applicable), DEA registration, malpractice history, OIG/SAM exclusion check, and hospital privileges",
            "OIG/SAM exclusion monitoring: Monthly screening of all providers against OIG List of Excluded Individuals/Entities (LEIE) and GSA System for Award Management (SAM). Immediate termination for matched providers — claims for services rendered by excluded providers are not payable by federal healthcare programs",
            "Provider ring detection: Network analysis identifying clusters of 3-5 providers with unusually high member sharing rates (> 30% overlapping patient panels) combined with billing anomalies. Flagged provider clusters referred to Special Investigations Unit (SIU) for investigation",
            "Aberrant billing patterns: Providers with billing patterns > 2 standard deviations from specialty peer benchmarks on any of: E/M code distribution, denial rate, services per member per year, or charges per member per year — flagged for focused review",
        ],
        "step_therapy": [
            "Step 1: Automated monthly provider monitoring (OIG, SAM, license boards, Medicare opt-out list)",
            "Step 2: Quarterly provider profiling — peer comparison analysis across billing metrics",
            "Step 3: Educational outreach for first-time outlier identification",
            "Step 4: Focused review, prepayment review, or SIU referral for persistent or egregious patterns",
            "Step 5: Provider termination from network for confirmed fraud, license actions, or exclusion",
        ],
        "required_documentation": [
            "Credentialing file: License verification, board certification, DEA, malpractice history, NPDB query, OIG/SAM check",
            "Re-credentialing: Updated credentials every 3 years with performance review",
            "For SIU referral: Detailed analysis of billing patterns, member overlap analysis, and supporting claims data",
        ],
        "exclusions": [
            "Claims from excluded providers — not payable, subject to recoupment if paid in error",
            "Providers who fail to cooperate with credentialing or audit requests — subject to network termination",
            "Provider ring participants — all associated claims reviewed for potential recoupment",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-049",
        "policy_name": "Drug Switching and Therapeutic Substitution — Generic-to-Brand Switch Detection",
        "service_category": "pharmacy_integrity",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-11-10",
        "purpose": (
            "This policy establishes monitoring standards for detecting inappropriate drug switching "
            "patterns where patients are switched from lower-cost generic medications to higher-cost "
            "brand-name products within the same therapeutic class without clinical justification."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Prescription medications subject to generic mandate and switch monitoring", "cost_range": (5, 6000)},
        ],
        "diagnosis_codes": [
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("E78.5", "Hyperlipidemia, unspecified"),
            ("I10", "Essential (primary) hypertension"),
            ("F32.9", "Major depressive disorder, single episode"),
        ],
        "clinical_criteria": [
            "Generic mandate: When an FDA-approved generic equivalent is available, generic must be dispensed unless: (a) prescriber writes 'Dispense as Written' (DAW) with documented clinical justification, (b) member has documented allergy or adverse reaction to generic formulation, or (c) narrow therapeutic index drug where generic substitution is clinically inappropriate",
            "Drug switching detection: Members switched from generic to brand-name within same therapeutic class within 60 days — flagged for review. Pattern: metformin generic -> Riomet (brand metformin liquid) without documented swallowing difficulty; atorvastatin generic -> Lipitor brand without clinical justification",
            "Therapeutic class switching: Members switched between therapeutic classes without clinical justification (e.g., statin to PCSK9 inhibitor without statin failure documentation) flagged for step therapy compliance review",
            "Pharmacy-initiated switches: Pharmacies that consistently switch members from generic to brand-name without DAW designation — pharmacy profiled and investigated for potential financial incentive (brand manufacturer rebates or higher dispensing fees)",
        ],
        "step_therapy": [
            "Step 1: Generic dispensing enforced at POS (pharmacy must dispense generic unless DAW)",
            "Step 2: DAW override requires prescriber clinical justification documented in medical record",
            "Step 3: Pharmacy profiling identifies pharmacies with high generic-to-brand switch rates",
            "Step 4: Investigation of systematic switching patterns — potential pharmacy fraud referral",
        ],
        "required_documentation": [
            "For DAW/brand-name dispensing: Prescriber documentation of clinical reason generic is inappropriate (allergy, formulation intolerance, NTI drug)",
            "For therapeutic class switch: Documentation of failure or intolerance of prior class agent",
            "Pharmacy records showing dispensing patterns and DAW codes",
        ],
        "exclusions": [
            "Brand-name medication when generic equivalent is available and no clinical justification for brand — member pays full cost difference",
            "Pharmacies with generic-to-brand switch rates > 3x network average — subject to audit and potential network termination",
            "Prescribers with DAW rates > 20% of total prescriptions — educational outreach and monitoring",
        ],
    },
    {
        "policy_id": "RBI-PA-2025-050",
        "policy_name": "Member Doctor Shopping Detection — Multiple Provider and Pharmacy Utilization Monitoring",
        "service_category": "member_integrity",
        "effective_date": "2025-01-01",
        "last_reviewed": "2024-12-05",
        "purpose": (
            "This policy establishes monitoring criteria for detecting member-level utilization patterns "
            "consistent with doctor shopping, pharmacy shopping, and controlled substance misuse to "
            "protect member safety and prevent drug diversion."
        ),
        "covered_services": [
            {"code": "J3490", "system": "HCPCS", "description": "Controlled substance prescriptions subject to monitoring", "cost_range": (5, 500)},
            {"code": "99213", "system": "CPT", "description": "Office visits at multiple providers for same condition", "cost_range": (95, 180)},
        ],
        "diagnosis_codes": [
            ("F11.20", "Opioid dependence, uncomplicated"),
            ("F13.20", "Sedative, hypnotic or anxiolytic dependence, uncomplicated"),
            ("G89.29", "Other chronic pain"),
            ("M54.5", "Low back pain"),
        ],
        "clinical_criteria": [
            "Doctor shopping threshold: Member seeing >= 5 different prescribers for the same controlled substance class (opioids, benzodiazepines, stimulants) within a 90-day period triggers mandatory review. PDMP data corroborated with claims data",
            "Pharmacy shopping threshold: Member filling controlled substance prescriptions at >= 3 different pharmacies within a 90-day period flagged for review. Geographic analysis: pharmacies > 30 miles apart from each other and/or member residence raises suspicion level",
            "Morphine milligram equivalent (MME) monitoring: Members receiving > 120 MME/day from combined prescriptions — urgent safety review. Members receiving > 90 MME/day — care coordination outreach. Concurrent opioid + benzodiazepine + muscle relaxant (triple combination) flagged for immediate prescriber notification",
            "ED utilization for controlled substances: Members presenting to >= 4 different EDs in 12 months with pain complaints resulting in controlled substance prescriptions — flagged for care management and potential lock-in program",
        ],
        "step_therapy": [
            "Step 1: Automated PDMP monitoring — daily claims feed analyzed for multi-prescriber/multi-pharmacy patterns",
            "Step 2: Member outreach by care manager — education on safe medication use, PCP referral, SUD screening",
            "Step 3: Prescriber notification — all prescribers informed of member's utilization pattern",
            "Step 4: Lock-in program — member required to use single prescriber and single pharmacy for controlled substances (minimum 12-month enrollment)",
        ],
        "required_documentation": [
            "PDMP report showing all controlled substance prescriptions filled in past 12 months",
            "Claims data analysis showing prescriber and pharmacy utilization patterns",
            "Care management outreach documentation",
            "For lock-in program: Member notification letter with appeal rights",
            "Prescriber notifications sent and responses received",
        ],
        "exclusions": [
            "Members in active cancer treatment or hospice — exempt from doctor shopping monitoring (legitimate need for multiple prescribers)",
            "Members transitioning providers due to insurance change — 90-day grace period before monitoring thresholds apply",
            "Lock-in program members who demonstrate compliance for 12 consecutive months — may petition for release from lock-in",
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

# Red Bricks Insurance — reference codes and lookup data (ICD-10, CPT, denial codes, etc.).

# ICD-10 diagnosis codes (code, description) and weights for weighted sampling
ICD10_CODES = [
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
    ("E78.5", "Hyperlipidemia, unspecified"),
    ("I10", "Essential (primary) hypertension"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery"),
    ("I50.9", "Heart failure, unspecified"),
    ("J44.1", "COPD with acute exacerbation"),
    ("J45.20", "Mild intermittent asthma, uncomplicated"),
    ("M54.5", "Low back pain"),
    ("M17.11", "Primary osteoarthritis, right knee"),
    ("F32.9", "Major depressive disorder, single episode, unspecified"),
    ("F41.1", "Generalized anxiety disorder"),
    ("G47.33", "Obstructive sleep apnea"),
    ("N18.3", "Chronic kidney disease, stage 3"),
    ("K21.0", "GERD with esophagitis"),
    ("S72.001A", "Fracture of neck of right femur, init"),
    ("K80.20", "Calculus of gallbladder without cholecystitis"),
    ("O80", "Encounter for full-term uncomplicated delivery"),
    ("C50.919", "Malignant neoplasm unspecified female breast"),
    ("C34.90", "Malignant neoplasm unspecified bronchus or lung"),
    ("N40.0", "Benign prostatic hyperplasia"),
    ("Z23", "Encounter for immunization"),
    ("Z00.00", "Adult medical examination without abnormal findings"),
    ("R10.9", "Unspecified abdominal pain"),
]

ICD10_WEIGHTS = [
    8, 4, 10, 12, 5, 4, 4, 5, 8, 5, 6, 5, 4, 3, 5,
    2, 2, 2, 2, 1, 3, 6, 8, 4,
]

# CPT professional (code, desc, cost_low, cost_high)
CPT_PROFESSIONAL = [
    ("99213", "Office visit, established, level 3", 95, 180),
    ("99214", "Office visit, established, level 4", 140, 260),
    ("99215", "Office visit, established, level 5", 200, 380),
    ("99203", "Office visit, new patient, level 3", 130, 250),
    ("99395", "Preventive visit, 18-39, established", 150, 280),
    ("99396", "Preventive visit, 40-64, established", 165, 310),
    ("90834", "Psychotherapy, 45 minutes", 100, 190),
    ("36415", "Venipuncture", 10, 25),
    ("71046", "Chest X-ray, 2 views", 40, 120),
    ("80053", "Comprehensive metabolic panel", 15, 45),
    ("85025", "CBC with differential", 12, 35),
    ("81001", "Urinalysis, automated with microscopy", 8, 20),
]

# CPT institutional IP/OP and ER
CPT_INSTITUTIONAL_IP = [
    ("27447", "Total knee arthroplasty", 10000, 28000),
    ("27130", "Total hip arthroplasty", 11000, 30000),
    ("44950", "Appendectomy", 3500, 10000),
    ("47562", "Laparoscopic cholecystectomy", 4000, 12000),
    ("59400", "Routine obstetric care, vaginal delivery", 3000, 8000),
    ("43239", "Upper GI endoscopy with biopsy", 1800, 5000),
]

CPT_INSTITUTIONAL_OP = [
    ("45380", "Colonoscopy with biopsy", 700, 2800),
    ("29881", "Knee arthroscopy with meniscectomy", 2500, 8000),
    ("62323", "Lumbar epidural injection", 700, 2500),
    ("66984", "Cataract surgery with IOL", 1800, 5000),
]

CPT_ER = [
    ("99281", "ED visit, level 1", 80, 250),
    ("99282", "ED visit, level 2", 150, 450),
    ("99283", "ED visit, level 3", 250, 750),
    ("99284", "ED visit, level 4", 450, 1300),
    ("99285", "ED visit, level 5", 700, 2200),
]

# Revenue codes (code, description)
REVENUE_CODES_IP = [
    ("0110", "Room & Board, Semi-Private"),
    ("0120", "Room & Board, Private"),
    ("0250", "Pharmacy"),
    ("0300", "Laboratory"),
    ("0320", "Radiology - Diagnostic"),
    ("0360", "Operating Room"),
    ("0370", "Anesthesia"),
]

REVENUE_CODES_OP = [
    ("0450", "Emergency Room"),
    ("0320", "Radiology - Diagnostic"),
    ("0300", "Laboratory"),
    ("0360", "Operating Room"),
    ("0510", "Clinic"),
]

# DRGs (code, desc, cost_low, cost_high)
DRGS = [
    ("470", "Major Hip and Knee Joint Replacement", 10000, 28000),
    ("871", "Septicemia or Severe Sepsis without MV >96 hrs with MCC", 12000, 35000),
    ("291", "Heart Failure and Shock with MCC", 7000, 20000),
    ("392", "Esophagitis, Gastroenteritis with MCC", 5000, 14000),
    ("690", "Kidney and Urinary Tract Infections without MCC", 3500, 10000),
    ("775", "Vaginal Delivery without Complicating Diagnoses", 3000, 8500),
]

# Place of service
POS_CODES = {
    "Professional": [
        ("11", "Office"),
        ("22", "On Campus-Outpatient Hospital"),
        ("23", "Emergency Room - Hospital"),
        ("02", "Telehealth - Provider Site"),
    ],
    "Institutional_IP": [("21", "Inpatient Hospital")],
    "Institutional_OP": [
        ("22", "On Campus-Outpatient Hospital"),
        ("24", "Ambulatory Surgical Center"),
    ],
    "ER": [("23", "Emergency Room - Hospital")],
}

# Industry-standard denial / adjustment codes (CARC + group code; RARC-like messages)
# CO=Contractual, PR=Patient Responsibility, PI=Payer Initiated, OA=Other
DENIAL_CODES = [
    "CO-4",     # Procedure code inconsistent with modifier
    "CO-16",    # Claim lacks information needed for adjudication
    "CO-29",    # Time limit for filing expired
    "CO-50",    # Non-covered / not medically necessary
    "CO-97",    # Benefit for this service in another claim
    "PI-204",   # Service not covered under patient's plan
    "PR-1",     # Deductible amount
    "PR-2",     # Coinsurance amount
    "PR-3",     # Co-payment amount
    "CO-197",   # Precertification/authorization absent
    "CO-18",    # Duplicate claim/service
    "OA-23",    # Charges under capitation agreement
    "CO-27",    # Expenses after coverage terminated
    "PI-39",    # Denied at time of auth request
]

ADJUSTMENT_CODES = [
    "CO-45", "CO-16", "CO-253", "OA-23", "PR-1", "PR-2", "PR-3", "CO-24",
]

# Pharmacy
PHARMACY_DRUGS = [
    ("00002322730", "Atorvastatin 20mg", "HMG-CoA Reductase Inhibitors", 12.50, False),
    ("00071015523", "Lisinopril 10mg", "ACE Inhibitors", 8.00, False),
    ("00093718001", "Metformin 500mg", "Biguanides", 6.00, False),
    ("00078040105", "Amlodipine 5mg", "Calcium Channel Blockers", 7.00, False),
    ("00069024230", "Omeprazole 20mg", "Proton Pump Inhibitors", 10.00, False),
    ("63304082601", "Levothyroxine 50mcg", "Thyroid Hormones", 11.00, False),
    ("16729004401", "Escitalopram 10mg", "SSRIs", 10.50, False),
    ("00591040501", "Albuterol Inhaler", "Bronchodilators", 35.00, False),
    ("50242004001", "Humira 40mg pen", "TNF Inhibitors", 5800.00, True),
    ("57894015001", "Eliquis 5mg", "Direct Oral Anticoagulants", 480.00, False),
    ("00169413012", "Ozempic 1mg pen", "GLP-1 Receptor Agonists", 935.00, False),
]

# Provider specialties (name, weight)
SPECIALTIES = [
    ("Internal Medicine", 15),
    ("Family Medicine", 15),
    ("Pediatrics", 8),
    ("Cardiology", 6),
    ("Orthopedic Surgery", 5),
    ("General Surgery", 4),
    ("Obstetrics/Gynecology", 5),
    ("Psychiatry", 4),
    ("Emergency Medicine", 5),
    ("Gastroenterology", 4),
    ("Oncology", 3),
]

# Line of business and plan types
LOB_CONFIG = {
    "Commercial": {
        "weight": 40,
        "plan_types": ["PPO", "HMO", "POS", "HDHP"],
        "age_range": (18, 64),
        "premium_range": (650, 1300),
    },
    "Medicare Advantage": {
        "weight": 10,
        "plan_types": ["HMO", "PPO", "PFFS", "SNP"],
        "age_range": (65, 95),
        "premium_range": (900, 1500),
    },
    "Medicaid": {
        "weight": 20,
        "plan_types": ["Managed Care", "FFS"],
        "age_range": (0, 64),
        "premium_range": (400, 800),
    },
    "ACA Marketplace": {
        "weight": 30,
        "plan_types": ["Bronze", "Silver", "Gold", "Platinum"],
        "age_range": (18, 64),
        "premium_range": (500, 1000),
    },
}

# ---------------------------------------------------------------------------
# FWA (Fraud, Waste & Abuse) reference data
# ---------------------------------------------------------------------------

# Fraud types with weights (sum = 100)
FWA_FRAUD_TYPES = [
    ("duplicate_billing", "Duplicate Billing — Same member, date, and procedure billed on multiple claims", 20),
    ("upcoding", "Upcoding — Professional claims coded at higher E&M level than warranted", 20),
    ("unbundling", "Unbundling — Related CPT codes billed separately instead of bundled", 15),
    ("impossible_day", "Impossible Day — Provider billing >50 patients/day or >24 hrs of services", 8),
    ("phantom_billing", "Phantom Billing — Claims with no other clinical activity at provider within ±90 days", 7),
    ("provider_ring", "Provider Ring — Cluster of 3-5 providers sharing members at unusually high rates", 5),
    ("doctor_shopping", "Doctor Shopping — Member seeing 5+ providers for same diagnosis in 90 days", 10),
    ("short_refill", "Short Refill — Pharmacy fill where next fill occurs before 75% of days_supply", 8),
    ("drug_switching", "Drug Switching — Generic to brand-name switch within same therapeutic class in 60 days", 7),
]

# Detection methods
FWA_DETECTION_METHODS = [
    "rules_engine",
    "statistical_outlier",
    "peer_comparison",
    "temporal_pattern",
    "geographic_analysis",
    "network_analysis",
    "ai_model_v1",
    "tip_hotline",
    "audit_sample",
]

# Severity levels with weights
FWA_SEVERITY_LEVELS = [
    ("Critical", 10),
    ("High", 25),
    ("Medium", 40),
    ("Low", 25),
]

# Provider thresholds for anomaly detection
FWA_PROVIDER_THRESHOLDS = {
    "max_patients_per_day": 50,
    "max_hours_per_day": 24,
    "e5_visit_pct_threshold": 0.40,       # >40% level-5 visits is suspicious
    "billed_to_allowed_threshold": 2.0,    # >2x billed-to-allowed is suspicious
    "member_overlap_pct_threshold": 0.30,  # >30% shared members between providers
    "short_refill_threshold": 0.75,        # Next fill < 75% of days_supply
    "drug_switch_window_days": 60,         # Generic→brand switch window
    "doctor_shopping_providers": 5,        # 5+ providers for same dx in 90 days
    "doctor_shopping_window_days": 90,
}

# Investigation statuses for pre-seeded cases
FWA_INVESTIGATION_STATUSES = [
    ("Open", 25),
    ("Under Review", 20),
    ("Evidence Gathering", 20),
    ("Referred to SIU", 10),
    ("Recovery In Progress", 5),
    ("Closed — Confirmed Fraud", 10),
    ("Closed — No Fraud", 7),
    ("Closed — Insufficient Evidence", 3),
]

# ---------------------------------------------------------------------------
# Network Adequacy reference data
# ---------------------------------------------------------------------------

# Map existing Red Bricks specialties to CMS HSD specialty types
# Our 11 specialties → CMS 29 specialty types (many-to-one mapping for matching)
SPECIALTY_TO_CMS = {
    "Internal Medicine": "Primary Care",
    "Family Medicine": "Primary Care",
    "Pediatrics": "Primary Care",
    "Cardiology": "Cardiology",
    "Orthopedic Surgery": "Orthopedic Surgery",
    "General Surgery": "General Surgery",
    "Obstetrics/Gynecology": "Gynecology/OB-GYN",
    "Psychiatry": "Psychiatry",
    "Emergency Medicine": "Primary Care",  # EM maps to PCP for access
    "Gastroenterology": "Gastroenterology",
    "Oncology": "Oncology - Medical",
}

# Provider panel sizes by specialty (min, max)
PANEL_SIZE_BY_SPECIALTY = {
    "Internal Medicine": (800, 2500),
    "Family Medicine": (800, 2500),
    "Pediatrics": (600, 2000),
    "Cardiology": (300, 1200),
    "Orthopedic Surgery": (200, 800),
    "General Surgery": (200, 900),
    "Obstetrics/Gynecology": (400, 1500),
    "Psychiatry": (150, 600),
    "Emergency Medicine": (0, 0),  # EM doesn't have panels
    "Gastroenterology": (250, 1000),
    "Oncology": (150, 600),
}

# Telehealth capability rates by specialty
TELEHEALTH_RATE_BY_SPECIALTY = {
    "Internal Medicine": 0.55,
    "Family Medicine": 0.55,
    "Pediatrics": 0.45,
    "Cardiology": 0.30,
    "Orthopedic Surgery": 0.15,
    "General Surgery": 0.10,
    "Obstetrics/Gynecology": 0.35,
    "Psychiatry": 0.80,
    "Emergency Medicine": 0.05,
    "Gastroenterology": 0.25,
    "Oncology": 0.30,
}

# Appointment wait time ranges by specialty (min_days, max_days)
WAIT_TIME_BY_SPECIALTY = {
    "Internal Medicine": (3, 21),
    "Family Medicine": (3, 21),
    "Pediatrics": (2, 14),
    "Cardiology": (7, 45),
    "Orthopedic Surgery": (7, 42),
    "General Surgery": (7, 35),
    "Obstetrics/Gynecology": (5, 28),
    "Psychiatry": (14, 60),
    "Emergency Medicine": (0, 0),
    "Gastroenterology": (10, 45),
    "Oncology": (5, 30),
}

# Credentialing statuses with weights
CREDENTIALING_STATUSES = [
    ("Active", 85),
    ("Provisional", 8),
    ("Expired", 5),
    ("Suspended", 2),
]

# Languages spoken by providers in NC (language, probability of speaking)
PROVIDER_LANGUAGES = [
    ("English", 1.0),
    ("Spanish", 0.25),
    ("Mandarin", 0.05),
    ("Hindi", 0.04),
    ("Vietnamese", 0.03),
    ("Arabic", 0.03),
    ("French", 0.02),
    ("Korean", 0.02),
    ("Tagalog", 0.02),
]

# OON leakage reasons with weights
LEAKAGE_REASONS = [
    ("no_inn_available", 25),
    ("inn_wait_time", 20),
    ("member_choice", 25),
    ("referral", 15),
    ("emergency", 15),
]

# HCC codes for risk adjustment (simplified; code, description, typical factor)
HCC_CODES = [
    ("HCC18", "Diabetes with chronic complications", 0.4),
    ("HCC19", "Diabetes without complication", 0.2),
    ("HCC85", "Congestive heart failure", 0.5),
    ("HCC96", "Specified heart arrhythmias", 0.2),
    ("HCC111", "Chronic obstructive pulmonary disease", 0.4),
    ("HCC134", "Chronic kidney disease, stage 4-5", 0.6),
    ("HCC135", "Chronic kidney disease, stage 3", 0.2),
    ("HCC138", "Unstable angina and acute MI", 0.5),
]

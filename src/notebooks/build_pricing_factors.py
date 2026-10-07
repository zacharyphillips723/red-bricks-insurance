# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Governed Pricing Factor Tables
# MAGIC
# MAGIC Builds the actuarial rate build-up factor tables into Unity Catalog as a
# MAGIC governed Delta table (`analytics.gold_pricing_factors`), the source of truth
# MAGIC for the Underwriting Simulation app's `pricing_engine.py` (which keeps the
# MAGIC same values hardcoded only as a fallback).
# MAGIC
# MAGIC **Data-derived, not hand-seeded.** Where the gold experience tables support
# MAGIC it, the factors are *computed from actual claims experience* and normalized
# MAGIC to a reference band, so the curves reflect this book of business:
# MAGIC
# MAGIC | Factor | Derived from | Method |
# MAGIC |--------|--------------|--------|
# MAGIC | `base_rate` (PMPM by LOB) | `analytics.gold_pmpm` | avg paid PMPM grossed to an 85% target loss ratio |
# MAGIC | `industry_factor` | `analytics.gold_group_experience` | industry loss ratio ÷ book loss ratio (relativity) |
# MAGIC | `age_factor` | `claims.silver_claims_medical` × `members.silver_members` | paid-per-member by age band ÷ the 36-45 reference band |
# MAGIC | `area_factor` | — | curated default (needs a county→area-type crosswalk) |
# MAGIC | `trend_factor` | — | curated lookup (labelled annual-trend multipliers) |
# MAGIC | `experience_mod` | — | curated credibility-blend bounds |
# MAGIC
# MAGIC Each factor the query can't robustly derive falls back to the curated value,
# MAGIC and every row's `description` records whether it was data-derived or curated,
# MAGIC so provenance is auditable. Governing these in UC lets actuaries review,
# MAGIC version, and update rate assumptions without a code deploy — and the
# MAGIC in-app Factor Governance workflow publishes approved versions straight into
# MAGIC this same table.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

TABLE_NAME = f"{catalog_sql}.analytics.gold_pricing_factors"
# Target loss ratio used to gross derived paid-PMPM up to a community base premium.
TARGET_LOSS_RATIO = 0.85
print(f"Target: {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated defaults (fallback)
# MAGIC
# MAGIC Tidy (long) format — one row per factor. `factor_type` groups the curves;
# MAGIC `factor_key` is the band/category; `factor_value` is the multiplier (or base
# MAGIC premium for `base_rate`). These are the fallbacks; the next cell overrides
# MAGIC them with values derived from actual experience wherever possible.

# COMMAND ----------

# (factor_type, factor_key, factor_value, unit, description)
STATIC_ROWS = [
    # Base community rates (monthly PMPM starting point) by line of business
    ("base_rate", "Commercial", 385.00, "pmpm_usd", "Community-rated base monthly premium — Commercial"),
    ("base_rate", "Medicare Advantage", 925.00, "pmpm_usd", "Community-rated base monthly premium — Medicare Advantage"),
    ("base_rate", "Medicaid", 310.00, "pmpm_usd", "Community-rated base monthly premium — Medicaid"),
    ("base_rate", "Individual", 420.00, "pmpm_usd", "Community-rated base monthly premium — Individual/ACA"),

    # Age rating factors (ACA 3:1 compliant)
    ("age_factor", "0-17", 0.72, "multiplier", "Age rating factor — 0-17"),
    ("age_factor", "18-25", 0.85, "multiplier", "Age rating factor — 18-25"),
    ("age_factor", "26-35", 0.92, "multiplier", "Age rating factor — 26-35"),
    ("age_factor", "36-45", 1.00, "multiplier", "Age rating factor — 36-45 (reference band)"),
    ("age_factor", "46-55", 1.15, "multiplier", "Age rating factor — 46-55"),
    ("age_factor", "56-64", 1.25, "multiplier", "Age rating factor — 56-64"),
    ("age_factor", "65+", 1.45, "multiplier", "Age rating factor — 65+"),

    # Geographic area factors
    ("area_factor", "urban", 0.95, "multiplier", "Geographic area factor — urban"),
    ("area_factor", "suburban", 1.00, "multiplier", "Geographic area factor — suburban (reference)"),
    ("area_factor", "rural", 1.10, "multiplier", "Geographic area factor — rural"),

    # Industry (SIC) factors
    ("industry_factor", "healthcare", 1.15, "multiplier", "Industry risk factor — healthcare"),
    ("industry_factor", "office", 0.90, "multiplier", "Industry risk factor — office/clerical"),
    ("industry_factor", "manufacturing", 1.05, "multiplier", "Industry risk factor — manufacturing"),
    ("industry_factor", "technology", 0.88, "multiplier", "Industry risk factor — technology"),
    ("industry_factor", "retail", 0.98, "multiplier", "Industry risk factor — retail"),
    ("industry_factor", "construction", 1.12, "multiplier", "Industry risk factor — construction"),
    ("industry_factor", "education", 0.93, "multiplier", "Industry risk factor — education"),
    ("industry_factor", "finance", 0.91, "multiplier", "Industry risk factor — finance"),
    ("industry_factor", "hospitality", 1.02, "multiplier", "Industry risk factor — hospitality"),
    ("industry_factor", "transportation", 1.08, "multiplier", "Industry risk factor — transportation"),
    ("industry_factor", "government", 0.95, "multiplier", "Industry risk factor — government"),
    ("industry_factor", "agriculture", 1.06, "multiplier", "Industry risk factor — agriculture"),

    # Medical trend factors (annual)
    ("trend_factor", "7%", 1.07, "multiplier", "Annual medical trend factor — 7%"),
    ("trend_factor", "8%", 1.08, "multiplier", "Annual medical trend factor — 8%"),
    ("trend_factor", "9%", 1.09, "multiplier", "Annual medical trend factor — 9%"),
    ("trend_factor", "10%", 1.10, "multiplier", "Annual medical trend factor — 10%"),
    ("trend_factor", "11%", 1.11, "multiplier", "Annual medical trend factor — 11%"),
    ("trend_factor", "12%", 1.12, "multiplier", "Annual medical trend factor — 12%"),

    # Experience modification bounds (credibility-weighted blend)
    ("experience_mod", "min", 0.70, "multiplier", "Experience mod lower bound (best experience)"),
    ("experience_mod", "neutral", 1.00, "multiplier", "Experience mod neutral (no adjustment)"),
    ("experience_mod", "max", 1.40, "multiplier", "Experience mod upper bound (worst experience)"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive factors from actual experience
# MAGIC
# MAGIC Each derivation is independent and defensive: if a source table is missing
# MAGIC or returns no rows, that factor group silently keeps its curated default.

# COMMAND ----------

def _sql_rows(query: str) -> list:
    """Run a query, returning [] on any failure so derivation degrades gracefully."""
    try:
        return spark.sql(query).collect()
    except Exception as e:  # noqa: BLE001 — intentional broad fallback
        print(f"  [skip] {e}")
        return []


# Overrides keyed by (factor_type, factor_key) -> (value, provenance_note)
derived: dict = {}

# --- base_rate: avg paid PMPM by LOB, grossed to a community base premium ---
print("Deriving base_rate from analytics.gold_pmpm …")
for r in _sql_rows(f"""
    SELECT line_of_business AS lob, AVG(pmpm_paid) AS pmpm
    FROM {catalog_sql}.analytics.gold_pmpm
    WHERE pmpm_paid IS NOT NULL AND pmpm_paid > 0
    GROUP BY line_of_business
"""):
    lob = r["lob"]
    if not lob or r["pmpm"] is None:
        continue
    base = round(float(r["pmpm"]) / TARGET_LOSS_RATIO, 2)
    key = "Individual" if lob == "ACA Marketplace" else lob
    derived[("base_rate", key)] = (base, f"derived from gold_pmpm (avg paid PMPM / {TARGET_LOSS_RATIO:.0%} LR)")

# --- industry_factor: industry loss ratio ÷ book loss ratio (relativity) ---
print("Deriving industry_factor from analytics.gold_group_experience …")
book = _sql_rows(f"""
    SELECT SUM(total_claims_paid) AS claims, SUM(total_premium_revenue) AS premium
    FROM {catalog_sql}.analytics.gold_group_experience
""")
book_lr = None
if book and book[0]["premium"]:
    book_lr = float(book[0]["claims"] or 0) / float(book[0]["premium"])
if book_lr and book_lr > 0:
    for r in _sql_rows(f"""
        SELECT LOWER(industry) AS industry,
               SUM(total_claims_paid) AS claims,
               SUM(total_premium_revenue) AS premium
        FROM {catalog_sql}.analytics.gold_group_experience
        WHERE industry IS NOT NULL
        GROUP BY LOWER(industry)
        HAVING SUM(total_premium_revenue) > 0
    """):
        lr = float(r["claims"] or 0) / float(r["premium"])
        # Relativity vs the book, clamped to a sane rating range.
        factor = max(0.80, min(1.25, round(lr / book_lr, 3)))
        derived[("industry_factor", r["industry"])] = (
            factor, f"derived relativity (industry LR {lr:.2f} / book LR {book_lr:.2f})"
        )

# --- age_factor: paid-per-member by age band ÷ the 36-45 reference band ---
print("Deriving age_factor from claims × members …")
age_rows = _sql_rows(f"""
    WITH claim_age AS (
        SELECT c.member_id, c.paid_amount,
               FLOOR(DATEDIFF(c.service_from_date, m.date_of_birth) / 365.25) AS age
        FROM {catalog_sql}.claims.silver_claims_medical c
        JOIN {catalog_sql}.members.silver_members m ON c.member_id = m.member_id
        WHERE c.paid_amount IS NOT NULL AND m.date_of_birth IS NOT NULL
          AND c.service_from_date IS NOT NULL
    )
    SELECT CASE
             WHEN age <= 17 THEN '0-17'  WHEN age <= 25 THEN '18-25'
             WHEN age <= 35 THEN '26-35' WHEN age <= 45 THEN '36-45'
             WHEN age <= 55 THEN '46-55' WHEN age <= 64 THEN '56-64'
             ELSE '65+'
           END AS age_band,
           SUM(paid_amount) / NULLIF(COUNT(DISTINCT member_id), 0) AS cost_per_member
    FROM claim_age
    WHERE age >= 0 AND age <= 120
    GROUP BY 1
""")
age_cost = {r["age_band"]: float(r["cost_per_member"]) for r in age_rows if r["cost_per_member"]}
ref = age_cost.get("36-45")
if ref and ref > 0:
    for band, cost in age_cost.items():
        factor = max(0.5, min(2.0, round(cost / ref, 3)))
        derived[("age_factor", band)] = (factor, f"derived (paid/member {cost:,.0f} vs 36-45 ref {ref:,.0f})")

print(f"Derived {len(derived)} factor overrides from experience.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge derived over curated defaults and write

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, TimestampType,
)

effective = date(date.today().year, 1, 1)
now = datetime.now()

final_rows = []
for ftype, fkey, value, unit, desc in STATIC_ROWS:
    override = derived.pop((ftype, fkey), None)
    if override is not None:
        value, note = override
        desc = f"{desc} [data-derived: {note}]"
    else:
        desc = f"{desc} [curated default]"
    final_rows.append((ftype, fkey, float(value), unit, desc))

# Any derived keys not present in the curated set (e.g. a new LOB/industry) get added.
for (ftype, fkey), (value, note) in derived.items():
    unit = "pmpm_usd" if ftype == "base_rate" else "multiplier"
    final_rows.append((ftype, fkey, float(value), unit, f"{ftype} — {fkey} [data-derived: {note}]"))

schema = StructType([
    StructField("factor_type", StringType(), False),
    StructField("factor_key", StringType(), False),
    StructField("factor_value", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("description", StringType(), True),
    StructField("effective_date", DateType(), True),
    StructField("updated_at", TimestampType(), True),
])
df = spark.createDataFrame([Row(*r, effective, now) for r in final_rows], schema=schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.analytics")
(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{catalog}.analytics.gold_pricing_factors"))

spark.sql(f"""
    COMMENT ON TABLE {TABLE_NAME} IS
    'Governed actuarial rate build-up factors (base rates, age/area/industry/trend curves, experience-mod bounds) for the Underwriting Simulation portal. Base rates, industry, and age factors are DERIVED from this book of business (gold_pmpm, gold_group_experience, claims x members) and normalized to reference bands; area/trend/experience-mod are curated defaults. One row per factor; the description records provenance. Source of truth for the rate build-up pricing engine and the in-app Factor Governance publish workflow.'
""")

derived_n = sum(1 for d in final_rows if "data-derived" in d[4])
print(f"Wrote {df.count()} factor rows to {TABLE_NAME} ({derived_n} data-derived, {df.count() - derived_n} curated).")
display(spark.sql(f"""
    SELECT factor_type, COUNT(*) AS n,
           SUM(CASE WHEN description LIKE '%data-derived%' THEN 1 ELSE 0 END) AS derived
    FROM {TABLE_NAME} GROUP BY factor_type ORDER BY factor_type
"""))

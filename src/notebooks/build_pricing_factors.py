# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Governed Pricing Factor Tables
# MAGIC
# MAGIC Seeds the actuarial rate build-up factor tables into Unity Catalog as a
# MAGIC governed Delta table (`analytics.gold_pricing_factors`), replacing the
# MAGIC values that were hardcoded in the Underwriting Simulation app's
# MAGIC `pricing_engine.py`.
# MAGIC
# MAGIC **Why govern these in UC?** Rate factors (base rates, age/area/industry/
# MAGIC trend curves) are regulated pricing assumptions. Storing them in a
# MAGIC governed, versioned Delta table means actuaries can audit, lineage-track,
# MAGIC and update them without a code deploy — and Unity Catalog enforces who can
# MAGIC read vs. modify them. The app reads this table (with a hardcoded fallback
# MAGIC if it's absent), so pricing stays data-driven.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

TABLE_NAME = f"{catalog_sql}.analytics.gold_pricing_factors"
print(f"Target: {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Factor reference data
# MAGIC
# MAGIC Tidy (long) format — one row per factor so the table is easy to govern,
# MAGIC query, and extend. `factor_type` groups the curves; `factor_key` is the
# MAGIC band/category; `factor_value` is the multiplier (or base premium for
# MAGIC `base_rate`).

# COMMAND ----------

from datetime import date

# (factor_type, factor_key, factor_value, unit, description)
ROWS = [
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

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

effective = date(date.today().year, 1, 1)
schema = StructType([
    StructField("factor_type", StringType(), False),
    StructField("factor_key", StringType(), False),
    StructField("factor_value", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("description", StringType(), True),
    StructField("effective_date", DateType(), True),
])
df = spark.createDataFrame([Row(*r, effective) for r in ROWS], schema=schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.analytics")
(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{catalog}.analytics.gold_pricing_factors"))

spark.sql(f"""
    COMMENT ON TABLE {TABLE_NAME} IS
    'Governed actuarial rate build-up factors (base rates, age/area/industry/trend curves, experience-mod bounds) for the Underwriting Simulation portal. Tidy format: one row per factor. Source of truth for the rate build-up pricing engine — replaces hardcoded values so actuaries can audit and version pricing assumptions under Unity Catalog governance.'
""")

print(f"Wrote {df.count()} factor rows to {TABLE_NAME}")
display(spark.sql(f"SELECT factor_type, COUNT(*) AS n FROM {TABLE_NAME} GROUP BY factor_type ORDER BY factor_type"))

# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Unity Catalog Metric Views
# MAGIC
# MAGIC Creates **metric views** as a governed semantic layer on top of existing gold tables.
# MAGIC Metric views define measures and dimensions as YAML, queried with the `MEASURE()` function.
# MAGIC They ensure every consumer — actuaries, dashboards, Genie, AI/BI — computes metrics
# MAGIC the same way.
# MAGIC
# MAGIC **Why a notebook instead of SDP SQL?** Metric views are standalone UC objects
# MAGIC (`CREATE VIEW ... WITH METRICS`), not SDP constructs. They can't be defined inside
# MAGIC an SDP pipeline alongside `CREATE OR REFRESH MATERIALIZED VIEW`.
# MAGIC
# MAGIC **Requires:** Databricks Runtime 17.2+ or a SQL Warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# YAML block delimiter — injected as variable to prevent Databricks notebook
# parameter substitution from collapsing $$ to $
DD = "$$"

print(f"Creating metric views in: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_financial_overview
# MAGIC Core financial KPIs: PMPM (paid/allowed), total paid/allowed, member months.
# MAGIC Source: `gold_pmpm`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_financial_overview
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed financial KPIs — PMPM paid/allowed, total paid/allowed, member months. Source of truth for cost metrics across all consumers."
  source: {catalog}.{schema}.gold_pmpm
  dimensions:
    - name: line_of_business
      expr: line_of_business
    - name: service_year_month
      expr: service_year_month
  measures:
    - name: Total Paid
      expr: SUM(total_paid)
      comment: "Total paid claims amount"
    - name: Total Allowed
      expr: SUM(total_allowed)
      comment: "Total allowed claims amount"
    - name: PMPM Paid
      expr: SUM(total_paid) / NULLIF(SUM(member_months), 0)
      comment: "Per Member Per Month paid cost"
    - name: PMPM Allowed
      expr: SUM(total_allowed) / NULLIF(SUM(member_months), 0)
      comment: "Per Member Per Month allowed cost"
    - name: Member Months
      expr: SUM(member_months)
      comment: "Total member months of coverage exposure"
{DD}
""")

print("✓ mv_financial_overview created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_mlr_compliance
# MAGIC Medical Loss Ratio with ACA compliance tracking.
# MAGIC Source: `gold_mlr`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_mlr_compliance
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed MLR and admin ratio metrics with ACA compliance context."
  source: {catalog}.{schema}.gold_mlr
  dimensions:
    - name: line_of_business
      expr: line_of_business
    - name: service_year
      expr: service_year
  measures:
    - name: MLR
      expr: SUM(total_claims_paid) / NULLIF(SUM(total_premiums), 0)
      comment: "Medical Loss Ratio — ACA target >=80pct Commercial/ACA, >=85pct MA/Medicaid"
    - name: Total Claims Paid
      expr: SUM(total_claims_paid)
      comment: "Total medical + pharmacy claims paid"
    - name: Total Premiums
      expr: SUM(total_premiums)
      comment: "Total premium revenue collected"
    - name: Medical Claims
      expr: SUM(medical_claims_paid)
      comment: "Medical claims paid (excludes pharmacy)"
    - name: Pharmacy Claims
      expr: SUM(pharmacy_claims_paid)
      comment: "Pharmacy claims paid"
    - name: Admin Ratio
      expr: (SUM(total_premiums) - SUM(total_claims_paid)) / NULLIF(SUM(total_premiums), 0)
      comment: "Administrative cost ratio — proportion of premiums not paid out as claims"
{DD}
""")

print("✓ mv_mlr_compliance created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_utilization
# MAGIC Utilization rate metrics per 1,000 member months.
# MAGIC Source: `gold_utilization_per_1000`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_utilization
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed utilization benchmarks per 1,000 member months — standard actuarial rates for cross-LOB comparison."
  source: {catalog}.{schema}.gold_utilization_per_1000
  dimensions:
    - name: line_of_business
      expr: line_of_business
    - name: service_year
      expr: service_year
    - name: service_category
      expr: service_category
  measures:
    - name: Claims per 1000
      expr: SUM(total_claims) * 1000.0 / NULLIF(SUM(member_months), 0)
      comment: "Claims volume per 1,000 member months"
    - name: Patients per 1000
      expr: SUM(unique_patients) * 1000.0 / NULLIF(SUM(member_months), 0)
      comment: "Unique patients per 1,000 member months (prevalence)"
    - name: Cost per 1000
      expr: SUM(total_paid) * 1000.0 / NULLIF(SUM(member_months), 0)
      comment: "Total paid cost per 1,000 member months"
    - name: Admits per 1000
      expr: SUM(ip_admits) FILTER (WHERE service_category = 'Inpatient') * 1000.0 / NULLIF(SUM(member_months), 0)
      comment: "Inpatient admissions per 1,000 member months"
    - name: Avg Cost per Claim
      expr: SUM(total_paid) / NULLIF(SUM(total_claims), 0)
      comment: "Average cost per claim across service categories"
    - name: Total Claims
      expr: SUM(total_claims)
      comment: "Total claim count"
    - name: Member Months
      expr: SUM(member_months)
      comment: "Total member months of coverage exposure"
{DD}
""")

print("✓ mv_utilization created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_enrollment
# MAGIC Enrollment exposure metrics.
# MAGIC Source: `silver_member_months`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_enrollment
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed enrollment exposure metrics — member months, active members, premium revenue, and risk scores."
  source: {catalog}.{schema}.silver_member_months
  dimensions:
    - name: line_of_business
      expr: line_of_business
    - name: plan_type
      expr: plan_type
    - name: eligibility_year
      expr: eligibility_year
    - name: eligibility_month
      expr: eligibility_month
  measures:
    - name: Member Months
      expr: COUNT(*)
      comment: "Total member months of coverage (one row = one member-month)"
    - name: Active Members
      expr: COUNT(DISTINCT member_id)
      comment: "Distinct active members"
    - name: Avg Premium
      expr: AVG(monthly_premium)
      comment: "Average monthly premium per member-month"
    - name: Premium Revenue
      expr: SUM(monthly_premium)
      comment: "Total premium revenue"
    - name: Avg Risk Score
      expr: AVG(risk_score)
      comment: "Average member risk score (higher = sicker population)"
{DD}
""")

print("✓ mv_enrollment created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_ibnr
# MAGIC IBNR reserve indicators — payment lag and completion rates.
# MAGIC Source: `gold_ibnr_estimate`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_ibnr
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed IBNR reserve indicators — payment lag, completion rates, and outstanding claims beyond 90 days."
  source: {catalog}.{schema}.gold_ibnr_estimate
  dimensions:
    - name: service_year_month
      expr: service_year_month
  measures:
    - name: Avg Payment Lag Days
      expr: AVG(avg_lag_days)
      comment: "Average days between service date and payment date"
    - name: Completion Rate
      expr: SUM(claims_under_30_days + claims_30_to_90) * 1.0 / NULLIF(SUM(total_claims), 0)
      comment: "Proportion of claims settled within 90 days"
    - name: Claims Over 90 Days Pct
      expr: SUM(claims_90_to_180 + claims_over_180) * 1.0 / NULLIF(SUM(total_claims), 0)
      comment: "Proportion of claims still outstanding beyond 90 days — higher means greater reserve need"
    - name: Total Claims
      expr: SUM(total_claims)
      comment: "Total claim count for the service period"
{DD}
""")

print("✓ mv_ibnr created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_denials
# MAGIC Denial financial impact metrics.
# MAGIC Source: `gold_denial_analysis`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_denials
WITH METRICS
LANGUAGE YAML
AS {DD}
  version: 1.1
  comment: "Governed denial financial impact — denial counts, denied amounts, and averages by AI-classified category."
  source: {catalog}.{schema}.gold_denial_analysis
  dimensions:
    - name: denial_category
      expr: denial_category
    - name: line_of_business
      expr: line_of_business
    - name: claim_type
      expr: claim_type
  measures:
    - name: Denial Count
      expr: SUM(denial_count)
      comment: "Total number of denied claims"
    - name: Total Denied Amount
      expr: SUM(total_denied_amount)
      comment: "Total dollar amount of denied claims"
    - name: Avg Denied Amount
      expr: SUM(total_denied_amount) / NULLIF(SUM(denial_count), 0)
      comment: "Average denied amount per denial"
{DD}
""")

print("✓ mv_denials created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC List all metric views and run sample `MEASURE()` queries.

# COMMAND ----------

print("=" * 60)
print("Metric Views Created")
print("=" * 60)

metric_views = ["mv_financial_overview", "mv_mlr_compliance", "mv_utilization",
                "mv_enrollment", "mv_ibnr", "mv_denials"]

for mv in metric_views:
    try:
        cols = spark.sql(f"DESCRIBE {catalog}.{schema}.{mv}").collect()
        print(f"\n✓ {mv} — {len(cols)} columns")
    except Exception as e:
        print(f"\n✗ {mv} — ERROR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample MEASURE() Queries

# COMMAND ----------

# PMPM by line of business
print("PMPM by Line of Business:")
display(spark.sql(f"""
    SELECT `line_of_business`, MEASURE(`PMPM Paid`) AS pmpm_paid, MEASURE(`Member Months`) AS member_months
    FROM {catalog}.{schema}.mv_financial_overview
    GROUP BY `line_of_business`
    ORDER BY pmpm_paid DESC
"""))

# COMMAND ----------

# MLR compliance by LOB
print("MLR by Line of Business:")
display(spark.sql(f"""
    SELECT `line_of_business`, `service_year`, MEASURE(`MLR`) AS mlr, MEASURE(`Admin Ratio`) AS admin_ratio
    FROM {catalog}.{schema}.mv_mlr_compliance
    GROUP BY `line_of_business`, `service_year`
    ORDER BY `service_year` DESC, mlr DESC
"""))

# COMMAND ----------

# Utilization per 1,000 by service category
print("Utilization per 1,000 by Service Category:")
display(spark.sql(f"""
    SELECT `service_category`, `line_of_business`,
           MEASURE(`Claims per 1000`) AS claims_per_1000,
           MEASURE(`Cost per 1000`) AS cost_per_1000
    FROM {catalog}.{schema}.mv_utilization
    GROUP BY `service_category`, `line_of_business`
    ORDER BY cost_per_1000 DESC
"""))

# COMMAND ----------

# Enrollment summary
print("Enrollment by LOB:")
display(spark.sql(f"""
    SELECT `line_of_business`, MEASURE(`Member Months`) AS member_months,
           MEASURE(`Active Members`) AS active_members, MEASURE(`Premium Revenue`) AS premium_revenue
    FROM {catalog}.{schema}.mv_enrollment
    GROUP BY `line_of_business`
    ORDER BY member_months DESC
"""))

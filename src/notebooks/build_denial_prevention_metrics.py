# Databricks notebook source
# MAGIC %md
# MAGIC # Build Denial-Prevention Metrics (gold source for the metric view)
# MAGIC
# MAGIC Aggregates claims + model propensity into a governed gold table that powers
# MAGIC the `mv_denial_prevention` Unity Catalog metric view. Grain:
# MAGIC **(service_year_month × line_of_business × reason_category)**.
# MAGIC
# MAGIC "Preventable" denial categories are the ones a provider can fix *before*
# MAGIC submission — missing_info, no_auth, coding_mismatch, frequency_limit — which
# MAGIC is exactly what the Claim Scrubber targets. This lets the business quantify
# MAGIC preventable denied dollars, first-pass denial rate, and avg denial propensity
# MAGIC through one governed semantic layer.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"

# Denial reason categories a provider can remediate pre-submission.
PREVENTABLE = ("missing_info", "no_auth", "coding_mismatch", "frequency_limit")
_preventable_sql = ", ".join(f"'{c}'" for c in PREVENTABLE)

print(f"Building {catalog}.{CLAIMS_SCHEMA}.gold_denial_prevention")

# COMMAND ----------

# One row per (month, LOB, reason_category). Non-denied claims land under
# reason_category = 'not_denied' so the denominator (claim_count) is complete and
# the first-pass denial rate is well defined. avg propensity is carried as a
# sum + count so the metric view can average correctly across any grouping.
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_sql}.{CLAIMS_SCHEMA}.gold_denial_prevention AS
WITH scored AS (
    SELECT claim_id, denial_prob
    FROM {catalog_sql}.{CLAIMS_SCHEMA}.gold_denial_risk_scores
),
lob AS (
    SELECT member_id, MAX(line_of_business) AS line_of_business
    FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment
    GROUP BY member_id
),
enriched AS (
    SELECT
        c.service_year_month,
        COALESCE(l.line_of_business, 'Unknown')                    AS line_of_business,
        CASE
            WHEN LOWER(c.claim_status) = 'denied'
                THEN COALESCE(ref.reason_category, 'other')
            ELSE 'not_denied'
        END                                                        AS reason_category,
        CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END AS is_denied,
        c.billed_amount,
        s.denial_prob
    FROM {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c
    LEFT JOIN lob l          ON c.member_id = l.member_id
    LEFT JOIN scored s       ON c.claim_id = s.claim_id
    LEFT JOIN {catalog_sql}.{CLAIMS_SCHEMA}.carc_reference ref
                             ON c.denial_reason_code = ref.carc_code
)
SELECT
    service_year_month,
    line_of_business,
    reason_category,
    COUNT(*)                                                       AS claim_count,
    SUM(is_denied)                                                 AS denied_count,
    SUM(CASE WHEN is_denied = 1 THEN billed_amount ELSE 0 END)     AS denied_amount,
    SUM(CASE WHEN is_denied = 1 AND reason_category IN ({_preventable_sql})
             THEN billed_amount ELSE 0 END)                        AS preventable_denied_amount,
    SUM(COALESCE(denial_prob, 0.0))                                AS sum_denial_propensity,
    SUM(CASE WHEN denial_prob IS NOT NULL THEN 1 ELSE 0 END)       AS scored_claim_count
FROM enriched
GROUP BY service_year_month, line_of_business, reason_category
""")

# COMMAND ----------

n = spark.sql(f"SELECT COUNT(*) c FROM {catalog_sql}.{CLAIMS_SCHEMA}.gold_denial_prevention").first()["c"]
print(f"Wrote {n} rows to {catalog}.{CLAIMS_SCHEMA}.gold_denial_prevention")
display(spark.sql(f"""
    SELECT reason_category,
           SUM(denied_count) AS denials,
           ROUND(SUM(preventable_denied_amount), 0) AS preventable_denied_usd
    FROM {catalog_sql}.{CLAIMS_SCHEMA}.gold_denial_prevention
    GROUP BY reason_category ORDER BY denials DESC
"""))

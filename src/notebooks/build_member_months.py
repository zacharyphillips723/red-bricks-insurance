# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Build Silver Member Months
# MAGIC
# MAGIC Explodes enrollment spans into one row per member per month of active coverage.
# MAGIC This table is the actuarial-correct denominator for PMPM, utilization per 1,000,
# MAGIC and MLR calculations.
# MAGIC
# MAGIC **Why a standalone notebook?** The `EXPLODE(SEQUENCE(...))` pattern is too slow
# MAGIC inside SDP materialized views due to streaming state management overhead.
# MAGIC Running as a regular Spark SQL job completes in seconds.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

catalog = dbutils.widgets.get("catalog")

TABLE_NAME = f"{catalog}.members.silver_member_months"
SOURCE_TABLE = f"{catalog}.members.silver_enrollment"

print(f"Source:  {SOURCE_TABLE}")
print(f"Target:  {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Member Months

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME}
    COMMENT 'Exploded member-month enrollment records — one row per member per month of active coverage. Serves as the denominator for PMPM, utilization per 1,000, and MLR calculations.'
    AS
    SELECT
      member_id,
      subscriber_id,
      eligibility_month,
      YEAR(eligibility_month)  AS eligibility_year,
      MONTH(eligibility_month) AS eligibility_month_num,
      line_of_business,
      plan_type,
      group_number,
      monthly_premium,
      risk_score
    FROM (
      SELECT
        member_id,
        subscriber_id,
        line_of_business,
        plan_type,
        group_number,
        monthly_premium,
        risk_score,
        EXPLODE(
          SEQUENCE(
            DATE_TRUNC('month', eligibility_start_date),
            DATE_TRUNC('month', COALESCE(eligibility_end_date, CURRENT_DATE())),
            INTERVAL 1 MONTH
          )
        ) AS eligibility_month
      FROM {SOURCE_TABLE}
      WHERE eligibility_start_date IS NOT NULL
        AND DATE_TRUNC('month', eligibility_start_date)
            <= DATE_TRUNC('month', COALESCE(eligibility_end_date, CURRENT_DATE()))
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE_NAME}").collect()[0]["cnt"]
member_count = spark.sql(f"SELECT COUNT(DISTINCT member_id) AS cnt FROM {TABLE_NAME}").collect()[0]["cnt"]
avg_months = round(row_count / member_count, 1) if member_count > 0 else 0

print(f"silver_member_months built successfully:")
print(f"  Total rows:    {row_count:,}")
print(f"  Members:       {member_count:,}")
print(f"  Avg months/member: {avg_months}")

display(spark.sql(f"""
    SELECT line_of_business, COUNT(*) AS member_months, COUNT(DISTINCT member_id) AS members
    FROM {TABLE_NAME}
    GROUP BY line_of_business
    ORDER BY member_months DESC
"""))

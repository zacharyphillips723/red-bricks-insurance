# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Group Reporting Governed Tables
# MAGIC
# MAGIC Provisions two governed UC Delta tables for the Group Reporting Portal:
# MAGIC
# MAGIC 1. **`analytics.gold_competitive_benchmarks`** — carrier benchmark reference
# MAGIC    data (PMPM index, network size, wellness programs, satisfaction) that the
# MAGIC    competitive-benchmark view reads, replacing values previously fabricated
# MAGIC    in app code. Governed + auditable.
# MAGIC 2. **`analytics.group_renewal_scenarios`** — write-back target for renewal
# MAGIC    rate-change scenarios modeled in the app (traceability / lineage around
# MAGIC    what was modeled, by whom, when). Empty at seed time; the app appends.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

BENCH_TABLE = f"{catalog_sql}.analytics.gold_competitive_benchmarks"
SCEN_TABLE = f"{catalog_sql}.analytics.group_renewal_scenarios"
print(f"Benchmark table: {BENCH_TABLE}")
print(f"Renewal-scenario table: {SCEN_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Governed competitive-benchmark reference table
# MAGIC
# MAGIC One row per (carrier × group-size tier). `pmpm_index` is a multiplier
# MAGIC applied to the Red Bricks PMPM (so the benchmark scales with the actual
# MAGIC group), keeping the data realistic without hardcoding dollar amounts.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.analytics")

# (carrier, network, size_tier, pmpm_index, member_satisfaction, network_size, wellness_programs)
BENCH = [
    ("Anthem BlueCross", "Broad PPO", "Small", 1.04, 3.8, "12,000+ providers", "Wellness Rewards|Telehealth|Mental Health EAP"),
    ("Anthem BlueCross", "Broad PPO", "Mid-Market", 1.02, 3.9, "15,000+ providers", "Wellness Rewards|Telehealth|Mental Health EAP"),
    ("Anthem BlueCross", "Broad PPO", "Large", 0.99, 3.9, "20,000+ providers", "Wellness Rewards|Telehealth|Mental Health EAP"),
    ("Anthem BlueCross", "Broad PPO", "Jumbo", 0.97, 4.0, "25,000+ providers", "Wellness Rewards|Telehealth|Mental Health EAP"),
    ("UnitedHealthcare", "National PPO", "Small", 1.06, 3.5, "18,000+ providers", "Rally Digital Health|Real Appeal|Fitness Discounts"),
    ("UnitedHealthcare", "National PPO", "Mid-Market", 1.03, 3.6, "22,000+ providers", "Rally Digital Health|Real Appeal|Fitness Discounts"),
    ("UnitedHealthcare", "National PPO", "Large", 1.00, 3.7, "30,000+ providers", "Rally Digital Health|Real Appeal|Fitness Discounts"),
    ("UnitedHealthcare", "National PPO", "Jumbo", 0.98, 3.7, "35,000+ providers", "Rally Digital Health|Real Appeal|Fitness Discounts"),
    ("Aetna", "Open Choice PPO", "Small", 1.05, 3.6, "14,000+ providers", "Attain Wellness|Mindfulness Programs|Chronic Care"),
    ("Aetna", "Open Choice PPO", "Mid-Market", 1.02, 3.7, "17,000+ providers", "Attain Wellness|Mindfulness Programs|Chronic Care"),
    ("Aetna", "Open Choice PPO", "Large", 1.00, 3.8, "22,000+ providers", "Attain Wellness|Mindfulness Programs|Chronic Care"),
    ("Aetna", "Open Choice PPO", "Jumbo", 0.98, 3.8, "28,000+ providers", "Attain Wellness|Mindfulness Programs|Chronic Care"),
    ("Cigna", "OAP Network", "Small", 1.03, 3.4, "11,000+ providers", "Cigna Wellbeing|Health Coaches|Diabetes Prevention"),
    ("Cigna", "OAP Network", "Mid-Market", 1.01, 3.5, "14,000+ providers", "Cigna Wellbeing|Health Coaches|Diabetes Prevention"),
    ("Cigna", "OAP Network", "Large", 0.99, 3.6, "18,000+ providers", "Cigna Wellbeing|Health Coaches|Diabetes Prevention"),
    ("Cigna", "OAP Network", "Jumbo", 0.97, 3.6, "22,000+ providers", "Cigna Wellbeing|Health Coaches|Diabetes Prevention"),
]

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
bench_schema = StructType([
    StructField("carrier_name", StringType(), False),
    StructField("network", StringType(), True),
    StructField("size_tier", StringType(), False),
    StructField("pmpm_index", DoubleType(), True),
    StructField("member_satisfaction", DoubleType(), True),
    StructField("network_size", StringType(), True),
    StructField("wellness_programs", StringType(), True),  # pipe-delimited
])
bench_df = spark.createDataFrame(BENCH, bench_schema)
(bench_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(f"{catalog}.analytics.gold_competitive_benchmarks"))
spark.sql(f"""COMMENT ON TABLE {BENCH_TABLE} IS
    'Governed competitive-carrier benchmark reference (PMPM index by carrier x group-size tier, network size, wellness programs, member satisfaction) for the Group Reporting competitive-benchmark view. Replaces values previously fabricated in app code so benchmarks are auditable and versioned.'""")
print(f"Wrote {bench_df.count()} benchmark rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Renewal-scenario write-back table (traceability)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCEN_TABLE} (
        scenario_id STRING,
        group_id STRING,
        group_name STRING,
        rate_change_pct DOUBLE,
        projected_loss_ratio DOUBLE,
        projected_churn_pct DOUBLE,
        baseline_premium DOUBLE,
        projected_premium DOUBLE,
        narrative STRING,
        created_by STRING,
        created_at TIMESTAMP
    )
    COMMENT 'Write-back log of renewal rate-change scenarios modeled in the Group Reporting Portal — traceability/lineage for what-if pricing analysis (who modeled what, when).'
""")
print("Renewal-scenario write-back table ready.")
display(spark.sql(f"SELECT * FROM {BENCH_TABLE} LIMIT 5"))

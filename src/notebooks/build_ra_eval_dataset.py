# Databricks notebook source
# MAGIC %md
# MAGIC # Build the RA golden evaluation dataset (Phase 4)
# MAGIC Ground truth comes free from the coherent evidence layer (Phase 1): `suspected` = should-find,
# MAGIC `weak` = should-flag, anything else the agent surfaces = over-coding. See DESIGN.md §15.2.
# COMMAND ----------
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("n", "50", "Number of members")
catalog = dbutils.widgets.get("catalog"); n = int(dbutils.widgets.get("n"))
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.analytics.ra_eval_golden AS
SELECT member_id, suspected_hccs AS expected_suspected, weak_hccs AS expected_weak,
       potential_raf_gain, raf_at_radv_risk
FROM {catalog}.risk_adjustment.v_member_hcc_coherent
WHERE suspected_hccs <> '' ORDER BY potential_raf_gain DESC LIMIT {n}
""")
display(spark.table(f"{catalog}.analytics.ra_eval_golden"))

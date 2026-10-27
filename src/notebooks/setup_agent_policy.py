# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — RA agent org priority context (`risk_adjustment.agent_policy`)
# MAGIC
# MAGIC Phase 2. The **org priority context**: authoritative, governed in Unity Catalog,
# MAGIC **read-only to the agent**, and it **wins every conflict** with agent self-authored
# MAGIC memory (DESIGN.md §8.1). Grant the app service principal `SELECT` only.

# COMMAND ----------
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS risk_adjustment")
spark.sql("""
CREATE OR REPLACE TABLE risk_adjustment.agent_policy (
  policy_key STRING, policy_text STRING, severity STRING, active BOOLEAN, policy_version INT
)
""")

POLICIES = [
    ("never_auto_submit",       "The agent must never submit HCCs to RAPS/EDPS. Export is always a human action.", "hard", True, 1),
    ("human_adjudication",      "Every suggested HCC requires explicit coder adjudication: code, reject, or query provider.", "hard", True, 1),
    ("deterministic_grouping",  "HCC grouping must use reference.cms_hcc_v28_grouper. Never infer the HCC category with the LLM.", "hard", True, 1),
    ("high_value_signoff",      "High-value HCCs (e.g. HCC134 CKD 4-5/ESRD, HCC138 acute MI) require explicit sign-off with the cited chart passage.", "hard", True, 1),
    ("memory_subordinate",      "Agent self-authored memory (lessons, prefs) is advisory and never overrides this policy.", "hard", True, 1),
    ("allowlisted_schemas",     "Agent read access is limited to: clinical, documents, claims, pharmacy, members, risk_adjustment, analytics, reference.", "hard", True, 1),
    ("phi_handling",            "PHI is masked at the data layer. Do not echo raw PHI into lessons, logs, or traces.", "hard", True, 1),
    ("suspect_requires_citation","Never surface a suspected HCC without a cited note chunk as evidence.", "hard", True, 1),
]
df = spark.createDataFrame(POLICIES, "policy_key string, policy_text string, severity string, active boolean, policy_version int")
df.write.mode("overwrite").saveAsTable("risk_adjustment.agent_policy")

spark.sql("""COMMENT ON TABLE risk_adjustment.agent_policy IS
  'Org priority context (read-only to the RA agent). Authoritative; wins all conflicts with agent self-authored memory. See DESIGN.md 8.1.'""")

# COMMAND ----------
# MAGIC %md After the app service principal exists, lock it to read-only, e.g.:
# MAGIC ```sql
# MAGIC GRANT SELECT ON TABLE risk_adjustment.agent_policy TO `<app-service-principal>`;
# MAGIC ```
display(spark.sql("SELECT policy_key, severity, active FROM risk_adjustment.agent_policy ORDER BY policy_key"))

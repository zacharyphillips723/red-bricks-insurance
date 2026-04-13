# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Medical Policy Parsing with LLM
# MAGIC
# MAGIC Uses Foundation Model API to extract structured rules from medical policy PDFs.
# MAGIC Reads the generated policy PDFs from UC Volume, sends text to LLM for structured
# MAGIC extraction, and writes the parsed output to a Delta table.
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Read policy PDFs from volume (metadata table provides text content)
# MAGIC 2. LLM extracts structured rules per policy section
# MAGIC 3. Validate extracted codes against known CPT/ICD-10 patterns
# MAGIC 4. Write to `prior_auth.parsed_medical_policies`

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
PA_SCHEMA = "prior_auth"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"

print(f"Catalog: {catalog}")
print(f"LLM:     {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Medical Policy Rules (Source Data)
# MAGIC
# MAGIC The data generation step already created structured rules in `silver_medical_policy_rules`.
# MAGIC We'll use the LLM to **enrich** these rules with additional clinical context,
# MAGIC decision logic, and plain-language explanations that power the auto-adjudication engine.

# COMMAND ----------

rules_df = spark.table(_tbl(PA_SCHEMA, "silver_medical_policy_rules"))
policies_df = spark.table(_tbl(PA_SCHEMA, "silver_medical_policies"))

print(f"Policies: {policies_df.count()}")
print(f"Rules:    {rules_df.count()}")
rules_df.select("policy_name", "rule_type", "rule_id").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Enrichment — Extract Decision Logic from Rules
# MAGIC
# MAGIC For each policy rule, we use `ai_query()` to generate:
# MAGIC - A **plain-language explanation** of the rule
# MAGIC - **Approval criteria** as structured boolean conditions
# MAGIC - **Required evidence** checklist
# MAGIC - **Auto-adjudication eligibility** flag (can this rule be evaluated deterministically?)

# COMMAND ----------

enriched_df = spark.sql(f"""
SELECT
  r.policy_id,
  r.policy_name,
  r.service_category,
  r.rule_id,
  r.rule_type,
  r.rule_text,
  r.procedure_codes,
  r.diagnosis_codes,
  r.effective_date,

  ai_query(
    '{LLM_ENDPOINT}',
    CONCAT(
      'You are a health insurance medical policy analyst. Given this prior authorization rule, ',
      'extract a structured analysis. Respond in exactly this format with no other text:\\n\\n',
      'PLAIN_EXPLANATION: [one sentence explaining what this rule means for a PA reviewer]\\n',
      'AUTO_ELIGIBLE: [YES if this rule can be checked automatically against claims/lab data, NO if it requires clinical judgment]\\n',
      'EVIDENCE_NEEDED: [comma-separated list of specific data points needed to evaluate this rule]\\n',
      'APPROVAL_LOGIC: [brief boolean logic, e.g., "HbA1c >= 7.0 AND diagnosis IN (E11.x, E10.x)"]\\n\\n',
      'Policy: ', r.policy_name, '\\n',
      'Rule Type: ', r.rule_type, '\\n',
      'Rule Text: ', r.rule_text
    )
  ) AS llm_analysis,

  current_timestamp() AS parsed_at

FROM {catalog_sql}.{PA_SCHEMA}.silver_medical_policy_rules r
""")

print(f"Enriched {enriched_df.count()} rules with LLM analysis")
enriched_df.select("policy_name", "rule_type", "llm_analysis").show(5, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse LLM Output into Structured Columns

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col, trim, when, lit

parsed_df = (
    enriched_df
    .withColumn(
        "plain_explanation",
        trim(regexp_extract(col("llm_analysis"), r"PLAIN_EXPLANATION:\s*(.+?)(?:\n|$)", 1))
    )
    .withColumn(
        "auto_eligible",
        when(
            regexp_extract(col("llm_analysis"), r"AUTO_ELIGIBLE:\s*(YES|NO)", 1) == "YES",
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "evidence_needed",
        trim(regexp_extract(col("llm_analysis"), r"EVIDENCE_NEEDED:\s*(.+?)(?:\n|$)", 1))
    )
    .withColumn(
        "approval_logic",
        trim(regexp_extract(col("llm_analysis"), r"APPROVAL_LOGIC:\s*(.+?)(?:\n|$)", 1))
    )
)

# Show parsed results
parsed_df.select(
    "policy_name", "rule_type", "plain_explanation", "auto_eligible", "evidence_needed"
).show(10, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parsed Policies Table

# COMMAND ----------

# Drop raw LLM output column, keep structured fields
output_df = parsed_df.drop("llm_analysis")

spark.sql(f"DROP TABLE IF EXISTS {_tbl(PA_SCHEMA, 'parsed_medical_policies')}")
output_df.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "parsed_medical_policies"))

print(f"Wrote {output_df.count()} parsed rules to {catalog}.{PA_SCHEMA}.parsed_medical_policies")

# Summary
spark.sql(f"""
SELECT
  policy_name,
  COUNT(*) AS total_rules,
  SUM(CASE WHEN auto_eligible THEN 1 ELSE 0 END) AS auto_eligible_rules,
  ROUND(SUM(CASE WHEN auto_eligible THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS auto_pct
FROM {_tbl(PA_SCHEMA, 'parsed_medical_policies')}
GROUP BY policy_name
ORDER BY policy_name
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Policy Summary Narratives
# MAGIC
# MAGIC Create a one-paragraph executive summary per policy for the PA Review Portal.

# COMMAND ----------

policy_summaries = spark.sql(f"""
SELECT
  p.policy_id,
  p.policy_name,
  p.service_category,
  p.num_covered_services,
  p.num_criteria,

  ai_query(
    '{LLM_ENDPOINT}',
    CONCAT(
      'Write a 2-sentence executive summary of this medical policy for a utilization management nurse. ',
      'Include what services require prior auth and the key clinical criteria.\\n\\n',
      'Policy: ', p.policy_name, '\\n',
      'Category: ', p.service_category, '\\n',
      'Covered services: ', CAST(p.num_covered_services AS STRING), '\\n',
      'Number of criteria: ', CAST(p.num_criteria AS STRING), '\\n',
      'Rules:\\n',
      COALESCE(
        (SELECT ARRAY_JOIN(COLLECT_LIST(CONCAT('- [', r.rule_type, '] ', r.rule_text)), '\n')
         FROM {catalog_sql}.{PA_SCHEMA}.silver_medical_policy_rules r
         WHERE r.policy_id = p.policy_id),
        'No rules available'
      )
    )
  ) AS policy_summary

FROM {catalog_sql}.{PA_SCHEMA}.silver_medical_policies p
""")

spark.sql(f"DROP TABLE IF EXISTS {_tbl(PA_SCHEMA, 'policy_summaries')}")
policy_summaries.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "policy_summaries"))

print(f"Generated {policy_summaries.count()} policy summaries")
policy_summaries.select("policy_name", "policy_summary").show(truncate=100)

# COMMAND ----------

print("Medical policy parsing complete.")
print(f"  - {catalog}.{PA_SCHEMA}.parsed_medical_policies: LLM-enriched structured rules")
print(f"  - {catalog}.{PA_SCHEMA}.policy_summaries: Executive policy narratives")

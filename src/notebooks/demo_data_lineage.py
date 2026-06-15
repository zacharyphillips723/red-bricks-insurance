# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lineage Demo — Red Bricks Insurance
# MAGIC
# MAGIC Demonstrates Unity Catalog's **end-to-end data lineage** across the Red Bricks Insurance
# MAGIC lakehouse. This notebook traces data from raw ingestion through bronze/silver/gold layers
# MAGIC into AI models, dashboards, and apps — showing how Databricks tracks every transformation.
# MAGIC
# MAGIC ### What You'll See
# MAGIC 1. **Table lineage** — upstream/downstream dependencies across the medallion architecture
# MAGIC 2. **Column lineage** — field-level provenance from source to gold
# MAGIC 3. **AI function lineage** — how UC AI functions connect to governed tables
# MAGIC 4. **Pipeline-to-dashboard** lineage — SDP expectations → gold views → AI/BI dashboards
# MAGIC 5. **Cross-domain impact analysis** — "if I change this source field, what breaks?"

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat = f"`{catalog}`"

spark.sql(f"USE CATALOG {cat}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table-Level Lineage — Medallion Architecture
# MAGIC
# MAGIC Unity Catalog automatically tracks lineage for all SDP pipeline tables. Let's query the
# MAGIC system tables to see the full dependency graph.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upstream lineage for gold_member_360: what feeds into the Member 360 view?
# MAGIC SELECT
# MAGIC   source_table_full_name AS upstream_table,
# MAGIC   target_table_full_name AS downstream_table,
# MAGIC   source_type,
# MAGIC   target_type
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE '%gold_member_360%'
# MAGIC   AND source_table_catalog = target_table_catalog
# MAGIC ORDER BY upstream_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize the Lineage Graph
# MAGIC
# MAGIC The `gold_member_360` view pulls from **6+ domain tables** — members, enrollment, claims,
# MAGIC clinical, risk adjustment, and care management. Each of those has its own bronze → silver
# MAGIC pipeline with data quality expectations.

# COMMAND ----------

lineage_df = spark.sql(f"""
SELECT
  source_table_full_name AS upstream,
  target_table_full_name AS downstream,
  source_type,
  target_type
FROM system.access.table_lineage
WHERE (
  target_table_full_name LIKE '%{catalog}%'
  OR source_table_full_name LIKE '%{catalog}%'
)
AND source_table_catalog = target_table_catalog
ORDER BY downstream, upstream
""")

lineage_count = lineage_df.count()
print(f"Total lineage edges tracked: {lineage_count}")
lineage_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Column-Level Lineage
# MAGIC
# MAGIC Unity Catalog also tracks **field-level** lineage. This is critical for PHI governance —
# MAGIC when you need to know "where did this SSN come from?" or "what downstream reports use date_of_birth?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column lineage: trace member_id from source to gold
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   source_column_name,
# MAGIC   target_table_full_name,
# MAGIC   target_column_name
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_column_name = 'member_id'
# MAGIC   AND source_table_full_name LIKE '%members%silver_members%'
# MAGIC ORDER BY target_table_full_name
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC ### PHI Column Tracing
# MAGIC
# MAGIC For HIPAA compliance, we need to trace every PHI field. Let's see where `date_of_birth`
# MAGIC and `ssn_last_4` flow through the system — and confirm our column masks protect them
# MAGIC at every access point.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trace PHI columns: where does date_of_birth appear downstream?
# MAGIC SELECT
# MAGIC   source_table_full_name AS source_table,
# MAGIC   source_column_name AS phi_column,
# MAGIC   target_table_full_name AS downstream_table,
# MAGIC   target_column_name AS downstream_column
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_column_name IN ('date_of_birth', 'ssn_last_4', 'phone', 'email', 'address_line_1')
# MAGIC   AND source_table_full_name LIKE '%members%'
# MAGIC ORDER BY source_column_name, target_table_full_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AI Function Lineage
# MAGIC
# MAGIC Our Unity Catalog AI functions (in `ai_tools` schema) query governed tables. UC tracks
# MAGIC this lineage too — so you can see that `get_member_profile()` reads from `gold_member_360`,
# MAGIC which reads from `silver_members`, which reads from `bronze_members`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What tables do our AI tool functions depend on?
# MAGIC SELECT
# MAGIC   source_table_full_name AS queried_table,
# MAGIC   target_table_full_name AS ai_function,
# MAGIC   target_type
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE '%ai_tools%'
# MAGIC ORDER BY ai_function, queried_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Impact Analysis: "What breaks if I change `silver_members`?"
# MAGIC
# MAGIC This is a common question in production — before modifying a table, you need to know
# MAGIC every downstream dependency: views, functions, dashboards, and models.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Downstream impact of silver_members
# MAGIC SELECT
# MAGIC   target_table_full_name AS impacted_asset,
# MAGIC   target_type AS asset_type,
# MAGIC   COUNT(*) AS dependency_count
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name LIKE '%silver_members%'
# MAGIC GROUP BY target_table_full_name, target_type
# MAGIC ORDER BY dependency_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Expectations — Pipeline Health
# MAGIC
# MAGIC SDP pipelines enforce data quality via `EXPECT` constraints. Let's query the system tables
# MAGIC to see how many rows passed/failed each expectation — this is the quality layer that
# MAGIC feeds into the lineage graph.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent pipeline expectation results
# MAGIC SELECT
# MAGIC   e.dataset AS table_name,
# MAGIC   e.name AS expectation_name,
# MAGIC   e.passed_records,
# MAGIC   e.failed_records,
# MAGIC   ROUND(e.passed_records / NULLIF(e.passed_records + e.failed_records, 0) * 100, 2) AS pass_rate_pct,
# MAGIC   u.pipeline_name
# MAGIC FROM system.lakeflow.flow_progress_expectations e
# MAGIC JOIN system.lakeflow.pipeline_updates u
# MAGIC   ON e.update_id = u.update_id
# MAGIC WHERE u.pipeline_name LIKE '%red_bricks%'
# MAGIC   AND e.failed_records > 0
# MAGIC ORDER BY e.failed_records DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cross-Domain Lineage Summary
# MAGIC
# MAGIC Let's build a comprehensive view of the entire Red Bricks Insurance data estate — every
# MAGIC domain, its tables, and how they connect.

# COMMAND ----------

from pyspark.sql.functions import col, split, element_at, count, countDistinct

# Build domain-level lineage summary
summary = spark.sql(f"""
SELECT
  SPLIT(source_table_full_name, '\\\\.')[1] AS source_schema,
  SPLIT(target_table_full_name, '\\\\.')[1] AS target_schema,
  COUNT(*) AS edge_count,
  COUNT(DISTINCT source_table_full_name) AS source_tables,
  COUNT(DISTINCT target_table_full_name) AS target_tables
FROM system.access.table_lineage
WHERE source_table_catalog = '{catalog}'
  AND target_table_catalog = '{catalog}'
GROUP BY 1, 2
ORDER BY edge_count DESC
""")

print("Cross-Domain Lineage: Schema → Schema dependencies")
summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Governance + Lineage Combined
# MAGIC
# MAGIC The power of Unity Catalog is combining lineage with governance. Here's a unified view:
# MAGIC which tables have PHI columns, what masks protect them, and what downstream assets use them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Governance coverage: PHI tables with their protection status
# MAGIC SELECT
# MAGIC   'members.silver_members' AS table_name,
# MAGIC   'ssn_last_4' AS phi_column,
# MAGIC   'governance.mask_ssn' AS column_mask,
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'ssn_last_4'
# MAGIC        AND source_table_full_name LIKE '%silver_members%'), 0
# MAGIC   ) AS downstream_consumers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'members.silver_members', 'date_of_birth', 'governance.mask_dob',
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'date_of_birth'
# MAGIC        AND source_table_full_name LIKE '%silver_members%'), 0
# MAGIC   )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'members.silver_members', 'phone', 'governance.mask_phone',
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'phone'
# MAGIC        AND source_table_full_name LIKE '%silver_members%'), 0
# MAGIC   )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'members.silver_members', 'email', 'governance.mask_email',
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'email'
# MAGIC        AND source_table_full_name LIKE '%silver_members%'), 0
# MAGIC   )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'members.silver_members', 'address_line_1', 'governance.mask_address',
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'address_line_1'
# MAGIC        AND source_table_full_name LIKE '%silver_members%'), 0
# MAGIC   )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'members.silver_enrollment', 'line_of_business',
# MAGIC   'governance.filter_by_lob (ROW FILTER)' AS column_mask,
# MAGIC   COALESCE(
# MAGIC     (SELECT COUNT(DISTINCT target_table_full_name)
# MAGIC      FROM system.access.column_lineage
# MAGIC      WHERE source_column_name = 'line_of_business'
# MAGIC        AND source_table_full_name LIKE '%silver_enrollment%'), 0
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Talking Points
# MAGIC
# MAGIC 1. **Automatic lineage tracking** — Unity Catalog captures table and column lineage for every
# MAGIC    SDP pipeline, every SQL query, and every AI function. Zero instrumentation required.
# MAGIC
# MAGIC 2. **PHI traceability** — For HIPAA and state privacy regulations, you can trace every PHI
# MAGIC    field from source → bronze → silver → gold → AI function → app. Column masks ensure
# MAGIC    protection follows the data across all access patterns.
# MAGIC
# MAGIC 3. **Impact analysis before changes** — Before modifying a source table, query lineage to see
# MAGIC    every downstream consumer: views, functions, dashboards, and agent tools. This prevents
# MAGIC    breaking production workloads.
# MAGIC
# MAGIC 4. **Data quality + lineage** — SDP expectations enforce quality contracts at each layer.
# MAGIC    Combined with lineage, you get a complete audit trail: which records were dropped, why,
# MAGIC    and what downstream assets were affected.
# MAGIC
# MAGIC 5. **Governance at scale** — Row filters and column masks are set once and enforced everywhere:
# MAGIC    SQL, Python, Genie, dashboards, apps, and AI agents. This is how regulated industries
# MAGIC    (healthcare, financial services) operationalize data governance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Navigation
# MAGIC
# MAGIC To explore lineage visually in the Databricks UI:
# MAGIC 1. Go to **Catalog** → select any table → click the **Lineage** tab
# MAGIC 2. The graph shows upstream sources and downstream consumers
# MAGIC 3. Click any node to drill into column-level lineage
# MAGIC 4. Use the **Impact Analysis** button to see what would be affected by changes

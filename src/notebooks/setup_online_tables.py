# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Online Feature Store for Low-Latency Agent Lookups
# MAGIC
# MAGIC Publishes key agent-facing datasets to the **Databricks Online Feature Store**
# MAGIC (backed by Lakebase Autoscaling) for sub-10ms point lookups.
# MAGIC
# MAGIC ### Tables Published:
# MAGIC - `analytics.gold_member_360_feature` → online feature table for Care Intelligence Agent
# MAGIC - `providers.silver_providers_feature` → online feature table for FWA and Network Adequacy agents
# MAGIC
# MAGIC ### Approach:
# MAGIC SDP-managed tables (materialized views, streaming tables) cannot be published directly
# MAGIC to the Online Feature Store because they don't support `ALTER TABLE ADD CONSTRAINT`.
# MAGIC We create standalone feature tables with PK constraints, populate them via CTAS, then
# MAGIC publish to the existing Lakebase Autoscaling project using `FeatureEngineeringClient`.
# MAGIC
# MAGIC ### Graceful Degradation:
# MAGIC If the Online Feature Store is unavailable, the notebook succeeds with a warning.
# MAGIC Agents fall back to SQL warehouse queries (~1-3s latency).

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")
catalog = dbutils.widgets.get("catalog")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

print(f"Catalog: {catalog}")
print(f"Lakebase Project: {lakebase_project_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Feature Tables with Primary Key Constraints
# MAGIC
# MAGIC SDP streaming tables and materialized views don't support `ALTER TABLE ADD CONSTRAINT`.
# MAGIC We create standalone Delta tables with PK constraints via CTAS + ALTER.

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

def create_feature_table(source_table: str, target_table: str, pk_col: str, constraint_name: str):
    """Create a feature table with NOT NULL PK from an SDP-managed source."""
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    # Get schema from source, make PK column NOT NULL
    source_df = spark.table(source_table).where(f"{pk_col} IS NOT NULL")
    from pyspark.sql.types import StructType, StructField
    new_fields = []
    for f in source_df.schema.fields:
        if f.name == pk_col:
            new_fields.append(StructField(f.name, f.dataType, nullable=False, metadata=f.metadata))
        else:
            new_fields.append(f)
    not_null_schema = StructType(new_fields)

    # Write with enforced schema
    (source_df
     .select([pk_col] + [c for c in source_df.columns if c != pk_col])
     .write
     .format("delta")
     .option("overwriteSchema", "true")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite")
     .saveAsTable(target_table))

    # Set CDF property
    spark.sql(f"ALTER TABLE {target_table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

    # Alter column to NOT NULL then add PK
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {pk_col} SET NOT NULL")
    spark.sql(f"ALTER TABLE {target_table} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({pk_col})")
    print(f"Created {target_table} with PK constraint on {pk_col}")

create_feature_table(
    f"{catalog}.analytics.gold_member_360",
    f"{catalog}.analytics.gold_member_360_feature",
    "member_id", "pk_member_id"
)

create_feature_table(
    f"{catalog}.providers.silver_providers",
    f"{catalog}.providers.silver_providers_feature",
    "npi", "pk_npi"
)

print("\nFeature tables created and populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Publish to Online Feature Store (Lakebase Autoscaling)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Get or create the online store backed by Lakebase Autoscaling
online_store_name = f"{lakebase_project_id}-feature-store"

try:
    online_store = fe.get_online_store(name=online_store_name)
    print(f"Online store exists: {online_store_name}")
except Exception:
    try:
        print(f"Creating online store: {online_store_name}")
        fe.create_online_store(name=online_store_name, capacity="CU_1")
        online_store = fe.get_online_store(name=online_store_name)
        print(f"Created online store: {online_store_name}")
    except Exception as e:
        err = str(e)
        if "deprecated" in err.lower() or "not allowed" in err.lower() or "not supported" in err.lower():
            print(f"WARNING: Online Feature Store is not available on this workspace.")
            print(f"  Error: {err}")
            print(f"  Agents will use SQL warehouse queries instead.")
            online_store = None
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Publish Feature Tables

# COMMAND ----------

if online_store:
    for source_table, online_table in [
        (f"{catalog}.analytics.gold_member_360_feature", f"{catalog}.analytics.gold_member_360_online"),
        (f"{catalog}.providers.silver_providers_feature", f"{catalog}.providers.silver_providers_online"),
    ]:
        try:
            fe.publish_table(
                online_store=online_store,
                source_table_name=source_table,
                online_table_name=online_table,
            )
            print(f"Published: {source_table} → {online_table}")
        except Exception as e:
            err = str(e)
            if "already exists" in err.lower():
                print(f"Already published: {online_table}")
            elif "deprecated" in err.lower() or "not allowed" in err.lower():
                print(f"WARNING: Cannot publish {source_table}: {err}")
            else:
                print(f"ERROR publishing {source_table}: {err}")
                raise
else:
    print("Skipping publish — online store not available.")
    print("Agents will use SQL warehouse queries (~1-3s latency).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("ONLINE FEATURE STORE SETUP COMPLETE")
print("=" * 60)

if online_store:
    print(f"\nOnline Store: {online_store_name}")
    print(f"Feature Tables Published:")
    print(f"  {catalog}.analytics.gold_member_360_online (PK: member_id)")
    print(f"  {catalog}.providers.silver_providers_online (PK: npi)")
    print(f"\nAgent Integration:")
    print(f"  Agents query via serving endpoint for sub-10ms lookups.")
else:
    print(f"\nOnline Feature Store not available — using graceful fallback.")
    print(f"Feature tables created (can be queried directly via SQL):")
    print(f"  {catalog}.analytics.gold_member_360_feature")
    print(f"  {catalog}.providers.silver_providers_feature")
    print(f"\nAgent latency: ~1-3s via SQL warehouse (vs <10ms with Online Feature Store)")

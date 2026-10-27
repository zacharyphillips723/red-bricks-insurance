# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — RA agent memory on Lakebase (Phase 2b)
# MAGIC
# MAGIC Creates the agent's **self-authored memory** tiers + case transactional state on
# MAGIC Lakebase Postgres (DESIGN.md §8.3). Run **in-workspace** (native Lakebase OAuth).
# MAGIC
# MAGIC Prereqs / decisions:
# MAGIC - A target **Lakebase instance** must exist. As of 2026-10-22 this workspace had only
# MAGIC   FWA online-store instances — provision a dedicated instance (recommended name
# MAGIC   `red-bricks-ra-memory`) or point `instance_name` at a chosen existing one.
# MAGIC - **pgvector** is verified below before the lessons tier is created (DESIGN.md §8.5).

# COMMAND ----------
# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("instance_name", "red-bricks-ra-memory", "Lakebase instance")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")
INSTANCE = dbutils.widgets.get("instance_name")
DATABASE = dbutils.widgets.get("database")

import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
inst = w.database.get_database_instance(name=INSTANCE)
host = inst.read_write_dns
user = w.current_user.me().user_name
cred = w.database.generate_database_credential(instance_names=[INSTANCE], request_id="ra-memory-setup")
token = cred.token

conn = psycopg2.connect(host=host, port=5432, dbname=DATABASE, user=user,
                        password=token, sslmode="require")
conn.autocommit = True
cur = conn.cursor()

# COMMAND ----------
# MAGIC %md ### Verify pgvector availability (gates the lessons embedding column)
cur.execute("SELECT name, default_version FROM pg_available_extensions WHERE name = 'vector'")
row = cur.fetchone()
PGVECTOR = row is not None
print("pgvector available:" , PGVECTOR, row)
if not PGVECTOR:
    print("⚠ pgvector NOT available — will create agent_lessons WITHOUT the embedding column; "
          "use Databricks Vector Search fallback (DESIGN.md §8.5).")

# COMMAND ----------
# MAGIC %md ### Apply the schema
# Read the DDL file from the repo (adjust path to the deployed workspace files root).
ddl = open("/Workspace/Repos/red-bricks-insurance/src/risk_adjustment_lakebase_schema.sql").read()
if not PGVECTOR:
    # strip the vector extension + embedding column + ANN index for the fallback path
    ddl = ddl.replace("CREATE EXTENSION IF NOT EXISTS vector;", "-- pgvector unavailable; skipped")
    ddl = ddl.replace("  embedding       VECTOR(1024),     -- pgvector; see note above\n", "")

cur.execute(ddl)
print("✅ RA memory schema applied to", f"{INSTANCE}/{DATABASE}")

# COMMAND ----------
# MAGIC %md ### Verify
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema='public'
                 AND table_name IN ('agent_episodes','agent_lessons','agent_user_prefs',
                                    'ra_cases','ra_hcc_candidates','ra_adjudications')
               ORDER BY table_name""")
print([r[0] for r in cur.fetchall()])
cur.close(); conn.close()

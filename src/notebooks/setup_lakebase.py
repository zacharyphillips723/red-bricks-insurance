# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Database Setup
# MAGIC
# MAGIC Creates PostgreSQL databases inside Lakebase instances and runs DDL schemas.
# MAGIC Lakebase **instances** are provisioned by the DAB (Terraform). This notebook
# MAGIC handles the database internals that Terraform cannot manage:
# MAGIC
# MAGIC 1. Creates databases (if they don't exist)
# MAGIC 2. Runs DDL schemas (enums, tables, indexes)
# MAGIC 3. Grants PUBLIC access so app service principals can connect
# MAGIC
# MAGIC This task should run **before** bootstrap_workspace, which handles seeding,
# MAGIC SP role creation, and Genie space setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Unity Catalog Name")

# COMMAND ----------

# MAGIC %pip install psycopg[binary] --quiet
# MAGIC %restart_python

# COMMAND ----------

import uuid
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Resolve repo root — works in both bundle-deployed and local contexts
try:
    _here = Path(__file__).resolve().parent
    _repo_root = _here.parent.parent
except NameError:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _ws_root = "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0] if not _nb_path.startswith("/Workspace") else _nb_path.rsplit("/src/notebooks/", 1)[0]
    _repo_root = Path(_ws_root)

print(f"Repo root: {_repo_root}")

# ---------------------------------------------------------------------------
# Lakebase database configs — instance_name must match DAB resource names
# ---------------------------------------------------------------------------
LAKEBASE_CONFIGS = [
    {
        "instance_name": "red-bricks-command-center",
        "database_name": "red_bricks_alerts",
        "schema_file": "src/lakebase_schema.sql",
    },
    {
        "instance_name": "fwa-investigations",
        "database_name": "fwa_cases",
        "schema_file": "src/fwa_lakebase_schema.sql",
    },
    {
        "instance_name": "uw-simulations",
        "database_name": "uw_sim",
        "schema_file": "src/underwriting_sim_lakebase_schema.sql",
    },
]

# COMMAND ----------

def get_pg_connection(instance_name: str, database_name: str) -> psycopg.Connection:
    """Get a psycopg connection to a Lakebase database."""
    instance = w.database.get_database_instance(name=instance_name)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    return psycopg.connect(
        f"host={instance.read_write_dns} "
        f"dbname={database_name} "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} "
        f"sslmode=require"
    )


def create_database(instance_name: str, database_name: str) -> None:
    """Create the PostgreSQL database if it doesn't exist."""
    conn = get_pg_connection(instance_name, "postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {database_name}")
            print(f"  Database '{database_name}' created.")
        else:
            print(f"  Database '{database_name}' already exists.")
    conn.close()


def run_ddl(instance_name: str, database_name: str, schema_file: str) -> None:
    """Run the DDL schema SQL file against the database."""
    schema_path = _repo_root / schema_file
    ddl = schema_path.read_text()
    print(f"  Running DDL from {schema_file}...")
    with get_pg_connection(instance_name, database_name) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print(f"  DDL applied successfully.")


def grant_public_access(instance_name: str, database_name: str) -> None:
    """Grant PUBLIC access so app service principals can connect."""
    print(f"  Granting PUBLIC access on {database_name}...")
    with get_pg_connection(instance_name, database_name) as conn:
        with conn.cursor() as cur:
            cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC")
            cur.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO PUBLIC")
            cur.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO PUBLIC")
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO PUBLIC")
        conn.commit()
    print(f"  PUBLIC access granted.")

# COMMAND ----------

print("=" * 60)
print("Lakebase Database Setup")
print("=" * 60)

for cfg in LAKEBASE_CONFIGS:
    print(f"\n--- {cfg['instance_name']} / {cfg['database_name']} ---")
    create_database(cfg["instance_name"], cfg["database_name"])
    run_ddl(cfg["instance_name"], cfg["database_name"], cfg["schema_file"])
    grant_public_access(cfg["instance_name"], cfg["database_name"])

print("\nAll Lakebase databases ready.")

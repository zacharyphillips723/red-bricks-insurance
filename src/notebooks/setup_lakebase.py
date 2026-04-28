# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Autoscaling Project Setup
# MAGIC
# MAGIC Creates the Lakebase Autoscaling project, databases, and runs DDL schemas.
# MAGIC The Autoscaling project is managed here (not by DAB) because DAB does not yet
# MAGIC support native Autoscaling project resources.
# MAGIC
# MAGIC 1. Creates or verifies the Autoscaling project
# MAGIC 2. Ensures the endpoint is awake (handles scale-to-zero)
# MAGIC 3. Creates databases (if they don't exist)
# MAGIC 4. Runs DDL schemas (enums, tables, indexes)
# MAGIC 5. Grants PUBLIC access so app service principals can connect
# MAGIC
# MAGIC This task should run **before** bootstrap_workspace, which handles seeding,
# MAGIC SP role creation, permissions, and Genie space setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Unity Catalog Name")

# COMMAND ----------

# MAGIC %pip install psycopg[binary] --quiet
# MAGIC %restart_python

# COMMAND ----------

import time
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec

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
# Lakebase Autoscaling config — single project with 4 databases
# ---------------------------------------------------------------------------
LAKEBASE_PROJECT_ID = "red-bricks-insurance"
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT_PATH = f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH}/endpoints/primary"

LAKEBASE_DATABASES = [
    {
        "database_name": "red_bricks_alerts",
        "schema_file": "src/lakebase_schema.sql",
    },
    {
        "database_name": "fwa_cases",
        "schema_file": "src/fwa_lakebase_schema.sql",
    },
    {
        "database_name": "uw_sim",
        "schema_file": "src/underwriting_sim_lakebase_schema.sql",
    },
    {
        "database_name": "pa_reviews",
        "schema_file": "src/pa_reviews_lakebase_schema.sql",
    },
]

# COMMAND ----------

def get_or_create_project(project_id: str) -> None:
    """Create a Lakebase Autoscaling project if it doesn't already exist."""
    from databricks.sdk.errors import NotFound
    resource_name = f"projects/{project_id}"
    try:
        project = w.postgres.get_project(name=resource_name)
        print(f"  Project '{project_id}' already exists (uid: {project.uid})")
    except NotFound:
        print(f"  Creating Autoscaling project '{project_id}' (this may take 1-2 min)...")
        project = w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name=project_id,
                    pg_version="17",
                )
            ),
            project_id=project_id,
        ).wait()
        print(f"  Created. UID: {project.uid}")


def ensure_endpoint_awake(endpoint_path: str) -> str:
    """Poll the endpoint until hosts are available (handles scale-to-zero wake-up)."""
    print(f"  Ensuring endpoint is awake: {endpoint_path}")
    max_attempts = 15
    for attempt in range(1, max_attempts + 1):
        ep = w.postgres.get_endpoint(name=endpoint_path)
        if ep.status and ep.status.hosts and ep.status.hosts.host:
            host = ep.status.hosts.host
            print(f"  Endpoint ready: {host}")
            return host
        if attempt < max_attempts:
            wait = min(5 * attempt, 30)
            print(f"  Endpoint not ready (attempt {attempt}/{max_attempts}), retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Endpoint {endpoint_path} did not become ready after {max_attempts} attempts")


def get_pg_connection(project_id: str, database_name: str) -> psycopg.Connection:
    """Get a psycopg connection to a Lakebase Autoscaling database."""
    endpoint_path = f"projects/{project_id}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
    ep = w.postgres.get_endpoint(name=endpoint_path)
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    return psycopg.connect(
        f"host={host} "
        f"dbname={database_name} "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} "
        f"sslmode=require"
    )


def create_database(project_id: str, database_name: str) -> None:
    """Create the PostgreSQL database if it doesn't exist."""
    conn = get_pg_connection(project_id, "databricks_postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {database_name}")
            print(f"  Database '{database_name}' created.")
        else:
            print(f"  Database '{database_name}' already exists.")
    conn.close()


def run_ddl(project_id: str, database_name: str, schema_file: str) -> None:
    """Run the DDL schema SQL file against the database."""
    schema_path = _repo_root / schema_file
    ddl = schema_path.read_text()
    print(f"  Running DDL from {schema_file}...")
    with get_pg_connection(project_id, database_name) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print(f"  DDL applied successfully.")


def grant_public_access(project_id: str, database_name: str) -> None:
    """Grant PUBLIC access so app service principals can connect."""
    print(f"  Granting PUBLIC access on {database_name}...")
    with get_pg_connection(project_id, database_name) as conn:
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
print("Lakebase Autoscaling Project Setup")
print("=" * 60)

# 1. Create project
print(f"\n--- Project: {LAKEBASE_PROJECT_ID} ---")
get_or_create_project(LAKEBASE_PROJECT_ID)

# 2. Ensure endpoint is awake
endpoint_host = ensure_endpoint_awake(LAKEBASE_ENDPOINT_PATH)

# 3. Create databases, run DDL, grant access
for cfg in LAKEBASE_DATABASES:
    print(f"\n--- Database: {cfg['database_name']} ---")
    create_database(LAKEBASE_PROJECT_ID, cfg["database_name"])
    run_ddl(LAKEBASE_PROJECT_ID, cfg["database_name"], cfg["schema_file"])
    grant_public_access(LAKEBASE_PROJECT_ID, cfg["database_name"])

print("\nAll Lakebase databases ready.")

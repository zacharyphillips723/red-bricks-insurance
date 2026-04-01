# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Workspace Bootstrap
# MAGIC
# MAGIC **One-command post-deploy setup** for any workspace. Run this after `databricks bundle deploy`
# MAGIC to provision Lakebase instances, apply UC/warehouse grants for app service principals,
# MAGIC and seed operational data.
# MAGIC
# MAGIC ### What this notebook does:
# MAGIC 1. Creates Lakebase instances (Command Center + FWA Investigation + UW Simulation)
# MAGIC 2. Creates databases and runs DDL schemas
# MAGIC 3. Seeds care managers and fraud investigators
# MAGIC 4. Discovers app service principals and grants:
# MAGIC    - Unity Catalog: USE CATALOG, USE SCHEMA, SELECT on all domain schemas
# MAGIC    - SQL Warehouse: CAN_USE permission
# MAGIC    - Genie Spaces: CAN_RUN permission
# MAGIC    - Lakebase: PUBLIC access for app connections
# MAGIC 5. Creates Genie spaces (Analytics, FWA, Group Reporting, Financial, Underwriting) with dynamic table references
# MAGIC 6. Seeds alerts and investigation cases from gold tables
# MAGIC 7. Creates the empty `fwa_ml_predictions` table for gold MV compatibility
# MAGIC 8. Restarts all apps so they re-initialize Lakebase connections and auto-detect Genie spaces
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC - `databricks bundle deploy` has been run (apps exist in workspace)
# MAGIC - Gold tables are populated (run `red_bricks_refresh` job first, then run this)
# MAGIC - Or run Steps 1-4 before gold tables exist, then re-run Step 5 after pipelines complete

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Unity Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (leave empty to auto-detect)")

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")

print(f"Catalog: {catalog}")
print(f"Warehouse ID: {warehouse_id or '(will auto-detect)'}")

# COMMAND ----------

# MAGIC %pip install psycopg[binary] --quiet
# MAGIC %restart_python

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")

import json
import random
import uuid
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient

random.seed(42)
w = WorkspaceClient()

# Auto-detect SQL warehouse if not provided (prefer RUNNING, fall back to any)
if not warehouse_id.strip():
    all_wh = list(w.warehouses.list())
    running = [wh for wh in all_wh if wh.state and wh.state.value == "RUNNING"]
    if running:
        warehouse_id = running[0].id
        print(f"Auto-detected warehouse (running): {warehouse_id} ({running[0].name})")
    elif all_wh:
        warehouse_id = all_wh[0].id
        print(f"Auto-detected warehouse (state={all_wh[0].state}): {warehouse_id} ({all_wh[0].name})")
    else:
        print("WARNING: No SQL warehouses found. Warehouse grants will be skipped.")

# Resolve paths — works in both bundle-deployed and local contexts
try:
    _here = Path(__file__).resolve().parent
    _repo_root = _here.parent.parent
except NameError:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _ws_root = "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0] if not _nb_path.startswith("/Workspace") else _nb_path.rsplit("/src/notebooks/", 1)[0]
    _repo_root = Path(_ws_root)

print(f"Repo root: {_repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Lakebase Instances

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

LAKEBASE_CONFIGS = [
    {
        "instance_name": "red-bricks-command-center",
        "database_name": "red_bricks_alerts",
        "schema_file": "src/lakebase_schema.sql",
        "app_name": "red-bricks-command-center-app",
    },
    {
        "instance_name": "fwa-investigations",
        "database_name": "fwa_cases",
        "schema_file": "src/fwa_lakebase_schema.sql",
        "app_name": "red-bricks-fwa-portal-app",
    },
    {
        "instance_name": "uw-simulations",
        "database_name": "uw_sim",
        "schema_file": "src/underwriting_sim_lakebase_schema.sql",
        "app_name": "rb-uw-sim",
    },
]

# All apps that need UC + warehouse grants
# Auto-discover by listing all apps whose names contain "red-bricks" or "rb-"
APP_NAME_PATTERNS = []
try:
    for app in w.apps.list():
        name = app.name or ""
        if "red-bricks" in name or name.startswith("rb-"):
            APP_NAME_PATTERNS.append(name)
    print(f"Auto-discovered {len(APP_NAME_PATTERNS)} apps: {APP_NAME_PATTERNS}")
except Exception as e:
    print(f"Could not auto-discover apps: {e}. Falling back to known patterns.")
    APP_NAME_PATTERNS = [
        "red-bricks-command-center-app",
        "red-bricks-fwa-portal-app",
        "rb-uw-sim",
    ]

# All UC schemas that apps may need to read
UC_SCHEMAS = [
    "raw", "members", "claims", "providers", "documents",
    "risk_adjustment", "underwriting", "clinical", "benefits",
    "analytics", "fwa",
]

CAPACITY = "CU_1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Instances + Databases + DDL

# COMMAND ----------

def get_or_create_instance(instance_name: str) -> None:
    """Create a Lakebase instance if it doesn't already exist."""
    try:
        inst = w.database.get_database_instance(name=instance_name)
        print(f"  Instance '{instance_name}' already exists (state: {inst.state})")
    except Exception:
        print(f"  Creating instance '{instance_name}'...")
        inst = w.database.create_database_instance(
            name=instance_name,
            capacity=CAPACITY,
            stopped=False,
        )
        print(f"  Created. DNS: {inst.read_write_dns}, State: {inst.state}")


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
    """Create the database if it doesn't exist (connects to 'postgres' first)."""
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


def create_lakebase_roles_for_sps(instance_name: str, database_name: str, sp_client_ids: list[str]) -> None:
    """Create PostgreSQL roles for app service principals.

    Apps connect to Lakebase as their SP client_id (UUID). The role must exist
    in PostgreSQL before the connection succeeds, even with PUBLIC grants.
    """
    if not sp_client_ids:
        return
    print(f"  Creating Lakebase roles for {len(sp_client_ids)} app SPs...")
    with get_pg_connection(instance_name, database_name) as conn:
        with conn.cursor() as cur:
            for sp_id in sp_client_ids:
                try:
                    # Use quoted identifier — UUIDs contain hyphens
                    cur.execute(f'CREATE ROLE "{sp_id}" WITH LOGIN')
                    print(f"    Role created: {sp_id}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"    Role exists: {sp_id}")
                        conn.rollback()
                    else:
                        print(f"    Role creation failed for {sp_id}: {e}")
                        conn.rollback()
        conn.commit()

# COMMAND ----------

print("=" * 60)
print("STEP 1: Lakebase Instances")
print("=" * 60)

for cfg in LAKEBASE_CONFIGS:
    print(f"\n--- {cfg['instance_name']} ---")
    get_or_create_instance(cfg["instance_name"])
    create_database(cfg["instance_name"], cfg["database_name"])
    run_ddl(cfg["instance_name"], cfg["database_name"], cfg["schema_file"])
    grant_public_access(cfg["instance_name"], cfg["database_name"])

print("\nAll Lakebase instances ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Seed Care Managers & Fraud Investigators

# COMMAND ----------

CARE_MANAGERS = [
    ("sarah.johnson@redbricks.example.com", "Sarah Johnson", "RN", "Care Management", 40),
    ("michael.chen@redbricks.example.com", "Michael Chen", "NP", "Care Management", 35),
    ("lisa.williams@redbricks.example.com", "Lisa Williams", "SW", "Behavioral Health", 45),
    ("james.patel@redbricks.example.com", "James Patel", "CHW", "Community Health", 50),
    ("maria.garcia@redbricks.example.com", "Maria Garcia", "RN", "Chronic Disease", 40),
    ("david.kim@redbricks.example.com", "David Kim", "Pharmacist", "Medication Therapy", 30),
    ("rachel.thompson@redbricks.example.com", "Rachel Thompson", "PA", "Care Management", 35),
    ("omar.hassan@redbricks.example.com", "Omar Hassan", "LPN", "Population Health", 45),
]

FRAUD_INVESTIGATORS = [
    ("karen.mitchell@redbricks.example.com", "Karen Mitchell", "SIU Analyst", "Special Investigations Unit", 30),
    ("robert.chen@redbricks.example.com", "Robert Chen", "SIU Analyst", "Special Investigations Unit", 30),
    ("diana.torres@redbricks.example.com", "Diana Torres", "SIU Manager", "Special Investigations Unit", 20),
    ("mark.anderson@redbricks.example.com", "Mark Anderson", "Clinical Reviewer", "Payment Integrity", 25),
    ("jennifer.wong@redbricks.example.com", "Jennifer Wong", "Legal Counsel", "Legal & Compliance", 15),
    ("steven.patel@redbricks.example.com", "Steven Patel", "Recovery Specialist", "Payment Integrity", 35),
]

# COMMAND ----------

print("=" * 60)
print("STEP 2: Seed Staff")
print("=" * 60)

# Command Center — Care Managers
print("\nSeeding care managers...")
with get_pg_connection("red-bricks-command-center", "red_bricks_alerts") as conn:
    with conn.cursor() as cur:
        for email, name, role, dept, caseload in CARE_MANAGERS:
            cur.execute(
                """INSERT INTO care_managers (email, display_name, role, department, max_caseload)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING""",
                (email, name, role, dept, caseload),
            )
    conn.commit()
print(f"  {len(CARE_MANAGERS)} care managers seeded.")

# FWA Portal — Fraud Investigators
print("\nSeeding fraud investigators...")
with get_pg_connection("fwa-investigations", "fwa_cases") as conn:
    with conn.cursor() as cur:
        for email, name, role, dept, caseload in FRAUD_INVESTIGATORS:
            cur.execute(
                """INSERT INTO fraud_investigators (email, display_name, role, department, max_caseload)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING""",
                (email, name, role, dept, caseload),
            )
    conn.commit()
print(f"  {len(FRAUD_INVESTIGATORS)} fraud investigators seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Discover App Service Principals & Grant Permissions

# COMMAND ----------

def discover_app_service_principals() -> list[dict]:
    """Discover service principals for all deployed apps.

    Returns the service_principal_client_id (UUID / application_id) as sp_name,
    which is the identifier accepted by both SQL GRANT and REST API permissions.
    """
    sps = []
    for app_name in APP_NAME_PATTERNS:
        try:
            app = w.apps.get(app_name)
            sp_id = getattr(app, "service_principal_id", None) or getattr(app, "effective_service_principal_id", None)

            # Prefer service_principal_client_id (UUID) — this is the application_id
            # that SQL GRANT and REST API permissions both accept.
            sp_client_id = getattr(app, "service_principal_client_id", None)

            # Fallback: look up the SP by numeric ID to get its application_id
            if not sp_client_id and sp_id:
                try:
                    sp_obj = w.service_principals.get(sp_id)
                    sp_client_id = sp_obj.application_id
                except Exception:
                    pass

            # Last resort: search by display name
            if not sp_client_id:
                all_sps = list(w.service_principals.list(filter=f"displayName co \"{app_name}\""))
                if all_sps:
                    sp_client_id = all_sps[0].application_id
                    sp_id = sp_id or all_sps[0].id

            if sp_client_id:
                sps.append({"app_name": app_name, "sp_name": sp_client_id, "sp_id": sp_id})
                print(f"  {app_name}: SP client_id={sp_client_id} (ID={sp_id})")
            else:
                print(f"  {app_name}: WARNING — could not discover service principal")
        except Exception as e:
            print(f"  {app_name}: not found ({e})")
    return sps

# COMMAND ----------

print("=" * 60)
print("STEP 3: Discover App Service Principals")
print("=" * 60)

app_sps = discover_app_service_principals()

if not app_sps:
    print("\nNo app service principals found. Run 'databricks bundle deploy' first.")
    print("Skipping grant steps.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Lakebase Roles for App Service Principals

# COMMAND ----------

# Apps connect to Lakebase using their SP client_id (UUID) as the PostgreSQL username.
# The role must exist before the connection succeeds, even with PUBLIC grants.
if app_sps:
    sp_client_ids = [sp["sp_name"] for sp in app_sps]
    for cfg in LAKEBASE_CONFIGS:
        print(f"\n--- {cfg['instance_name']} / {cfg['database_name']} ---")
        create_lakebase_roles_for_sps(cfg["instance_name"], cfg["database_name"], sp_client_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Unity Catalog Permissions

# COMMAND ----------

if app_sps:
    print("Granting Unity Catalog permissions...\n")
    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        app_name = sp_info["app_name"]
        print(f"  --- {app_name} (SP: {sp_name}) ---")

        # USE CATALOG + BROWSE (BROWSE enables catalog/warehouse auto-detection in apps)
        for priv in ["USE CATALOG", "BROWSE"]:
            try:
                spark.sql(f"GRANT {priv} ON CATALOG {catalog} TO `{sp_name}`")
                print(f"    {priv} on {catalog}")
            except Exception as e:
                print(f"    {priv}: {e}")

        # USE SCHEMA + SELECT on each domain schema
        for schema in UC_SCHEMAS:
            try:
                spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp_name}`")
                spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{sp_name}`")
                print(f"    USE SCHEMA + SELECT on {catalog}.{schema}")
            except Exception as e:
                print(f"    {catalog}.{schema}: {e}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant SQL Warehouse Permissions

# COMMAND ----------

if app_sps and warehouse_id:
    print(f"Granting CAN_USE on warehouse {warehouse_id}...\n")
    import requests

    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        app_name = sp_info["app_name"]

        resp = requests.patch(
            f"https://{host}/api/2.0/permissions/sql/warehouses/{warehouse_id}",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "access_control_list": [
                    {"service_principal_name": sp_name, "permission_level": "CAN_USE"}
                ]
            },
        )
        if resp.status_code == 200:
            print(f"  {app_name}: CAN_USE granted")
        else:
            print(f"  {app_name}: {resp.status_code} — {resp.text}")
elif app_sps and not warehouse_id:
    print("No warehouse_id provided — skipping warehouse grants.")
    print("Set the warehouse_id widget and re-run this cell if needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Serving Endpoint & Vector Search Permissions

# COMMAND ----------

if app_sps:
    import requests as _req_ep
    _host_ep = spark.conf.get("spark.databricks.workspaceUrl")
    _token_ep = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    _ep_headers = {"Authorization": f"Bearer {_token_ep}", "Content-Type": "application/json"}

    # Serving endpoints that app SPs need CAN_QUERY on (beyond Foundation Model API)
    CUSTOM_ENDPOINTS = ["fwa-fraud-scorer"]

    for ep_name in CUSTOM_ENDPOINTS:
        print(f"\nGranting CAN_QUERY on serving endpoint '{ep_name}'...")
        for sp_info in app_sps:
            resp = _req_ep.patch(
                f"https://{_host_ep}/api/2.0/permissions/serving-endpoints/{ep_name}",
                headers=_ep_headers,
                json={
                    "access_control_list": [
                        {"service_principal_name": sp_info["sp_name"], "permission_level": "CAN_QUERY"}
                    ]
                },
            )
            if resp.status_code == 200:
                print(f"  {sp_info['app_name']}: CAN_QUERY granted")
            else:
                print(f"  {sp_info['app_name']}: {resp.status_code} — {resp.text[:200]}")

    # Vector Search — grant CAN_USE on the endpoint
    VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"

    # Look up the endpoint UUID — Azure requires UUID in the permissions URL path
    _vs_endpoint_id = None
    try:
        _vs_resp = _req_ep.get(
            f"https://{_host_ep}/api/2.0/vector-search/endpoints/{VS_ENDPOINT_NAME}",
            headers=_ep_headers,
        )
        if _vs_resp.status_code == 200:
            _vs_endpoint_id = _vs_resp.json().get("id", VS_ENDPOINT_NAME)
        else:
            _vs_endpoint_id = VS_ENDPOINT_NAME  # fall back to name (works on AWS)
    except Exception:
        _vs_endpoint_id = VS_ENDPOINT_NAME

    print(f"\nGranting vector search permissions (endpoint id={_vs_endpoint_id})...")
    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        # Grant CAN_USE on the vector search endpoint
        resp = _req_ep.patch(
            f"https://{_host_ep}/api/2.0/permissions/vector-search-endpoints/{_vs_endpoint_id}",
            headers=_ep_headers,
            json={
                "access_control_list": [
                    {"service_principal_name": sp_name, "permission_level": "CAN_USE"}
                ]
            },
        )
        if resp.status_code == 200:
            print(f"  {sp_info['app_name']}: CAN_USE on VS endpoint granted")
        else:
            print(f"  {sp_info['app_name']}: VS endpoint — {resp.status_code} — {resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Genie Spaces & Grant Permissions

# COMMAND ----------

import requests as _requests

_host = spark.conf.get("spark.databricks.workspaceUrl")
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_genie_headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}
_current_user = w.current_user.me().user_name

# Define all Genie spaces — tables are dynamic based on catalog
GENIE_SPACE_CONFIGS = [
    {
        "title": "Red Bricks Insurance — Analytics Assistant",
        "description": "Natural language analytics: claims, HEDIS quality, risk adjustment, enrollment, pharmacy, and AI-generated member risk narratives.",
        "tables": sorted([
            f"{catalog}.analytics.gold_pmpm",
            f"{catalog}.analytics.gold_mlr",
            f"{catalog}.analytics.gold_denial_analysis",
            f"{catalog}.analytics.gold_denial_classification",
            f"{catalog}.analytics.gold_member_risk_narrative",
            f"{catalog}.analytics.gold_risk_adjustment_analysis",
            f"{catalog}.analytics.gold_hedis_member",
            f"{catalog}.analytics.gold_hedis_provider",
            f"{catalog}.analytics.gold_stars_provider",
            f"{catalog}.claims.gold_claims_summary",
            f"{catalog}.claims.gold_pharmacy_summary",
            f"{catalog}.members.gold_enrollment_summary",
            f"{catalog}.members.gold_member_demographics",
            f"{catalog}.providers.gold_provider_directory",
            f"{catalog}.underwriting.gold_underwriting_summary",
            f"{catalog}.claims.silver_claims_medical",
            f"{catalog}.members.silver_enrollment",
            f"{catalog}.members.silver_members",
        ]),
    },
    {
        "title": "Red Bricks Insurance — FWA Analytics",
        "description": "Fraud, waste, and abuse analytics: provider risk scores, flagged claims, investigation cases, and network analysis.",
        "tables": sorted([
            f"{catalog}.fwa.gold_fwa_provider_risk",
            f"{catalog}.fwa.gold_fwa_claim_flags",
            f"{catalog}.fwa.gold_fwa_summary",
            f"{catalog}.fwa.silver_fwa_signals",
            f"{catalog}.fwa.silver_fwa_investigation_cases",
            f"{catalog}.analytics.gold_fwa_member_risk",
            f"{catalog}.analytics.gold_fwa_network_analysis",
            f"{catalog}.claims.silver_claims_medical",
            f"{catalog}.members.silver_enrollment",
            f"{catalog}.providers.silver_providers",
        ]),
    },
    {
        "title": "Red Bricks Insurance — Group Reporting",
        "description": "Employer group analytics: report cards, experience ratings, renewal projections, stop-loss analysis, and total cost of care.",
        "tables": sorted([
            f"{catalog}.analytics.gold_group_report_card",
            f"{catalog}.analytics.gold_group_experience",
            f"{catalog}.analytics.gold_group_renewal",
            f"{catalog}.analytics.gold_group_stop_loss",
            f"{catalog}.analytics.gold_tcoc_summary",
        ]),
    },
    {
        "title": "Red Bricks Insurance — Financial Analytics",
        "description": "Financial KPIs, actuarial metrics, utilization benchmarking, and IBNR reserve analysis.",
        "tables": sorted([
            f"{catalog}.analytics.gold_pmpm",
            f"{catalog}.analytics.gold_mlr",
            f"{catalog}.analytics.gold_utilization_per_1000",
            f"{catalog}.analytics.gold_ibnr_estimate",
            f"{catalog}.analytics.gold_ibnr_triangle",
            f"{catalog}.analytics.gold_ibnr_completion_factors",
            f"{catalog}.analytics.gold_denial_analysis",
            f"{catalog}.analytics.gold_coding_completeness",
            f"{catalog}.analytics.gold_risk_adjustment_analysis",
            f"{catalog}.analytics.gold_group_experience",
            f"{catalog}.analytics.gold_group_renewal",
            f"{catalog}.analytics.gold_group_stop_loss",
            f"{catalog}.underwriting.gold_underwriting_summary",
        ]),
    },
    {
        "title": "Red Bricks Insurance — Underwriting Simulation",
        "description": "Underwriting and actuarial analytics: PMPM trends, MLR by LOB, enrollment, utilization, IBNR reserves, group experience, risk adjustment, and benefit design.",
        "tables": sorted([
            f"{catalog}.analytics.gold_pmpm",
            f"{catalog}.analytics.gold_mlr",
            f"{catalog}.members.gold_enrollment_summary",
            f"{catalog}.analytics.gold_utilization_per_1000",
            f"{catalog}.analytics.gold_ibnr_completion_factors",
            f"{catalog}.analytics.gold_ibnr_triangle",
            f"{catalog}.analytics.gold_risk_adjustment_analysis",
            f"{catalog}.analytics.gold_coding_completeness",
            f"{catalog}.analytics.gold_tcoc_summary",
            f"{catalog}.analytics.gold_member_tcoc",
            f"{catalog}.analytics.gold_group_experience",
            f"{catalog}.analytics.gold_group_renewal",
            f"{catalog}.analytics.gold_group_stop_loss",
            f"{catalog}.benefits.silver_benefits",
            f"{catalog}.underwriting.gold_underwriting_summary",
        ]),
    },
]


def create_or_get_genie_space(title: str, description: str, tables: list[str]) -> str | None:
    """Create a Genie space if one with the same title doesn't already exist. Returns space_id."""
    # Check if a space with this title already exists
    try:
        existing = _requests.get(
            f"https://{_host}/api/2.0/genie/spaces",
            headers=_genie_headers,
        ).json().get("spaces", [])
        for s in existing:
            if s.get("title") == title:
                print(f"  Already exists: {s['space_id']}")
                return s["space_id"]
    except Exception:
        pass

    # Filter tables to only those that actually exist in the catalog
    valid_tables = []
    for t in tables:
        try:
            spark.sql(f"DESCRIBE TABLE {t}")
            valid_tables.append(t)
        except Exception:
            print(f"  Skipping missing table: {t}")

    if not valid_tables:
        print("  No valid tables found — skipping space creation.")
        return None

    serialized = json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in sorted(valid_tables)]
        }
    })

    try:
        resp = _requests.post(
            f"https://{_host}/api/2.0/genie/spaces",
            headers=_genie_headers,
            json={
                "warehouse_id": warehouse_id,
                "serialized_space": serialized,
                "title": title,
                "description": description,
            },
        )
        if resp.status_code == 200:
            space_id = resp.json().get("space_id")
            print(f"  Created: {space_id}")
            return space_id
        else:
            print(f"  Creation failed ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        print(f"  Creation failed: {e}")
    return None


def grant_genie_permissions(space_id: str, sp_names: list[str]) -> None:
    """Grant CAN_RUN on a Genie space to all app service principals."""
    acl = [{"user_name": _current_user, "permission_level": "CAN_MANAGE"}]
    for sp_name in sp_names:
        acl.append({"service_principal_name": sp_name, "permission_level": "CAN_RUN"})

    resp = _requests.put(
        f"https://{_host}/api/2.0/permissions/genie/{space_id}",
        headers=_genie_headers,
        json={"access_control_list": acl},
    )
    if resp.status_code == 200:
        print(f"  Permissions granted to {len(sp_names)} SPs")
    else:
        print(f"  Permission grant failed ({resp.status_code}): {resp.text[:200]}")


# COMMAND ----------

print("Creating Genie spaces and granting permissions...\n")

sp_names_for_genie = [sp["sp_name"] for sp in app_sps] if app_sps else []

for cfg in GENIE_SPACE_CONFIGS:
    print(f"--- {cfg['title']} ---")
    if not warehouse_id:
        print("  Skipped — no warehouse_id available")
        continue

    space_id = create_or_get_genie_space(cfg["title"], cfg["description"], cfg["tables"])

    if space_id and sp_names_for_genie:
        grant_genie_permissions(space_id, sp_names_for_genie)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Empty ML Predictions Table

# COMMAND ----------

print("=" * 60)
print("STEP 4: Pre-create ML Predictions Table")
print("=" * 60)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.analytics")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.analytics.fwa_ml_predictions (
        claim_id STRING,
        ml_fraud_probability DOUBLE,
        ml_risk_tier STRING,
        model_version STRING,
        scored_at STRING
    ) USING DELTA
""")
print(f"  {catalog}.analytics.fwa_ml_predictions — ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Operational Data from Gold Tables
# MAGIC
# MAGIC **Run this step after pipelines have completed** — it reads from gold tables to
# MAGIC populate Lakebase with alerts and investigation cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Command Center Alerts

# COMMAND ----------

def seed_command_center_alerts() -> int:
    """Seed risk stratification alerts from gold tables into Lakebase.

    Idempotent: truncates existing alerts before re-seeding so re-runs
    don't create duplicates.
    """
    # Clear existing alerts for idempotent re-runs.
    # CASCADE also truncates alert_activity_log (FK dependency), which is
    # correct because activity entries for deleted alerts are meaningless.
    with get_pg_connection("red-bricks-command-center", "red_bricks_alerts") as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE risk_stratification_alerts CASCADE")
        conn.commit()
    print("  Cleared existing alerts + activity log (idempotent re-seed).")

    total = 0

    # --- High-risk members (RAF > 2.0) ---
    try:
        risk_df = spark.sql(f"""
            SELECT member_id, raf_score, hcc_codes, hcc_count,
                   line_of_business, risk_rank, clinical_summary
            FROM {catalog}.analytics.gold_member_risk_narrative
            WHERE risk_rank <= 100
            ORDER BY risk_rank
        """).collect()
    except Exception as e:
        print(f"  gold_member_risk_narrative not available: {e}")
        risk_df = []

    if risk_df:
        with get_pg_connection("red-bricks-command-center", "red_bricks_alerts") as conn:
            with conn.cursor() as cur:
                for row in risk_df:
                    if row.raf_score and row.raf_score > 3.0:
                        tier = "Critical"
                    elif row.raf_score and row.raf_score > 2.5:
                        tier = "High"
                    elif row.raf_score and row.raf_score > 2.0:
                        tier = "Elevated"
                    else:
                        tier = "Moderate"

                    risk_score = min(99.9, (row.raf_score or 0) * 20)
                    hcc_list = row.hcc_codes.split(",") if row.hcc_codes else []
                    primary = f"RAF Score {row.raf_score:.2f} — {row.hcc_count} HCC codes"
                    secondary = [f"HCC: {h.strip()}" for h in hcc_list[:3]]
                    if row.clinical_summary:
                        secondary.append(row.clinical_summary[:200])

                    cur.execute(
                        """INSERT INTO risk_stratification_alerts (
                            patient_id, mrn, member_id, risk_tier, risk_score,
                            primary_driver, secondary_drivers, alert_source,
                            payer, notes, status
                        ) VALUES (
                            %s, %s, %s, %s::risk_tier, %s,
                            %s, %s, 'High Glucose No Insulin'::alert_source,
                            %s, %s, 'Unassigned'::care_cycle_status
                        )""",
                        (row.member_id, row.member_id, row.member_id, tier,
                         round(risk_score, 2), primary, secondary,
                         row.line_of_business, row.clinical_summary),
                    )
                    total += 1
                conn.commit()
        print(f"  {len(risk_df)} high-risk member alerts seeded.")

    # --- HEDIS care gap alerts ---
    try:
        hedis_df = spark.sql(f"""
            SELECT m.member_id, m.measure_name, m.line_of_business
            FROM (
                SELECT member_id, line_of_business, measure_name,
                       ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY measure_name) as rn
                FROM {catalog}.analytics.gold_hedis_member
                WHERE is_compliant = 0
            ) m
            WHERE m.rn = 1
            LIMIT 80
        """).collect()
    except Exception as e:
        print(f"  gold_hedis_member not available: {e}")
        hedis_df = []

    if hedis_df:
        with get_pg_connection("red-bricks-command-center", "red_bricks_alerts") as conn:
            with conn.cursor() as cur:
                for row in hedis_df:
                    primary = f"HEDIS Care Gap: {row.measure_name} — Non-compliant"
                    secondary = [f"LOB: {row.line_of_business}"]
                    cur.execute(
                        """INSERT INTO risk_stratification_alerts (
                            patient_id, mrn, member_id, risk_tier, risk_score,
                            primary_driver, secondary_drivers, alert_source,
                            payer, status
                        ) VALUES (
                            %s, %s, %s, 'Elevated'::risk_tier, 35.0,
                            %s, %s, 'SDOH Risk'::alert_source,
                            %s, 'Unassigned'::care_cycle_status
                        )""",
                        (row.member_id, row.member_id, row.member_id,
                         primary, secondary, row.line_of_business),
                    )
                    total += 1
                conn.commit()
        print(f"  {len(hedis_df)} HEDIS care gap alerts seeded.")

    # --- Denial pattern alerts ---
    try:
        denial_df = spark.sql(f"""
            SELECT line_of_business, denial_category,
                   SUM(denial_count) as total_denials,
                   ROUND(SUM(total_denied_amount), 2) as total_denied_amt
            FROM {catalog}.analytics.gold_denial_analysis
            GROUP BY line_of_business, denial_category
            HAVING SUM(denial_count) > 500
            ORDER BY total_denials DESC
            LIMIT 20
        """).collect()
    except Exception as e:
        print(f"  gold_denial_analysis not available: {e}")
        denial_df = []

    if denial_df:
        with get_pg_connection("red-bricks-command-center", "red_bricks_alerts") as conn:
            with conn.cursor() as cur:
                for row in denial_df:
                    tier = "High" if row.total_denials > 1000 else "Elevated"
                    primary = f"{row.total_denials} denials ({row.denial_category}) — ${row.total_denied_amt:,.0f} denied"
                    secondary = [f"LOB: {row.line_of_business}", f"Category: {row.denial_category}"]
                    cur.execute(
                        """INSERT INTO risk_stratification_alerts (
                            patient_id, mrn, risk_tier, risk_score,
                            primary_driver, secondary_drivers, alert_source,
                            payer, status
                        ) VALUES (
                            %s, %s, %s::risk_tier, %s,
                            %s, %s, 'ED High Utilizer'::alert_source,
                            %s, 'Unassigned'::care_cycle_status
                        )""",
                        (f"LOB-{row.line_of_business[:10]}-{row.denial_category[:10]}",
                         f"DENIAL-{row.line_of_business[:10]}",
                         tier, min(99.9, row.total_denials / 50),
                         primary, secondary, row.line_of_business),
                    )
                    total += 1
                conn.commit()
        print(f"  {len(denial_df)} denial pattern alerts seeded.")

    return total

# COMMAND ----------

# MAGIC %md
# MAGIC ### FWA Investigations

# COMMAND ----------

def seed_fwa_investigations() -> int:
    """Seed FWA investigation cases from silver/gold tables into Lakebase."""

    # Get investigator IDs
    investigators = []
    with get_pg_connection("fwa-investigations", "fwa_cases") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT investigator_id, display_name FROM fraud_investigators WHERE is_active = TRUE")
            investigators = cur.fetchall()

    if not investigators:
        print("  WARNING: No investigators found. Seed investigators first.")
        return 0

    # Read investigation cases
    try:
        cases_df = spark.sql(f"""
            SELECT investigation_id, investigation_type, target_type, target_id, target_name,
                   fraud_types, severity, status, estimated_overpayment, claims_involved_count,
                   investigation_summary, evidence_summary, rules_risk_score, ml_risk_score,
                   created_date
            FROM {catalog}.fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC
        """).collect()
    except Exception as e:
        print(f"  silver_fwa_investigation_cases not available: {e}")
        return 0

    count = 0
    with get_pg_connection("fwa-investigations", "fwa_cases") as conn:
        with conn.cursor() as cur:
            for row in cases_df:
                assigned_id = None
                if row.status != "Open":
                    inv = investigators[count % len(investigators)]
                    assigned_id = str(inv[0])

                rules_score = row.rules_risk_score or 0.5
                ml_score = row.ml_risk_score or 0.5
                composite = round(0.6 * rules_score + 0.4 * ml_score, 3)
                est_overpayment = float(row.estimated_overpayment or 0)

                # Compute confirmed/recovered for closed cases
                confirmed_overpayment = None
                recovered_amount = 0
                if row.status == "Closed — Confirmed Fraud":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.70, 1.10), 2)
                    recovered_amount = round(confirmed_overpayment * random.uniform(0.40, 0.85), 2)
                elif row.status == "Recovery In Progress":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.75, 1.05), 2)
                    recovered_amount = round(confirmed_overpayment * random.uniform(0.10, 0.45), 2)
                elif row.status == "Closed — No Fraud":
                    confirmed_overpayment = 0
                    recovered_amount = 0
                elif row.status == "Closed — Insufficient Evidence":
                    confirmed_overpayment = round(est_overpayment * random.uniform(0.20, 0.50), 2)
                    recovered_amount = 0

                cur.execute(
                    """INSERT INTO fwa_investigations (
                        investigation_id, investigation_type, target_type, target_id, target_name,
                        fraud_types, severity, source, status, assigned_investigator_id,
                        estimated_overpayment, confirmed_overpayment, recovered_amount,
                        claims_involved_count,
                        investigation_summary, evidence_summary,
                        rules_risk_score, ml_risk_score, composite_risk_score,
                        created_at
                    ) VALUES (
                        %s, %s::investigation_type, %s, %s, %s,
                        %s, %s::fraud_severity, 'Rules Engine'::investigation_source,
                        %s::investigation_status, CAST(%s AS uuid),
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s::timestamptz
                    ) ON CONFLICT (investigation_id) DO NOTHING""",
                    (row.investigation_id, row.investigation_type, row.target_type,
                     row.target_id, row.target_name,
                     row.fraud_types.split(",") if row.fraud_types else [],
                     row.severity, row.status, assigned_id,
                     est_overpayment, confirmed_overpayment, recovered_amount,
                     row.claims_involved_count,
                     row.investigation_summary, row.evidence_summary,
                     rules_score, ml_score, composite,
                     f"{row.created_date}T00:00:00Z" if row.created_date else None),
                )

                # Audit log entry
                cur.execute(
                    """INSERT INTO investigation_audit_log (
                        investigation_id, action_type, new_status, note
                    ) VALUES (%s, 'auto_generated', %s::investigation_status, %s)""",
                    (row.investigation_id, row.status,
                     f"Investigation auto-generated from FWA pipeline. Rules score: {rules_score:.3f}, ML score: {ml_score:.3f}."),
                )
                count += 1
            conn.commit()
    print(f"  {count} investigation cases seeded.")

    # Seed evidence for top 20 investigations
    try:
        top_inv = spark.sql(f"""
            SELECT investigation_id, target_id, target_type
            FROM {catalog}.fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC LIMIT 20
        """).collect()
    except Exception:
        top_inv = []

    evidence_count = 0
    with get_pg_connection("fwa-investigations", "fwa_cases") as conn:
        with conn.cursor() as cur:
            # Clear auto-seeded evidence for idempotent re-runs
            cur.execute("DELETE FROM investigation_evidence WHERE evidence_type = 'claim'")
            conn.commit()
            for inv in top_inv:
                if inv.target_type == "provider":
                    where = f"provider_npi = '{inv.target_id}'"
                elif inv.target_type == "member":
                    where = f"member_id = '{inv.target_id}'"
                else:
                    continue

                claims = spark.sql(f"""
                    SELECT signal_id, claim_id, fraud_type, fraud_score,
                           evidence_summary, estimated_overpayment
                    FROM {catalog}.fwa.silver_fwa_signals
                    WHERE {where} LIMIT 10
                """).collect()

                for claim in claims:
                    cur.execute(
                        """INSERT INTO investigation_evidence (
                            investigation_id, evidence_type, reference_id, description, detail_json
                        ) VALUES (%s, 'claim', %s, %s, %s::jsonb)""",
                        (inv.investigation_id, claim.claim_id, claim.evidence_summary,
                         json.dumps({"signal_id": claim.signal_id, "fraud_type": claim.fraud_type,
                                     "fraud_score": float(claim.fraud_score),
                                     "estimated_overpayment": float(claim.estimated_overpayment)})),
                    )
                    evidence_count += 1
            conn.commit()
    print(f"  {evidence_count} evidence records seeded.")

    return count

# COMMAND ----------

print("=" * 60)
print("STEP 5: Seed Operational Data from Gold Tables")
print("=" * 60)

print("\n--- Command Center Alerts ---")
alert_count = seed_command_center_alerts()

print("\n--- FWA Investigations ---")
inv_count = seed_fwa_investigations()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Apps
# MAGIC Apps cache Genie space IDs and Lakebase connections at startup.
# MAGIC After bootstrap creates these resources and grants permissions,
# MAGIC apps must be restarted to pick up the new configuration.

# COMMAND ----------

print("=" * 60)
print("STEP 6: Deploy & Restart Apps")
print("=" * 60)

# Map app names to their source code directories (relative to bundle root).
# The DAB creates the app resource but does NOT deploy source code automatically.
# This step ensures source code is deployed and apps are restarted with fresh config.
APP_SOURCE_CODE_MAP = {
    "red-bricks-command-center-app": "app",
    "red-bricks-fwa-portal-app": "app-fwa",
}
# Group reporting app name varies by target (rb-grp-rpt-dev, rb-grp-rpt-prod, etc.)
for _app_name in APP_NAME_PATTERNS:
    if _app_name.startswith("rb-grp-rpt"):
        APP_SOURCE_CODE_MAP[_app_name] = "app-group-reporting"
    elif _app_name.startswith("rb-uw-sim"):
        APP_SOURCE_CODE_MAP[_app_name] = "app-underwriting-sim"

deploy_count = 0
for app_name in APP_NAME_PATTERNS:
    try:
        app_info = w.apps.get(app_name)
        source_dir = APP_SOURCE_CODE_MAP.get(app_name)

        if source_dir:
            source_path = str(_repo_root / source_dir)
            print(f"  Deploying {app_name} from {source_path}...")
            try:
                w.apps.deploy(app_name, source_code_path=source_path).result()
                print(f"    {app_name}: deployed ✓")
                deploy_count += 1
            except Exception as deploy_err:
                print(f"    {app_name}: deploy failed ({deploy_err}), trying stop+start instead...")
                try:
                    w.apps.stop(name=app_name).result()
                    w.apps.start(name=app_name).result()
                    print(f"    {app_name}: restarted ✓")
                    deploy_count += 1
                except Exception as restart_err:
                    print(f"    {app_name}: restart also failed ({restart_err})")
        else:
            # No source code mapping — just restart
            print(f"  Restarting {app_name} (no source code mapping)...")
            w.apps.stop(name=app_name).result()
            w.apps.start(name=app_name).result()
            print(f"    {app_name}: restarted ✓")
            deploy_count += 1
    except Exception as e:
        print(f"  {app_name}: failed ({e})")

print(f"\n  {deploy_count} apps deployed/restarted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("BOOTSTRAP COMPLETE")
print("=" * 60)

# Lakebase status
for cfg in LAKEBASE_CONFIGS:
    inst = w.database.get_database_instance(name=cfg["instance_name"])
    print(f"\n  Lakebase: {cfg['instance_name']} ({inst.state})")
    print(f"    Database: {cfg['database_name']}")
    try:
        with get_pg_connection(cfg["instance_name"], cfg["database_name"]) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
                table_count = cur.fetchone()[0]
                print(f"    Tables: {table_count}")
    except Exception as e:
        print(f"    Connection check: {e}")

# App SPs
print(f"\n  App service principals granted: {len(app_sps)}")
for sp in app_sps:
    print(f"    {sp['app_name']}: {sp['sp_name']}")

# UC
print(f"\n  UC catalog: {catalog}")
print(f"  Schemas with grants: {', '.join(UC_SCHEMAS)}")
print(f"  Warehouse: {warehouse_id or '(not set)'}")

# Genie spaces
try:
    _genie_list = _requests.get(
        f"https://{_host}/api/2.0/genie/spaces",
        headers=_genie_headers,
    ).json().get("spaces", [])
    print(f"\n  Genie spaces: {len(_genie_list)}")
    for s in _genie_list:
        print(f"    {s.get('title', 'untitled')} ({s['space_id']})")
except Exception:
    print("\n  Genie spaces: could not list")

# Seeded data
print(f"\n  Alerts seeded: {alert_count}")
print(f"  Investigations seeded: {inv_count}")
print(f"  ML predictions table: {catalog}.analytics.fwa_ml_predictions")

print(f"\n  Apps deployed/restarted: {deploy_count}")

print("\n" + "=" * 60)
print("Next steps:")
print("  1. If gold tables weren't ready, re-run Steps 5-6 after pipelines complete")
print("  2. Verify apps at their URLs — all connections and permissions are live")
print("=" * 60)

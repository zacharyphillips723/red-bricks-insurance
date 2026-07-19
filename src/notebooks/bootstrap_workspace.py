# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Workspace Bootstrap
# MAGIC
# MAGIC **One-command post-deploy setup** for any workspace. Run this after `databricks bundle deploy`
# MAGIC to provision the Lakebase Autoscaling project, apply UC/warehouse grants for app service principals,
# MAGIC and seed operational data.
# MAGIC
# MAGIC ### What this notebook does:
# MAGIC 1. Creates Lakebase Autoscaling project with 3 databases (Command Center + FWA + UW Sim)
# MAGIC 2. Runs DDL schemas and grants PUBLIC access
# MAGIC 3. Seeds care managers and fraud investigators
# MAGIC 4. Discovers app service principals and grants:
# MAGIC    - Lakebase: project-level access via permissions API (replaces DAB security labels)
# MAGIC    - Unity Catalog: USE CATALOG, USE SCHEMA, SELECT on all domain schemas
# MAGIC    - SQL Warehouse: CAN_USE permission
# MAGIC    - Genie Spaces: CAN_RUN permission
# MAGIC    - Lakebase: PostgreSQL roles for app connections
# MAGIC 5. Creates Genie spaces (Analytics, FWA, Group Reporting, Financial, Underwriting) with dynamic table references
# MAGIC 6. Seeds alerts and investigation cases from gold tables
# MAGIC 7. Creates the empty `fwa_ml_predictions` table for gold MV compatibility
# MAGIC 8. Restarts all apps so they re-initialize Lakebase connections and auto-detect Genie spaces
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC - `databricks bundle deploy` has been run (apps exist in workspace)
# MAGIC - Gold tables are populated (run `red_bricks_full_demo` job first, then run this)
# MAGIC - Or run Steps 1-4 before gold tables exist, then re-run Step 5 after pipelines complete

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (leave empty to auto-detect)")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

print(f"Catalog: {catalog}")
print(f"Warehouse ID: {warehouse_id or '(will auto-detect)'}")

# COMMAND ----------

# MAGIC %pip install psycopg[binary] databricks-sdk "mlflow>=3.14.0" --upgrade --quiet
# MAGIC %restart_python

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

import json
import random
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode
from databricks.sdk.service.postgres import Project, ProjectSpec

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
# MAGIC ## Step 1: Lakebase Autoscaling Project

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

LAKEBASE_PROJECT_ID = lakebase_project_id
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT_PATH = f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH}/endpoints/primary"

LAKEBASE_DATABASES = [
    {
        "database_name": "red_bricks_alerts",
        "schema_file": "src/lakebase_schema.sql",
        "app_name": "red-bricks-command-center-app",
    },
    {
        "database_name": "fwa_cases",
        "schema_file": "src/fwa_lakebase_schema.sql",
        "app_name": "red-bricks-fwa-portal-app",
    },
    {
        "database_name": "uw_sim",
        "schema_file": "src/underwriting_sim_lakebase_schema.sql",
        "app_name": "rb-uw-sim",
    },
    {
        "database_name": "pa_reviews",
        "schema_file": "src/pa_reviews_lakebase_schema.sql",
        "app_name": "red-bricks-pa-portal-app",
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
        "red-bricks-pa-portal-app",
        "rb-network-adequacy-app",
        "rb-uw-sim",
    ]

# All UC schemas that apps may need to read
UC_SCHEMAS = [
    "raw", "members", "claims", "providers", "documents",
    "risk_adjustment", "underwriting", "clinical", "benefits",
    "analytics", "fwa", "prior_auth", "network", "care_management",
    "adt", "ai_tools",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Autoscaling Project + Databases + DDL

# COMMAND ----------

import time


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
    """Poll the endpoint until hosts are available (handles scale-to-zero wake-up).

    Returns the resolved host.
    """
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
    """Create the database if it doesn't exist (connects to 'databricks_postgres' first)."""
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


def create_lakebase_roles_for_sps(project_id: str, sp_client_ids: list[str]) -> None:
    """Create Lakebase-managed OAuth roles for app service principals.

    Uses the SDK postgres.create_role API to register SPs with LAKEBASE_OAUTH_V1
    auth. This is required for SPs to generate valid database credentials —
    raw PostgreSQL CREATE ROLE results in NO_LOGIN auth which rejects OAuth tokens.

    On clean redeploys, apps get new SPs. This function deletes stale roles from
    previous deploys before creating roles for the current SPs.
    """
    import time
    from databricks.sdk.service.postgres import Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType

    if not sp_client_ids:
        return

    branch = f"projects/{project_id}/branches/{LAKEBASE_BRANCH}"

    # Snapshot all existing roles on this branch
    existing_roles = list(w.postgres.list_roles(parent=branch))
    existing_by_pg_role = {r.status.postgres_role: r for r in existing_roles}
    current_sp_set = set(sp_client_ids)

    # --- Clean up orphaned roles from previous deploys ---
    # On a clean redeploy, old SPs are gone but their Lakebase roles persist.
    # These stale roles also block role_id reuse, so delete them first.
    for role in existing_roles:
        pg_role = role.status.postgres_role
        role_name = role.name or ""
        if pg_role not in current_sp_set and "/roles/sp-" in role_name:
            print(f"    Deleting stale role for old SP: {pg_role} ({role_name})")
            try:
                w.postgres.delete_role(name=role_name)
                time.sleep(5)
            except Exception as e:
                print(f"    Could not delete stale role: {e}")

    # --- Create / verify roles for current SPs ---
    print(f"  Creating Lakebase OAuth roles for {len(sp_client_ids)} app SPs...")
    for sp_id in sp_client_ids:
        if sp_id in existing_by_pg_role:
            role = existing_by_pg_role[sp_id]
            if role.status.auth_method == RoleAuthMethod.LAKEBASE_OAUTH_V1:
                print(f"    {sp_id}: OAuth role already exists")
                continue
            # Delete NO_LOGIN role so we can recreate with OAuth
            print(f"    {sp_id}: Deleting NO_LOGIN role...")
            w.postgres.delete_role(name=role.name)
            time.sleep(8)

        # Derive role_id from SP UUID — deterministic and avoids index-based collisions
        # Pattern: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
        role_id = f"sp-{sp_id}"
        try:
            op = w.postgres.create_role(
                parent=branch,
                role=Role(
                    spec=RoleRoleSpec(
                        postgres_role=sp_id,
                        auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                        identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                    )
                ),
                role_id=role_id,
            )
            result = op.wait()
            print(f"    {sp_id}: OAuth role created ({result.name})")
            time.sleep(5)
        except Exception as e:
            print(f"    {sp_id}: Role creation failed: {e}")


def grant_lakebase_access_to_sps(project_id: str, sp_infos: list[dict]) -> None:
    """Grant SP access to the Autoscaling project via permissions API.

    Uses the database-projects permissions endpoint with the project UID.
    Permission level CAN_USE allows SPs to generate credentials and connect.
    """
    if not sp_infos:
        return

    import requests
    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Resolve project UID — the permissions API requires the UID, not the display name
    project = w.postgres.get_project(name=f"projects/{project_id}")
    project_uid = project.uid
    print(f"  Project UID: {project_uid}")

    # Build ACL with all SPs in a single PATCH
    acl = [
        {"service_principal_name": sp["sp_name"], "permission_level": "CAN_USE"}
        for sp in sp_infos
    ]

    print(f"  Granting CAN_USE on database-projects/{project_uid} to {len(acl)} app SPs...")
    resp = requests.patch(
        f"https://{host}/api/2.0/permissions/database-projects/{project_uid}",
        headers=headers,
        json={"access_control_list": acl},
    )
    if resp.status_code == 200:
        for sp in sp_infos:
            print(f"    {sp['app_name']}: CAN_USE granted")
    else:
        print(f"  Permission grant failed ({resp.status_code}): {resp.text[:300]}")
        print(f"  Grant manually: Workspace Settings → Lakebase → {project_id} → Permissions")

# COMMAND ----------

print("=" * 60)
print("STEP 1: Lakebase Autoscaling Project")
print("=" * 60)

# 1. Create project (single LRO)
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

print("\nLakebase Autoscaling project ready.")

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

# PA Reviewers — (email, name, role, department, specialty, max_caseload)
PA_REVIEWERS = [
    ("nancy.wright@redbricks.example.com", "Nancy Wright", "UM Nurse", "Utilization Management", "Medical/Surgical", 50),
    ("thomas.lee@redbricks.example.com", "Thomas Lee", "Medical Director", "Utilization Management", "Internal Medicine", 25),
    ("sandra.martinez@redbricks.example.com", "Sandra Martinez", "UM Nurse", "Utilization Management", "Behavioral Health", 50),
    ("john.harrison@redbricks.example.com", "John Harrison", "Peer Reviewer", "Clinical Review", "Orthopedics", 30),
    ("angela.pham@redbricks.example.com", "Angela Pham", "Clinical Pharmacist", "Pharmacy Management", "Specialty Drugs", 40),
    ("christopher.brown@redbricks.example.com", "Christopher Brown", "Appeals Coordinator", "Member Services", None, 35),
    ("patricia.jones@redbricks.example.com", "Patricia Jones", "UM Nurse", "Utilization Management", "Oncology", 45),
    ("david.nguyen@redbricks.example.com", "David Nguyen", "Medical Director", "Utilization Management", "Cardiology", 20),
]

# COMMAND ----------

print("=" * 60)
print("STEP 2: Seed Staff")
print("=" * 60)

# Command Center — Care Managers
print("\nSeeding care managers...")
with get_pg_connection(LAKEBASE_PROJECT_ID, "red_bricks_alerts") as conn:
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
with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
    with conn.cursor() as cur:
        for email, name, role, dept, caseload in FRAUD_INVESTIGATORS:
            cur.execute(
                """INSERT INTO fraud_investigators (email, display_name, role, department, max_caseload)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING""",
                (email, name, role, dept, caseload),
            )
    conn.commit()
print(f"  {len(FRAUD_INVESTIGATORS)} fraud investigators seeded.")

# PA Reviewers
print("\nSeeding PA reviewers...")
with get_pg_connection(LAKEBASE_PROJECT_ID, "pa_reviews") as conn:
    with conn.cursor() as cur:
        for email, name, role, dept, specialty, caseload in PA_REVIEWERS:
            cur.execute(
                """INSERT INTO pa_reviewers (email, display_name, role, department, specialty, max_caseload)
                   VALUES (%s, %s, %s::reviewer_role, %s, %s, %s) ON CONFLICT (email) DO NOTHING""",
                (email, name, role, dept, specialty, caseload),
            )
    conn.commit()
print(f"  {len(PA_REVIEWERS)} PA reviewers seeded.")

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


def discover_serving_endpoint_service_principals() -> list[dict]:
    """Discover service principals for custom model serving endpoints.

    Serving endpoints that run agent models (e.g. fwa-supervisor-agent) get their
    own SPs which need the same UC, warehouse, Genie, and Lakebase grants as app SPs.
    """
    # Custom serving endpoints that need grants (exclude FMAPI pay-per-token endpoints)
    SERVING_ENDPOINT_PATTERNS = ["fwa-supervisor-agent", "fwa-fraud-scorer"]

    import requests
    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}

    sps = []
    for ep_name in SERVING_ENDPOINT_PATTERNS:
        try:
            resp = requests.get(f"https://{host}/api/2.0/serving-endpoints/{ep_name}", headers=headers)
            if resp.status_code != 200:
                print(f"  {ep_name}: not found ({resp.status_code})")
                continue

            ep_data = resp.json()
            found_sp = None

            # Strategy 1: Search by display name (SDK-created endpoints)
            sp_filter = f"displayName co \"{ep_name}\""
            matching_sps = list(w.service_principals.list(filter=sp_filter))
            if matching_sps:
                found_sp = matching_sps[0]

            # Strategy 2: Check endpoint events for system SP creation.
            # The events API logs "System service principal creation with ID `<uuid>`"
            # for each config version. We want the most recent one.
            if not found_sp:
                try:
                    import re as _re_events
                    events_resp = requests.get(
                        f"https://{host}/api/2.0/serving-endpoints/{ep_name}/events",
                        headers=headers,
                    )
                    if events_resp.status_code == 200:
                        sp_uuid_pattern = _re_events.compile(
                            r"System service principal creation with ID `([0-9a-f-]{36})`"
                        )
                        # Events are chronological; iterate in reverse to get the latest SP
                        for evt in reversed(events_resp.json().get("events", [])):
                            m = sp_uuid_pattern.search(evt.get("message", ""))
                            if m:
                                evt_sp_uuid = m.group(1)
                                evt_sps = list(w.service_principals.list(
                                    filter=f"applicationId eq \"{evt_sp_uuid}\""
                                ))
                                if evt_sps:
                                    found_sp = evt_sps[0]
                                    print(f"  {ep_name}: discovered runtime SP via endpoint events")
                                break
                except Exception as e_evt:
                    print(f"  {ep_name}: endpoint events fallback failed: {e_evt}")

            # Strategy 3: Check warehouse query history for the endpoint's runtime SP.
            # UI-created endpoints get a system-managed SP with a UUID name that doesn't
            # match the endpoint name. The SP shows up in SQL query history when the model
            # executes queries via WorkspaceClient().
            if not found_sp:
                try:
                    wh_id = spark.conf.get("spark.databricks.warehouseId", "")
                    if not wh_id:
                        wh_id = [wh.id for wh in w.warehouses.list()][0] if list(w.warehouses.list()) else ""
                    if wh_id:
                        qh_resp = requests.get(
                            f"https://{host}/api/2.0/sql/history/queries",
                            headers=headers,
                            params={
                                "max_results": "20",
                                "include_metrics": "false",
                                "filter_by.warehouse_ids": wh_id,
                            },
                        )
                        if qh_resp.status_code == 200:
                            # Collect SP-like user_names (UUID format) that ran queries
                            import re
                            uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                            known_app_sps = {sp_info["sp_name"] for sp_info in app_sps} if "app_sps" in dir() else set()
                            candidate_sps = set()
                            for q in qh_resp.json().get("res", []):
                                user = q.get("user_name", "")
                                if uuid_pattern.match(user) and user not in known_app_sps:
                                    candidate_sps.add(user)
                            # Match candidates against SCIM to find the endpoint's SP
                            for cand in candidate_sps:
                                cand_sps = list(w.service_principals.list(filter=f"applicationId eq \"{cand}\""))
                                if cand_sps:
                                    sp_obj = cand_sps[0]
                                    # Accept if display name contains serving-related keywords or is unrecognized
                                    display = (sp_obj.display_name or "").lower()
                                    if "app-" not in display and "vending" not in display:
                                        found_sp = sp_obj
                                        print(f"  {ep_name}: discovered runtime SP via query history")
                                        break
                except Exception as e2:
                    print(f"  {ep_name}: query history fallback failed: {e2}")

            if found_sp:
                sps.append({"app_name": f"endpoint:{ep_name}", "sp_name": found_sp.application_id, "sp_id": found_sp.id})
                print(f"  {ep_name}: SP client_id={found_sp.application_id} (ID={found_sp.id}, name={found_sp.display_name})")
            else:
                print(f"  {ep_name}: WARNING — endpoint exists but no matching SP found")
                print(f"    Try granting manually: look up SP in workspace Settings → Identity → Service Principals")
        except Exception as e:
            print(f"  {ep_name}: error ({e})")
    return sps

# COMMAND ----------

print("=" * 60)
print("STEP 3: Discover App & Serving Endpoint Service Principals")
print("=" * 60)

print("\n--- App SPs ---")
app_sps = discover_app_service_principals()

print("\n--- Serving Endpoint SPs ---")
endpoint_sps = discover_serving_endpoint_service_principals()

# Merge — deduplicate by sp_name (client_id)
existing_sp_names = {sp["sp_name"] for sp in app_sps}
for sp in endpoint_sps:
    if sp["sp_name"] not in existing_sp_names:
        app_sps.append(sp)
        existing_sp_names.add(sp["sp_name"])
    else:
        print(f"  {sp['app_name']}: already in app_sps, skipping duplicate")

print(f"\nTotal SPs to grant: {len(app_sps)}")

if not app_sps:
    print("\nNo service principals found. Run 'databricks bundle deploy' first.")
    print("Skipping grant steps.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Lakebase Roles for App Service Principals

# COMMAND ----------

# Grant SPs project-level access (replaces DAB security labels)
if app_sps:
    print("\n--- Lakebase Project Access ---")
    grant_lakebase_access_to_sps(LAKEBASE_PROJECT_ID, app_sps)

# Apps connect to Lakebase using their SP client_id (UUID) as the PostgreSQL username.
# Lakebase OAuth roles are created at the branch level (not per-database).
# The SDK's create_role registers the SP with LAKEBASE_OAUTH_V1 auth method.
if app_sps:
    sp_client_ids = [sp["sp_name"] for sp in app_sps]
    create_lakebase_roles_for_sps(LAKEBASE_PROJECT_ID, sp_client_ids)

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
                spark.sql(f"GRANT {priv} ON CATALOG {catalog_sql} TO `{sp_name}`")
                print(f"    {priv} on {catalog}")
            except Exception as e:
                print(f"    {priv}: {e}")

        # USE SCHEMA + SELECT on each domain schema
        for schema in UC_SCHEMAS:
            try:
                spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_sql}.{schema} TO `{sp_name}`")
                spark.sql(f"GRANT SELECT ON SCHEMA {catalog_sql}.{schema} TO `{sp_name}`")
                print(f"    USE SCHEMA + SELECT on {catalog}.{schema}")
            except Exception as e:
                print(f"    {catalog}.{schema}: {e}")

        # EXECUTE on ai_tools schema functions (used by the Care Intelligence Agent)
        try:
            spark.sql(f"GRANT EXECUTE ON SCHEMA {catalog_sql}.ai_tools TO `{sp_name}`")
            print(f"    EXECUTE on {catalog}.ai_tools functions")
        except Exception as e:
            print(f"    EXECUTE on ai_tools: {e}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create PA Document Upload Volume & Grant App SPs
# MAGIC
# MAGIC The PA Review Portal lets users upload medical records that are parsed with
# MAGIC `ai_parse_document` / `ai_extract` and auto-adjudicated in real time. Uploaded
# MAGIC files land in this UC Volume. App SPs need READ+WRITE VOLUME to store and read
# MAGIC the uploaded blobs.

# COMMAND ----------

# Volume for uploaded medical records (PA document auto-adjudication feature).
PA_DOC_VOLUME = f"{catalog_sql}.prior_auth.pa_documents"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.prior_auth")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {PA_DOC_VOLUME} "
        "COMMENT 'Uploaded medical records for PA auto-adjudication (synthetic PHI only)'"
    )
    print(f"  Volume ready: {catalog}.prior_auth.pa_documents")

    if app_sps:
        for sp_info in app_sps:
            sp_name = sp_info["sp_name"]
            try:
                spark.sql(f"GRANT READ VOLUME ON VOLUME {PA_DOC_VOLUME} TO `{sp_name}`")
                spark.sql(f"GRANT WRITE VOLUME ON VOLUME {PA_DOC_VOLUME} TO `{sp_name}`")
                print(f"    READ + WRITE VOLUME granted to {sp_info['app_name']}")
            except Exception as e:
                print(f"    {sp_name} on pa_documents volume: {e}")
except Exception as e:
    print(f"  WARNING: PA document volume provisioning failed: {e}")

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

    # Serving endpoints that app SPs need CAN_QUERY on.
    # Includes the FWA supervisor agent (apps call it via SDK in endpoint mode)
    # and any custom scoring endpoints.
    CUSTOM_ENDPOINTS = ["fwa-supervisor-agent", "fwa-fraud-scorer"]

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
        "description": "Natural language analytics: claims, HEDIS quality, risk adjustment, enrollment, pharmacy, clinical conditions, vitals, labs, and AI-generated member risk narratives.",
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
            f"{catalog}.clinical.gold_member_conditions",
            f"{catalog}.clinical.gold_condition_prevalence",
            f"{catalog}.clinical.silver_conditions",
            f"{catalog}.clinical.gold_vitals_summary",
            f"{catalog}.clinical.gold_encounter_summary",
            f"{catalog}.clinical.gold_lab_results_summary",
            f"{catalog}.members.gold_enrollment_summary",
            f"{catalog}.members.gold_member_demographics",
            f"{catalog}.providers.gold_provider_directory",
            f"{catalog}.underwriting.gold_underwriting_summary",
            f"{catalog}.claims.silver_claims_medical",
            f"{catalog}.members.silver_enrollment",
            f"{catalog}.members.silver_members",
            # Metric views (governed semantic layer)
            f"{catalog}.analytics.mv_financial_overview",
            f"{catalog}.analytics.mv_mlr_compliance",
            f"{catalog}.analytics.mv_utilization",
            f"{catalog}.analytics.mv_enrollment",
            f"{catalog}.analytics.mv_ibnr",
            f"{catalog}.analytics.mv_denials",
            f"{catalog}.analytics.mv_cost_of_care",
            f"{catalog}.analytics.mv_fwa_risk",
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
            # Metric views (governed semantic layer)
            f"{catalog}.analytics.mv_financial_overview",
            f"{catalog}.analytics.mv_mlr_compliance",
            f"{catalog}.analytics.mv_utilization",
            f"{catalog}.analytics.mv_enrollment",
            f"{catalog}.analytics.mv_ibnr",
            f"{catalog}.analytics.mv_denials",
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
            f"{catalog}.analytics.gold_pricing_factors",
        ]),
    },
    {
        "title": "Red Bricks Insurance — PA Analytics",
        "description": "Prior authorization analytics: request volume, approval/denial rates, turnaround times, CMS-0057-F compliance, auto-adjudication performance, provider patterns, and medical policy utilization.",
        "tables": sorted([
            f"{catalog}.prior_auth.gold_pa_requests",
            f"{catalog}.prior_auth.gold_pa_metrics",
            f"{catalog}.prior_auth.gold_pa_provider_patterns",
            f"{catalog}.prior_auth.gold_pa_policy_utilization",
            f"{catalog}.prior_auth.gold_pa_auto_adjudication_performance",
            f"{catalog}.prior_auth.gold_pa_denial_analysis",
            f"{catalog}.prior_auth.gold_pa_tier1_evaluation",
            f"{catalog}.prior_auth.silver_medical_policies",
            f"{catalog}.prior_auth.silver_medical_policy_rules",
            f"{catalog}.prior_auth.parsed_medical_policies",
            f"{catalog}.prior_auth.policy_summaries",
        ]),
    },
    {
        "title": "Red Bricks Insurance — Network Analytics",
        "description": "Network adequacy analytics: CMS compliance by county and specialty, ghost network detection, out-of-network leakage, provider recruitment targets, and network gap analysis. Ask questions like 'Which counties are non-compliant for behavioral health?' or 'Show top OON leakage specialties by cost.'",
        "tables": sorted([
            f"{catalog}.network.gold_network_adequacy_compliance",
            f"{catalog}.network.gold_ghost_network_flags",
            f"{catalog}.network.gold_network_leakage",
            f"{catalog}.network.gold_leakage_summary",
            f"{catalog}.network.gold_provider_recruitment_targets",
            f"{catalog}.network.gold_network_gaps",
            f"{catalog}.network.silver_provider_geo",
            f"{catalog}.network.silver_member_geo",
            f"{catalog}.network.silver_claims_network",
            f"{catalog}.network.silver_county_classification",
            f"{catalog}.network.silver_cms_standards",
        ]),
    },
    {
        "title": "Red Bricks AI Ops — Agent Observability",
        "description": "Observability for the FWA Investigation Agent system. Query token consumption, cost estimates, request volumes, inference table logs (every agent request/response automatically captured), evaluation scores, ML predictions, and AI classifications across Llama 4 Maverick (supervisor), Claude Haiku 4.5 (analyst), and the XGBoost fraud scorer. JOIN system.serving.endpoint_usage eu with system.serving.served_entities se on eu.served_entity_id = se.served_entity_id to get endpoint names. IMPORTANT: Always filter system tables by workspace_id = '7474660722665158' to scope to this workspace. Filter to FWA endpoints: WHERE se.endpoint_name IN ('databricks-llama-4-maverick', 'databricks-claude-haiku-4-5', 'fwa-fraud-scorer', 'fwa-supervisor-agent') AND eu.workspace_id = '7474660722665158'. Cost rates: Llama $0.40/$1.60 per 1M input/output tokens, Claude Haiku 4.5 $1.00/$5.00 per 1M. Evaluation table scores are 1-5 scale from Claude Sonnet 4 judge. The fwa_supervisor_payload table contains every request/response to the FWA agent endpoint with execution_time_ms, status_code, and full request/response JSON.",
        "tables": sorted([
            "system.serving.endpoint_usage",
            "system.serving.served_entities",
            f"{catalog}.analytics.fwa_agent_evaluation_results",
            f"{catalog}.analytics.fwa_agent_otel_annotations",
            f"{catalog}.analytics.fwa_agent_otel_logs",
            f"{catalog}.analytics.fwa_agent_otel_spans",
            f"{catalog}.analytics.fwa_ml_predictions",
            f"{catalog}.analytics.fwa_model_inference",
            f"{catalog}.analytics.fwa_supervisor_payload",
            f"{catalog}.analytics.gold_fwa_ai_classification",
            f"{catalog}.analytics.gold_fwa_model_scores",
            f"{catalog}.analytics.gold_fwa_network_analysis",
            f"{catalog}.analytics.gold_fwa_member_risk",
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
            # Backtick-quote each part for catalogs with special chars (e.g. hyphens)
            quoted = ".".join(f"`{part}`" for part in t.split("."))
            spark.sql(f"DESCRIBE TABLE {quoted}")
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
    """Grant CAN_RUN on a Genie space to all app and serving endpoint service principals.

    Grants permissions at TWO levels:
    1. Genie API level (/permissions/genie/{space_id}) — controls Genie API access
    2. Workspace-objects level (/permissions/workspace-objects/{obj_id}) — controls
       workspace ACL traversal required by serving endpoint runtime SPs
    """
    acl = [{"user_name": _current_user, "permission_level": "CAN_MANAGE"}]
    for sp_name in sp_names:
        acl.append({"service_principal_name": sp_name, "permission_level": "CAN_RUN"})

    # 1. Genie API permissions
    resp = _requests.put(
        f"https://{_host}/api/2.0/permissions/genie/{space_id}",
        headers=_genie_headers,
        json={"access_control_list": acl},
    )
    if resp.status_code == 200:
        print(f"  Genie API permissions granted to {len(sp_names)} SPs")
    else:
        print(f"  Genie API permission grant failed ({resp.status_code}): {resp.text[:200]}")

    # 2. Workspace-objects permissions (required for serving endpoint runtime SPs)
    # The Genie permissions response contains the workspace object ID in object_id field
    try:
        perm_resp = _requests.get(
            f"https://{_host}/api/2.0/permissions/genie/{space_id}",
            headers=_genie_headers,
        )
        if perm_resp.status_code == 200:
            obj_path = perm_resp.json().get("object_id", "")  # e.g. "/genie/4095602807544805"
            ws_obj_id = obj_path.split("/")[-1] if "/" in obj_path else ""
            if ws_obj_id:
                ws_acl = [{"service_principal_name": sp, "permission_level": "CAN_RUN"} for sp in sp_names]
                ws_resp = _requests.patch(
                    f"https://{_host}/api/2.0/permissions/workspace-objects/{ws_obj_id}",
                    headers=_genie_headers,
                    json={"access_control_list": ws_acl},
                )
                if ws_resp.status_code == 200:
                    print(f"  Workspace-objects ACL granted (object_id={ws_obj_id})")
                else:
                    print(f"  Workspace-objects ACL failed ({ws_resp.status_code}): {ws_resp.text[:200]}")
    except Exception as e:
        print(f"  Workspace-objects ACL grant failed: {e}")


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
# MAGIC ### Enable AI Gateway Inference Tables on Serving Endpoints
# MAGIC Endpoint config updates reset AI Gateway settings, so this re-enables
# MAGIC inference table logging on every bootstrap run.

# COMMAND ----------

import requests as _gw_requests

_gw_host = spark.conf.get("spark.databricks.workspaceUrl")
_gw_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_gw_headers = {"Authorization": f"Bearer {_gw_token}", "Content-Type": "application/json"}

_ai_gateway_endpoints = {
    "fwa-supervisor-agent": {"catalog_name": catalog, "schema_name": "analytics", "table_name_prefix": "fwa_supervisor"},
}

print("\n--- AI Gateway Inference Tables ---")
for _ep_name, _itc in _ai_gateway_endpoints.items():
    _ep_check = _gw_requests.get(f"https://{_gw_host}/api/2.0/serving-endpoints/{_ep_name}", headers=_gw_headers)
    if _ep_check.status_code != 200:
        print(f"  {_ep_name}: not found, skipping")
        continue
    _gw_resp = _gw_requests.put(
        f"https://{_gw_host}/api/2.0/serving-endpoints/{_ep_name}/ai-gateway",
        headers=_gw_headers,
        json={"inference_table_config": {**_itc, "enabled": True}},
    )
    if _gw_resp.status_code == 200:
        print(f"  {_ep_name}: enabled -> {_itc['catalog_name']}.{_itc['schema_name']}.{_itc['table_name_prefix']}_payload")
    else:
        print(f"  {_ep_name}: WARNING ({_gw_resp.status_code}): {_gw_resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provision MLflow UC Trace Storage for the FWA Agent
# MAGIC
# MAGIC Links a dedicated MLflow experiment to Unity Catalog OTel tables so the FWA
# MAGIC Investigation app streams agent traces (supervisor + genie + gemini spans)
# MAGIC into `{catalog}.analytics.fwa_agent_otel_*` in real-time. Uses the modern
# MAGIC `mlflow.set_experiment(trace_location=UnityCatalog(...))` API, which creates
# MAGIC the backing tables server-side on first call and is idempotent thereafter.
# MAGIC
# MAGIC **Important:** the experiment name MUST be one that has never had legacy
# MAGIC `databricksTrace*StorageTable` tags set on it. If such a tag exists,
# MAGIC `set_experiment` early-returns and silently skips table creation (there is
# MAGIC no supported API to delete experiment tags on Databricks-managed MLflow).

# COMMAND ----------

print("=" * 60)
print("STEP 3b: Provision MLflow UC Trace Storage (FWA Agent)")
print("=" * 60)

# These MUST stay in sync with app-fwa/backend/env_config.py defaults and the
# MLFLOW_UC_EXPERIMENT / UC_TRACE_* env vars in resources/app_fwa.yml.
TRACE_EXPERIMENT = "/Shared/red-bricks-fwa-agent-traces-uc2"
TRACE_SCHEMA = "analytics"
TRACE_TABLE_PREFIX = "fwa_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available (required to provision UC trace tables).")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        # The link/provision call needs a warehouse to create the Delta tables.
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{TRACE_SCHEMA}")

        exp = mlflow.set_experiment(
            TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog,
                schema_name=TRACE_SCHEMA,
                table_prefix=TRACE_TABLE_PREFIX,
            ),
        )
        spans_table = f"{catalog}.{TRACE_SCHEMA}.{TRACE_TABLE_PREFIX}_otel_spans"
        print(f"  Experiment linked: {TRACE_EXPERIMENT} (ID: {exp.experiment_id})")
        print(f"  Resolved location: {exp.trace_location}")
        print(f"  Spans table:       {spans_table}")

        # Verify the OTel tables were actually created (guards against the
        # silent early-return failure mode described above).
        _otel_tables = [
            f"{TRACE_TABLE_PREFIX}_otel_spans",
            f"{TRACE_TABLE_PREFIX}_otel_logs",
            f"{TRACE_TABLE_PREFIX}_otel_annotations",
        ]
        _existing = {
            r["tableName"]
            for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{TRACE_SCHEMA} LIKE '{TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        _missing = [t for t in _otel_tables if t not in _existing]
        if _missing:
            print(
                f"  WARNING: expected OTel tables missing: {_missing}. "
                "The experiment may be linked to a stale destination. "
                f"Choose a fresh TRACE_EXPERIMENT name and re-run."
            )
        else:
            print(f"  Verified OTel tables exist: {sorted(_existing)}")

        # Grant the app service principals write + read on the trace tables so
        # the running app can export spans (bootstrap creates them as the
        # notebook runner, not as the app SP).
        if app_sps:
            print("\n  Granting app SPs access to trace tables...")
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    _fq = f"{catalog_sql}.{TRACE_SCHEMA}.`{_tbl}`"
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {_fq} TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
            print(f"    SELECT + MODIFY granted to {len(app_sps)} SP(s) on {len(_existing)} table(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: UC trace storage provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

print("=" * 60)
print("STEP 3b-2: Provision MLflow UC Trace Storage (PA Review Agent)")
print("=" * 60)

# These MUST stay in sync with app-prior-auth/backend/env_config.py defaults and
# the MLFLOW_UC_EXPERIMENT / UC_TRACE_* env vars in resources/app_prior_auth.yml.
PA_TRACE_EXPERIMENT = "/Shared/red-bricks-pa-agent-traces-uc"
PA_TRACE_SCHEMA = "analytics"
PA_TRACE_TABLE_PREFIX = "pa_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available (required to provision UC trace tables).")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{PA_TRACE_SCHEMA}")

        exp = mlflow.set_experiment(
            PA_TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog,
                schema_name=PA_TRACE_SCHEMA,
                table_prefix=PA_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"  Experiment linked: {PA_TRACE_EXPERIMENT} (ID: {exp.experiment_id})")

        _otel_tables = [
            f"{PA_TRACE_TABLE_PREFIX}_otel_spans",
            f"{PA_TRACE_TABLE_PREFIX}_otel_logs",
            f"{PA_TRACE_TABLE_PREFIX}_otel_annotations",
        ]
        _existing = {
            r["tableName"]
            for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{PA_TRACE_SCHEMA} LIKE '{PA_TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        _missing = [t for t in _otel_tables if t not in _existing]
        if _missing:
            print(f"  WARNING: expected OTel tables missing: {_missing}. "
                  "Choose a fresh PA_TRACE_EXPERIMENT name and re-run.")
        else:
            print(f"  Verified OTel tables exist: {sorted(_existing)}")

        if app_sps:
            print("\n  Granting app SPs access to PA trace tables...")
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    _fq = f"{catalog_sql}.{PA_TRACE_SCHEMA}.`{_tbl}`"
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {_fq} TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
            print(f"    SELECT + MODIFY granted to {len(app_sps)} SP(s) on {len(_existing)} table(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: PA UC trace storage provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

print("=" * 60)
print("STEP 3b-3: Provision MLflow UC Trace Storage (Underwriting Agent)")
print("=" * 60)

# These MUST stay in sync with app-underwriting-sim/backend/env_config.py
# defaults and the MLFLOW_UC_EXPERIMENT / UC_TRACE_* env vars in
# resources/app_underwriting_sim.yml.
UW_TRACE_EXPERIMENT = "/Shared/red-bricks-uw-agent-traces-uc"
UW_TRACE_SCHEMA = "analytics"
UW_TRACE_TABLE_PREFIX = "uw_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available (required to provision UC trace tables).")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{UW_TRACE_SCHEMA}")

        exp = mlflow.set_experiment(
            UW_TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog,
                schema_name=UW_TRACE_SCHEMA,
                table_prefix=UW_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"  Experiment linked: {UW_TRACE_EXPERIMENT} (ID: {exp.experiment_id})")

        _otel_tables = [
            f"{UW_TRACE_TABLE_PREFIX}_otel_spans",
            f"{UW_TRACE_TABLE_PREFIX}_otel_logs",
            f"{UW_TRACE_TABLE_PREFIX}_otel_annotations",
        ]
        _existing = {
            r["tableName"]
            for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{UW_TRACE_SCHEMA} LIKE '{UW_TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        _missing = [t for t in _otel_tables if t not in _existing]
        if _missing:
            print(f"  WARNING: expected OTel tables missing: {_missing}. "
                  "Choose a fresh UW_TRACE_EXPERIMENT name and re-run.")
        else:
            print(f"  Verified OTel tables exist: {sorted(_existing)}")

        if app_sps:
            print("\n  Granting app SPs access to UW trace tables...")
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    _fq = f"{catalog_sql}.{UW_TRACE_SCHEMA}.`{_tbl}`"
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {_fq} TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
            print(f"    SELECT + MODIFY granted to {len(app_sps)} SP(s) on {len(_existing)} table(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: UW UC trace storage provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

print("=" * 60)
print("STEP 3b-4: Provision MLflow UC Trace Storage (Group Reporting Agent)")
print("=" * 60)

# In sync with app-group-reporting/backend/env_config.py + resources/app_group_reporting.yml.
GR_TRACE_EXPERIMENT = "/Shared/red-bricks-group-agent-traces-uc"
GR_TRACE_SCHEMA = "analytics"
GR_TRACE_TABLE_PREFIX = "group_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available (required to provision UC trace tables).")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{GR_TRACE_SCHEMA}")

        exp = mlflow.set_experiment(
            GR_TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog,
                schema_name=GR_TRACE_SCHEMA,
                table_prefix=GR_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"  Experiment linked: {GR_TRACE_EXPERIMENT} (ID: {exp.experiment_id})")

        _existing = {
            r["tableName"]
            for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{GR_TRACE_SCHEMA} LIKE '{GR_TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        print(f"  OTel tables: {sorted(_existing)}")

        if app_sps:
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    _fq = f"{catalog_sql}.{GR_TRACE_SCHEMA}.`{_tbl}`"
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {_fq} TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
            # The Group Reporting app writes renewal scenarios back to a governed
            # Delta table — needs MODIFY beyond the schema-level SELECT grant.
            for _sp in app_sps:
                try:
                    spark.sql(
                        f"GRANT SELECT, MODIFY ON TABLE {catalog_sql}.analytics.group_renewal_scenarios "
                        f"TO `{_sp['sp_name']}`"
                    )
                except Exception as _ge:
                    print(f"    {_sp['sp_name']} on group_renewal_scenarios: {_ge}")
            print(f"    SELECT + MODIFY granted to {len(app_sps)} SP(s) on {len(_existing)} table(s) + renewal-scenario write-back")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: Group Reporting UC trace storage provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

print("=" * 60)
print("STEP 3b-5: Provision MLflow UC Trace Storage + write-back table (Network Adequacy Agent)")
print("=" * 60)

# In sync with app-network-adequacy/backend/env_config.py + resources/app_network_adequacy.yml.
NET_TRACE_EXPERIMENT = "/Shared/red-bricks-network-agent-traces-uc"
NET_TRACE_SCHEMA = "analytics"
NET_TRACE_TABLE_PREFIX = "network_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available.")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{NET_TRACE_SCHEMA}")

        # Recruitment-pipeline write-back table (read-only app persists state to UC).
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog_sql}.network.provider_recruitment_status (
                npi STRING, specialty STRING, county_name STRING, status STRING,
                potential_savings DOUBLE, members_served INT, priority_score DOUBLE,
                notes STRING, updated_by STRING, updated_at TIMESTAMP
            )
            COMMENT 'Provider recruitment pipeline status, written back by the Network Adequacy app (read-only app persists workflow state to governed UC).'
        """)

        exp = mlflow.set_experiment(
            NET_TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog, schema_name=NET_TRACE_SCHEMA, table_prefix=NET_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"  Experiment linked: {NET_TRACE_EXPERIMENT} (ID: {exp.experiment_id})")

        _existing = {
            r["tableName"] for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{NET_TRACE_SCHEMA} LIKE '{NET_TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        print(f"  OTel tables: {sorted(_existing)}")

        if app_sps:
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog_sql}.{NET_TRACE_SCHEMA}.`{_tbl}` TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
                try:
                    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog_sql}.network.provider_recruitment_status TO `{_sp_name}`")
                except Exception as _ge:
                    print(f"    {_sp_name} on provider_recruitment_status: {_ge}")
            print(f"    Trace + recruitment write-back grants applied to {len(app_sps)} SP(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: Network Adequacy UC provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

print("=" * 60)
print("STEP 3b-6: Provision Command Center — care-plan write-back, governed member view, UC traces")
print("=" * 60)

# In sync with app/backend/env_config.py + resources/app.yml.
CC_TRACE_EXPERIMENT = "/Shared/red-bricks-care-agent-traces-uc"
CC_TRACE_SCHEMA = "analytics"
CC_TRACE_TABLE_PREFIX = "care_agent"

if not warehouse_id:
    print("  Skipped — no warehouse_id available.")
else:
    try:
        import os as _os
        import mlflow
        from mlflow.entities import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        _os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.care_management")

        # Care-plan write-back table — every AI-generated care plan is persisted here
        # for lineage, audit, and downstream analytics (Command Center has a Lakebase
        # backend, but care plans belong in governed UC for cross-app reuse).
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog_sql}.care_management.care_plans (
                plan_id STRING, member_id STRING, member_name STRING,
                generated_by STRING, generated_at TIMESTAMP, model_endpoint STRING,
                goal_count INT, summary STRING, plan_json STRING
            )
            COMMENT 'AI-generated care plans written back by the Population Health Command Center for lineage, audit, and analytics.'
        """)

        # Governed member view — masks PHI unless the caller is in phi_full_access
        # (same group the Red Bricks governance demo uses). Powers the app's live
        # persona-based masking demo without requiring account-admin mask functions.
        spark.sql(f"""
            CREATE OR REPLACE VIEW {catalog_sql}.analytics.gold_member_360_governed AS
            SELECT
              member_id,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN member_name
                   ELSE CONCAT(LEFT(first_name, 1), '*** ', LEFT(last_name, 1), '***') END AS member_name,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN CAST(date_of_birth AS STRING)
                   ELSE CONCAT(CAST(YEAR(date_of_birth) AS STRING), '-**-**') END AS date_of_birth,
              age, gender,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN address_line_1 ELSE '*** REDACTED ***' END AS address,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN phone ELSE CONCAT('(***) ***-', RIGHT(COALESCE(phone, '0000'), 4)) END AS phone,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN email ELSE CONCAT(LEFT(COALESCE(email, 'x'), 2), '***@***.com') END AS email,
              risk_tier,
              CASE WHEN is_account_group_member('phi_full_access')
                   THEN 'Full PHI Access' ELSE 'Restricted — PHI Masked' END AS access_level,
              current_user() AS queried_by
            FROM {catalog_sql}.analytics.gold_member_360
        """)
        print(f"  care_plans table + gold_member_360_governed view ready")

        exp = mlflow.set_experiment(
            CC_TRACE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=catalog, schema_name=CC_TRACE_SCHEMA, table_prefix=CC_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"  Experiment linked: {CC_TRACE_EXPERIMENT} (ID: {exp.experiment_id})")

        _existing = {
            r["tableName"] for r in spark.sql(
                f"SHOW TABLES IN {catalog_sql}.{CC_TRACE_SCHEMA} LIKE '{CC_TRACE_TABLE_PREFIX}_otel_*'"
            ).collect()
        }
        print(f"  OTel tables: {sorted(_existing)}")

        if app_sps:
            for _sp in app_sps:
                _sp_name = _sp["sp_name"]
                for _tbl in sorted(_existing):
                    try:
                        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog_sql}.{CC_TRACE_SCHEMA}.`{_tbl}` TO `{_sp_name}`")
                    except Exception as _ge:
                        print(f"    {_sp_name} on {_tbl}: {_ge}")
                try:
                    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog_sql}.care_management.care_plans TO `{_sp_name}`")
                    spark.sql(f"GRANT SELECT ON VIEW {catalog_sql}.analytics.gold_member_360_governed TO `{_sp_name}`")
                except Exception as _ge:
                    print(f"    {_sp_name} on care_plans/governed view: {_ge}")
            print(f"    Trace + care-plan write-back + governed-view grants applied to {len(app_sps)} SP(s)")
    except Exception as _te:
        import traceback as _tb
        print(f"  WARNING: Command Center UC provisioning failed: {_te}")
        _tb.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Empty ML Predictions Table

# COMMAND ----------

print("=" * 60)
print("STEP 4: Pre-create ML Predictions Table")
print("=" * 60)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.analytics")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_sql}.analytics.fwa_ml_predictions (
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
    with get_pg_connection(LAKEBASE_PROJECT_ID, "red_bricks_alerts") as conn:
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
            FROM {catalog_sql}.analytics.gold_member_risk_narrative
            WHERE risk_rank <= 100
            ORDER BY risk_rank
        """).collect()
    except Exception as e:
        print(f"  gold_member_risk_narrative not available: {e}")
        risk_df = []

    if risk_df:
        with get_pg_connection(LAKEBASE_PROJECT_ID, "red_bricks_alerts") as conn:
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
                FROM {catalog_sql}.analytics.gold_hedis_member
                WHERE is_compliant = 0
            ) m
            WHERE m.rn = 1
            LIMIT 80
        """).collect()
    except Exception as e:
        print(f"  gold_hedis_member not available: {e}")
        hedis_df = []

    if hedis_df:
        with get_pg_connection(LAKEBASE_PROJECT_ID, "red_bricks_alerts") as conn:
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
            FROM {catalog_sql}.analytics.gold_denial_analysis
            GROUP BY line_of_business, denial_category
            HAVING SUM(denial_count) > 500
            ORDER BY total_denials DESC
            LIMIT 20
        """).collect()
    except Exception as e:
        print(f"  gold_denial_analysis not available: {e}")
        denial_df = []

    if denial_df:
        with get_pg_connection(LAKEBASE_PROJECT_ID, "red_bricks_alerts") as conn:
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
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
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
            FROM {catalog_sql}.fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC
        """).collect()
    except Exception as e:
        print(f"  silver_fwa_investigation_cases not available: {e}")
        return 0

    count = 0
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
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
            FROM {catalog_sql}.fwa.silver_fwa_investigation_cases
            ORDER BY rules_risk_score DESC LIMIT 20
        """).collect()
    except Exception:
        top_inv = []

    evidence_count = 0
    with get_pg_connection(LAKEBASE_PROJECT_ID, "fwa_cases") as conn:
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
                    FROM {catalog_sql}.fwa.silver_fwa_signals
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


def seed_pa_review_queue() -> int:
    """Seed PA review queue from gold_pa_requests into Lakebase."""

    # Get reviewer IDs
    reviewers = []
    with get_pg_connection(LAKEBASE_PROJECT_ID, "pa_reviews") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT reviewer_id, display_name, role::text FROM pa_reviewers WHERE is_active = TRUE")
            reviewers = cur.fetchall()

    if not reviewers:
        print("  WARNING: No PA reviewers found. Seed reviewers first.")
        return 0

    # Read PA requests from gold table
    try:
        pa_df = spark.sql(f"""
            SELECT
                r.auth_request_id, r.member_id, r.requesting_provider_npi,
                r.service_type, r.procedure_code, r.procedure_description,
                r.diagnosis_codes, r.policy_id, r.urgency, r.line_of_business,
                r.clinical_summary, r.estimated_cost, r.determination,
                r.determination_tier, r.determination_reason, r.denial_reason_code,
                r.reviewer_type, r.turnaround_hours, r.cms_compliant,
                r.request_date, r.determination_date,
                r.appeal_filed, r.appeal_outcome,
                -- Get policy name
                p.policy_name,
                -- Get member name from members table
                COALESCE(CONCAT(m.first_name, ' ', m.last_name), CONCAT('Member ', SUBSTR(r.member_id, 1, 8))) AS member_name,
                -- Get provider name from providers table
                COALESCE(CONCAT('Dr. ', prov.provider_name), CONCAT('Dr. Provider ', SUBSTR(r.requesting_provider_npi, 1, 6))) AS provider_name,
                -- Get ML prediction if available
                ml.predicted_determination AS ai_recommendation,
                ml.confidence AS ai_confidence,
                -- Get Tier 1 evaluation
                t1.tier1_auto_eligible
            FROM {catalog_sql}.prior_auth.gold_pa_requests r
            LEFT JOIN {catalog_sql}.prior_auth.silver_medical_policies p ON r.policy_id = p.policy_id
            LEFT JOIN {catalog_sql}.prior_auth.pa_ml_predictions ml ON r.auth_request_id = ml.auth_request_id
            LEFT JOIN {catalog_sql}.prior_auth.gold_pa_tier1_evaluation t1 ON r.auth_request_id = t1.auth_request_id
            LEFT JOIN {catalog_sql}.members.silver_members m ON r.member_id = m.member_id
            LEFT JOIN {catalog_sql}.providers.silver_providers prov ON r.requesting_provider_npi = prov.npi
            ORDER BY r.request_date DESC
        """).collect()
    except Exception as e:
        print(f"  gold_pa_requests not available: {e}")
        return 0

    # Map determination to review status
    status_map = {
        "approved": "Approved",
        "denied": "Denied",
        "pended": "In Review",
        "partially_approved": "Partially Approved",
    }

    # Build caseload-aware reviewer assignment
    # Track how many open cases each reviewer has; stop assigning past max_caseload
    reviewer_caseload = {}  # reviewer_id -> current count
    reviewer_max = {}       # reviewer_id -> max_caseload
    for i, (email, name, role, dept, specialty, max_cl) in enumerate(PA_REVIEWERS):
        # reviewers list from DB has (reviewer_id, display_name, role)
        if i < len(reviewers):
            rid = str(reviewers[i][0])
            reviewer_caseload[rid] = 0
            reviewer_max[rid] = max_cl

    # For "pended" cases, distribute across open statuses for realism
    import random as _rng
    _rng.seed(42)
    _pended_count = 0

    def _pended_status():
        """Distribute pended cases: 50% In Review, 30% Pending Review, 20% Additional Info Requested"""
        nonlocal _pended_count
        _pended_count += 1
        r = _rng.random()
        if r < 0.50:
            return "In Review"
        elif r < 0.80:
            return "Pending Review"
        else:
            return "Additional Info Requested"

    count = 0
    robin_idx = 0
    with get_pg_connection(LAKEBASE_PROJECT_ID, "pa_reviews") as conn:
        with conn.cursor() as cur:
            # Clear existing data for clean re-seed
            cur.execute("TRUNCATE pa_review_actions, pa_review_queue CASCADE")
            for row in pa_df:
                det = row.determination
                if det == "pended":
                    status = _pended_status()
                else:
                    status = status_map.get(det, "Pending Review")

                # Assign reviewer for non-pending cases (caseload-aware)
                assigned_id = None
                if status not in ("Pending Review",):
                    # Find next reviewer with capacity (round-robin with cap)
                    for _ in range(len(reviewers)):
                        rev = reviewers[robin_idx % len(reviewers)]
                        rid = str(rev[0])
                        robin_idx += 1
                        # Only count open statuses toward caseload
                        if status in ("In Review", "Additional Info Requested", "Peer Review Requested"):
                            if reviewer_caseload.get(rid, 0) < reviewer_max.get(rid, 50):
                                assigned_id = rid
                                reviewer_caseload[rid] = reviewer_caseload.get(rid, 0) + 1
                                break
                        else:
                            # Determined cases (Approved/Denied) — assign freely, don't count toward caseload
                            assigned_id = rid
                            break
                    # If all reviewers at capacity, leave unassigned and set to Pending Review
                    if assigned_id is None and status in ("In Review", "Additional Info Requested"):
                        status = "Pending Review"

                # Determine tier
                tier = row.determination_tier
                if tier and tier not in ("tier_1_auto", "tier_2_ml", "tier_3_llm"):
                    tier = "manual"

                cur.execute(
                    """INSERT INTO pa_review_queue (
                        auth_request_id, member_id, member_name,
                        requesting_provider_npi, provider_name,
                        service_type, procedure_code, procedure_description,
                        diagnosis_codes, policy_id, policy_name, line_of_business,
                        clinical_summary, urgency, estimated_cost,
                        status, determination_tier, assigned_reviewer_id,
                        ai_recommendation, ai_confidence, tier1_auto_eligible,
                        determination_reason, denial_reason_code,
                        request_date, determination_date,
                        turnaround_hours, cms_compliant,
                        appeal_filed, appeal_outcome
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s::pa_urgency, %s,
                        %s::pa_review_status, %s::pa_determination_tier, CAST(%s AS uuid),
                        %s, %s, %s,
                        %s, %s,
                        %s::timestamptz, %s::timestamptz,
                        %s, %s,
                        %s, %s
                    ) ON CONFLICT (auth_request_id) DO NOTHING""",
                    (
                        row.auth_request_id, row.member_id, row.member_name,
                        row.requesting_provider_npi, row.provider_name,
                        row.service_type, row.procedure_code, row.procedure_description,
                        row.diagnosis_codes, row.policy_id, row.policy_name, row.line_of_business,
                        row.clinical_summary, row.urgency, float(row.estimated_cost or 0),
                        status, tier, assigned_id,
                        row.ai_recommendation, float(row.ai_confidence) if row.ai_confidence else None,
                        bool(row.tier1_auto_eligible) if row.tier1_auto_eligible is not None else False,
                        row.determination_reason, row.denial_reason_code,
                        str(row.request_date) if row.request_date else None,
                        str(row.determination_date) if row.determination_date else None,
                        float(row.turnaround_hours) if row.turnaround_hours else None,
                        bool(row.cms_compliant) if row.cms_compliant is not None else True,
                        bool(row.appeal_filed) if row.appeal_filed is not None else False,
                        row.appeal_outcome,
                    ),
                )

                # Audit log entry
                cur.execute(
                    """INSERT INTO pa_review_actions (
                        auth_request_id, action_type, new_status, note
                    ) VALUES (%s, 'auto_generated', %s::pa_review_status, %s)""",
                    (row.auth_request_id, status,
                     f"PA request auto-seeded from pipeline. Tier: {tier or 'unknown'}."),
                )
                count += 1
            conn.commit()

    # --- Shift dates so the demo shows realistic CMS deadline scenarios ---
    # Pending/In Review requests: requested within the last 0-5 days (some within deadline, some close)
    # Determined requests (Approved/Denied): requested 3-30 days ago with determination 1-5 days after
    print("  Adjusting dates for realistic CMS deadlines...")
    with get_pg_connection(LAKEBASE_PROJECT_ID, "pa_reviews") as conn:
        with conn.cursor() as cur:
            # Pending/In Review: spread request_date from now-5d to now, recompute cms_deadline
            cur.execute("""
                UPDATE pa_review_queue
                SET request_date = now() - (random() * INTERVAL '5 days'),
                    determination_date = NULL,
                    turnaround_hours = NULL,
                    cms_deadline = CASE
                        WHEN urgency = 'expedited'
                            THEN now() - (random() * INTERVAL '5 days') + INTERVAL '72 hours'
                        WHEN urgency = 'standard'
                            THEN now() - (random() * INTERVAL '5 days') + INTERVAL '168 hours'
                        ELSE now() - (random() * INTERVAL '5 days') + INTERVAL '30 days'
                    END
                WHERE status IN ('Pending Review', 'In Review', 'Additional Info Requested')
            """)
            # For pending, use a correlated update so cms_deadline matches the shifted request_date
            cur.execute("""
                UPDATE pa_review_queue
                SET cms_deadline = request_date + CASE
                        WHEN urgency = 'expedited' THEN INTERVAL '72 hours'
                        WHEN urgency = 'standard'  THEN INTERVAL '168 hours'
                        ELSE INTERVAL '30 days'
                    END
                WHERE status IN ('Pending Review', 'In Review', 'Additional Info Requested')
            """)

            # Determined: spread request_date from now-30d to now-3d, determination 1-5d after request
            cur.execute("""
                WITH shifted AS (
                    SELECT auth_request_id,
                           now() - INTERVAL '3 days' - (random() * INTERVAL '27 days') AS new_request
                    FROM pa_review_queue
                    WHERE status IN ('Approved', 'Denied', 'Partially Approved')
                )
                UPDATE pa_review_queue q
                SET request_date = s.new_request,
                    determination_date = s.new_request + (random() * INTERVAL '4 days') + INTERVAL '1 day',
                    cms_deadline = s.new_request + CASE
                        WHEN q.urgency = 'expedited' THEN INTERVAL '72 hours'
                        WHEN q.urgency = 'standard'  THEN INTERVAL '168 hours'
                        ELSE INTERVAL '30 days'
                    END
                FROM shifted s
                WHERE q.auth_request_id = s.auth_request_id
            """)

            # Recompute turnaround_hours and cms_compliant for determined requests
            cur.execute("""
                UPDATE pa_review_queue
                SET turnaround_hours = EXTRACT(EPOCH FROM (determination_date - request_date)) / 3600.0,
                    cms_compliant = determination_date <= cms_deadline
                WHERE status IN ('Approved', 'Denied', 'Partially Approved')
                  AND determination_date IS NOT NULL
            """)
            conn.commit()
    print(f"  {count} PA review requests seeded with realistic dates.")
    return count

# COMMAND ----------

print("=" * 60)
print("STEP 5: Seed Operational Data from Gold Tables")
print("=" * 60)

print("\n--- Command Center Alerts ---")
alert_count = seed_command_center_alerts()

print("\n--- FWA Investigations ---")
inv_count = seed_fwa_investigations()

print("\n--- PA Review Queue ---")
pa_count = seed_pa_review_queue()

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
    "red-bricks-pa-portal-app": "app-prior-auth",
    "rb-network-adequacy-app": "app-network-adequacy",
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
                w.apps.deploy(
                    app_name=app_name,
                    app_deployment=AppDeployment(
                        source_code_path=source_path,
                        mode=AppDeploymentMode.SNAPSHOT,
                    ),
                ).result()
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
try:
    project = w.postgres.get_project(name=f"projects/{LAKEBASE_PROJECT_ID}")
    print(f"\n  Lakebase project: {LAKEBASE_PROJECT_ID} (uid: {project.uid})")
except Exception as e:
    print(f"\n  Lakebase project: {LAKEBASE_PROJECT_ID} (could not check: {e})")

for cfg in LAKEBASE_DATABASES:
    print(f"    Database: {cfg['database_name']}")
    try:
        with get_pg_connection(LAKEBASE_PROJECT_ID, cfg["database_name"]) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
                table_count = cur.fetchone()[0]
                print(f"      Tables: {table_count}")
    except Exception as e:
        print(f"      Connection check: {e}")

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
print(f"  PA reviews seeded: {pa_count}")
print(f"  ML predictions table: {catalog}.analytics.fwa_ml_predictions")

print(f"\n  Apps deployed/restarted: {deploy_count}")

print("\n" + "=" * 60)
print("Next steps:")
print("  1. If gold tables weren't ready, re-run Steps 5-6 after pipelines complete")
print("  2. Verify apps at their URLs — all connections and permissions are live")
print("=" * 60)

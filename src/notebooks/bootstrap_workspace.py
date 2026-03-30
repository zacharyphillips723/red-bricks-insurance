# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Workspace Bootstrap
# MAGIC
# MAGIC **One-command post-deploy setup** for any workspace. Run this after `databricks bundle deploy`
# MAGIC to provision Lakebase instances, apply UC/warehouse grants for app service principals,
# MAGIC and seed operational data.
# MAGIC
# MAGIC ### What this notebook does:
# MAGIC 1. Creates Lakebase instances (Command Center + FWA Investigation)
# MAGIC 2. Creates databases and runs DDL schemas
# MAGIC 3. Seeds care managers and fraud investigators
# MAGIC 4. Discovers app service principals and grants:
# MAGIC    - Unity Catalog: USE CATALOG, USE SCHEMA, SELECT on all domain schemas
# MAGIC    - SQL Warehouse: CAN_USE permission
# MAGIC    - Lakebase: PUBLIC access for app connections
# MAGIC 5. Seeds alerts and investigation cases from gold tables
# MAGIC 6. Creates the empty `fwa_ml_predictions` table for gold MV compatibility
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
]

# All apps that need UC + warehouse grants
# Discover dynamically — try common name patterns across targets
APP_NAME_PATTERNS = [
    "red-bricks-command-center-app",
    "red-bricks-fwa-portal-app",
    "rb-grp-rpt-dev",
    "rb-grp-rpt-hls-financial",
    "rb-grp-rpt-e2-field-eng",
    "rb-grp-rpt-prod",
    "rb-group-reporting-dev",  # Legacy name
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
    """Discover service principals for all deployed apps."""
    sps = []
    for app_name in APP_NAME_PATTERNS:
        try:
            app = w.apps.get(app_name)
            sp_name = getattr(app, "service_principal_name", None) or getattr(app, "effective_service_principal_name", None)
            sp_id = getattr(app, "service_principal_id", None) or getattr(app, "effective_service_principal_id", None)

            # Try the app's service_principal_client_id if available
            if not sp_name and hasattr(app, "service_principal_client_id"):
                sp_name = app.service_principal_client_id

            # Fallback: list SPs matching the app name pattern
            if not sp_name:
                all_sps = list(w.service_principals.list(filter=f"displayName co \"{app_name}\""))
                if all_sps:
                    sp_name = all_sps[0].application_id
                    sp_id = all_sps[0].id

            if sp_name:
                sps.append({"app_name": app_name, "sp_name": sp_name, "sp_id": sp_id})
                print(f"  {app_name}: SP={sp_name} (ID={sp_id})")
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
# MAGIC ### Grant Unity Catalog Permissions

# COMMAND ----------

if app_sps:
    print("Granting Unity Catalog permissions...\n")
    for sp_info in app_sps:
        sp_name = sp_info["sp_name"]
        app_name = sp_info["app_name"]
        print(f"  --- {app_name} (SP: {sp_name}) ---")

        # USE CATALOG
        try:
            spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_name}`")
            print(f"    USE CATALOG on {catalog}")
        except Exception as e:
            print(f"    USE CATALOG: {e}")

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
    """Seed risk stratification alerts from gold tables into Lakebase."""
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

# Seeded data
print(f"\n  Alerts seeded: {alert_count}")
print(f"  Investigations seeded: {inv_count}")
print(f"  ML predictions table: {catalog}.analytics.fwa_ml_predictions")

print("\n" + "=" * 60)
print("Next steps:")
print("  1. If gold tables weren't ready, re-run Step 5 after pipelines complete")
print("  2. Verify apps at their URLs")
print("  3. Create Genie spaces (config/genie_*_setup.py)")
print("=" * 60)

"""
Lakebase Setup — FWA Investigation Portal

Provision a Lakebase instance, create schema, and seed investigators + investigations.

Run this script after:
  1. The FWA SDP pipeline has materialized gold tables
  2. You have permissions to create a Lakebase Provisioned instance

This script:
  - Creates a Lakebase Provisioned instance (CU_1)
  - Runs the schema DDL (tables, enums, views, triggers)
  - Seeds sample fraud investigators
  - Seeds investigation cases from gold FWA tables
"""

import uuid
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INSTANCE_NAME = "fwa-investigations"
DATABASE_NAME = "fwa_cases"
CAPACITY = "CU_1"

import os, sys
CATALOG = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("UC_CATALOG", "red_bricks_insurance")

# ---------------------------------------------------------------------------
# Step 1: Create Lakebase instance
# ---------------------------------------------------------------------------

def create_instance(w: WorkspaceClient) -> None:
    """Create the Lakebase Provisioned instance."""
    print(f"Creating Lakebase instance '{INSTANCE_NAME}'...")
    instance = w.database.create_database_instance(
        name=INSTANCE_NAME,
        capacity=CAPACITY,
        stopped=False,
    )
    print(f"  DNS: {instance.read_write_dns}")
    print(f"  State: {instance.state}")


def get_connection(w: WorkspaceClient) -> psycopg.Connection:
    """Get a psycopg connection with a fresh OAuth token."""
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[INSTANCE_NAME],
    )
    conn_string = (
        f"host={instance.read_write_dns} "
        f"dbname={DATABASE_NAME} "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} "
        f"sslmode=require"
    )
    return psycopg.connect(conn_string)


# ---------------------------------------------------------------------------
# Step 2: Create the database and run DDL
# ---------------------------------------------------------------------------

def create_database(w: WorkspaceClient) -> None:
    """Create the database (connect to default 'postgres' first)."""
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[INSTANCE_NAME],
    )
    conn_string = (
        f"host={instance.read_write_dns} "
        f"dbname=postgres "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} "
        f"sslmode=require"
    )
    conn = psycopg.connect(conn_string, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE_NAME}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {DATABASE_NAME}")
            print(f"  Database '{DATABASE_NAME}' created.")
        else:
            print(f"  Database '{DATABASE_NAME}' already exists.")
    conn.close()


def run_schema_ddl(w: WorkspaceClient) -> None:
    """Execute the schema DDL from fwa_lakebase_schema.sql."""
    schema_path = Path(__file__).resolve().parent.parent / "src" / "fwa_lakebase_schema.sql"
    ddl = schema_path.read_text()

    print("Running FWA schema DDL...")
    with get_connection(w) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("  Schema created successfully.")


# ---------------------------------------------------------------------------
# Step 3: Seed fraud investigators
# ---------------------------------------------------------------------------

SAMPLE_INVESTIGATORS = [
    ("karen.mitchell@redbricks.example.com", "Karen Mitchell", "SIU Analyst", "Special Investigations Unit", 30),
    ("robert.chen@redbricks.example.com", "Robert Chen", "SIU Analyst", "Special Investigations Unit", 30),
    ("diana.torres@redbricks.example.com", "Diana Torres", "SIU Manager", "Special Investigations Unit", 20),
    ("mark.anderson@redbricks.example.com", "Mark Anderson", "Clinical Reviewer", "Payment Integrity", 25),
    ("jennifer.wong@redbricks.example.com", "Jennifer Wong", "Legal Counsel", "Legal & Compliance", 15),
    ("steven.patel@redbricks.example.com", "Steven Patel", "Recovery Specialist", "Payment Integrity", 35),
]


def seed_investigators(w: WorkspaceClient) -> None:
    """Insert sample fraud investigators."""
    print("Seeding fraud investigators...")
    with get_connection(w) as conn:
        with conn.cursor() as cur:
            for email, name, role, dept, caseload in SAMPLE_INVESTIGATORS:
                cur.execute(
                    """
                    INSERT INTO fraud_investigators (email, display_name, role, department, max_caseload)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (email, name, role, dept, caseload),
                )
        conn.commit()
    print(f"  {len(SAMPLE_INVESTIGATORS)} fraud investigators seeded.")


# ---------------------------------------------------------------------------
# Step 4: Seed investigations from gold tables
# ---------------------------------------------------------------------------

def seed_investigations_from_gold(w: WorkspaceClient, spark: SparkSession) -> None:
    """Read gold FWA tables from Unity Catalog and insert into Lakebase.

    Seeds investigation cases from silver_fwa_investigation_cases and enriches
    them with provider risk data from gold_fwa_provider_risk.
    """
    print("Seeding investigations from gold tables...")

    # Get investigator IDs for assignment
    investigators = []
    with get_connection(w) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT investigator_id, display_name FROM fraud_investigators WHERE is_active = TRUE")
            investigators = cur.fetchall()

    if not investigators:
        print("  WARNING: No investigators found. Run seed_investigators first.")
        return

    # Read investigation cases from silver
    cases_df = spark.sql(f"""
        SELECT investigation_id, investigation_type, target_type, target_id, target_name,
               fraud_types, severity, status, estimated_overpayment, claims_involved_count,
               investigation_summary, evidence_summary, rules_risk_score, ml_risk_score,
               created_date
        FROM {CATALOG}.fwa.silver_fwa_investigation_cases
        ORDER BY rules_risk_score DESC
    """).collect()

    with get_connection(w) as conn:
        with conn.cursor() as cur:
            count = 0
            for row in cases_df:
                # Assign investigator (round-robin for non-Open statuses)
                assigned_id = None
                if row.status != "Open":
                    inv = investigators[count % len(investigators)]
                    assigned_id = str(inv[0])

                # Composite risk score (blend of rules + ML)
                rules_score = row.rules_risk_score or 0.5
                ml_score = row.ml_risk_score or 0.5
                composite = round(0.6 * rules_score + 0.4 * ml_score, 3)

                cur.execute(
                    """
                    INSERT INTO fwa_investigations (
                        investigation_id, investigation_type, target_type, target_id, target_name,
                        fraud_types, severity, source, status, assigned_investigator_id,
                        estimated_overpayment, claims_involved_count,
                        investigation_summary, evidence_summary,
                        rules_risk_score, ml_risk_score, composite_risk_score,
                        created_at
                    ) VALUES (
                        %s, %s::investigation_type, %s, %s, %s,
                        %s, %s::fraud_severity, 'Rules Engine'::investigation_source,
                        %s::investigation_status, CAST(%s AS uuid),
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s::timestamptz
                    )
                    ON CONFLICT (investigation_id) DO NOTHING
                    """,
                    (
                        row.investigation_id,
                        row.investigation_type,
                        row.target_type,
                        row.target_id,
                        row.target_name,
                        row.fraud_types.split(",") if row.fraud_types else [],
                        row.severity,
                        row.status,
                        assigned_id,
                        row.estimated_overpayment,
                        row.claims_involved_count,
                        row.investigation_summary,
                        row.evidence_summary,
                        rules_score,
                        ml_score,
                        composite,
                        f"{row.created_date}T00:00:00Z" if row.created_date else None,
                    ),
                )

                # Add initial audit log entry
                cur.execute(
                    """
                    INSERT INTO investigation_audit_log (
                        investigation_id, action_type, new_status, note
                    ) VALUES (%s, 'auto_generated', %s::investigation_status, %s)
                    """,
                    (
                        row.investigation_id,
                        row.status,
                        f"Investigation auto-generated from FWA pipeline. Rules score: {rules_score:.3f}, ML score: {ml_score:.3f}.",
                    ),
                )
                count += 1
            conn.commit()
        print(f"  {count} investigation cases seeded.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Full setup: create instance, schema, seed data."""
    w = WorkspaceClient()

    create_instance(w)
    create_database(w)
    run_schema_ddl(w)
    seed_investigators(w)

    # seed_investigations_from_gold requires Spark — run in a notebook
    # spark = SparkSession.builder.getOrCreate()
    # seed_investigations_from_gold(w, spark)

    print("\nFWA Lakebase setup complete!")
    print(f"  Instance: {INSTANCE_NAME}")
    print(f"  Database: {DATABASE_NAME}")
    print("  Next: Run seed_investigations_from_gold() in a notebook with Spark context.")


if __name__ == "__main__":
    main()

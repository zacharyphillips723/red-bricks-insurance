"""
Lakebase Setup — Red Bricks Insurance Command Center

Provision a Lakebase instance, create schema, and seed alerts from gold tables.

Run this script after:
  1. The SDP pipelines have materialized gold tables
  2. You have permissions to create a Lakebase Provisioned instance

This script:
  - Creates a Lakebase Provisioned instance (CU_1)
  - Runs the schema DDL (tables, enums, views, triggers)
  - Seeds initial alerts by reading from the gold Unity Catalog tables
  - Seeds sample care managers
"""

import uuid
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INSTANCE_NAME = "red-bricks-command-center"
DATABASE_NAME = "red_bricks_alerts"
CAPACITY = "CU_1"

CATALOG = "red_bricks_insurance"

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
    """Execute the schema DDL from lakebase_schema.sql."""
    schema_path = Path(__file__).resolve().parent.parent / "src" / "lakebase_schema.sql"
    ddl = schema_path.read_text()

    print("Running schema DDL...")
    with get_connection(w) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("  Schema created successfully.")


# ---------------------------------------------------------------------------
# Step 3: Seed care managers
# ---------------------------------------------------------------------------

SAMPLE_CARE_MANAGERS = [
    ("sarah.johnson@redbricks.example.com", "Sarah Johnson", "RN", "Care Management", 40),
    ("michael.chen@redbricks.example.com", "Michael Chen", "NP", "Care Management", 35),
    ("lisa.williams@redbricks.example.com", "Lisa Williams", "SW", "Behavioral Health", 45),
    ("james.patel@redbricks.example.com", "James Patel", "CHW", "Community Health", 50),
    ("maria.garcia@redbricks.example.com", "Maria Garcia", "RN", "Chronic Disease", 40),
    ("david.kim@redbricks.example.com", "David Kim", "Pharmacist", "Medication Therapy", 30),
    ("rachel.thompson@redbricks.example.com", "Rachel Thompson", "PA", "Care Management", 35),
    ("omar.hassan@redbricks.example.com", "Omar Hassan", "LPN", "Population Health", 45),
]


def seed_care_managers(w: WorkspaceClient) -> None:
    """Insert sample care managers."""
    print("Seeding care managers...")
    with get_connection(w) as conn:
        with conn.cursor() as cur:
            for email, name, role, dept, caseload in SAMPLE_CARE_MANAGERS:
                cur.execute(
                    """
                    INSERT INTO care_managers (email, display_name, role, department, max_caseload)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (email, name, role, dept, caseload),
                )
        conn.commit()
    print(f"  {len(SAMPLE_CARE_MANAGERS)} care managers seeded.")


# ---------------------------------------------------------------------------
# Step 4: Seed alerts from gold tables
# ---------------------------------------------------------------------------

def seed_alerts_from_gold(w: WorkspaceClient, spark: SparkSession) -> None:
    """Read gold tables from Unity Catalog and insert alerts into Lakebase.

    Alert sources mapped from red-bricks-insurance gold tables:
      - High RAF Score members → 'High Glucose No Insulin' alert source
        (reused enum; these are high-risk members needing care management)
      - HEDIS non-compliant members → 'SDOH Risk' alert source
        (care gap alerts for members failing quality measures)
      - AI-classified high-denial members → 'ED High Utilizer' alert source
        (reused enum; these are members with excessive denials needing intervention)
    """
    print("Seeding alerts from gold tables...")

    # --- High-risk members (RAF score > 2.0) from gold_member_risk_narrative ---
    risk_df = spark.sql(f"""
        SELECT member_id, raf_score, hcc_codes, hcc_count,
               line_of_business, risk_rank, clinical_summary
        FROM {CATALOG}.analytics.gold_member_risk_narrative
        WHERE risk_rank <= 100
        ORDER BY risk_rank
    """).collect()

    with get_connection(w) as conn:
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
                    """
                    INSERT INTO risk_stratification_alerts (
                        patient_id, mrn, member_id, risk_tier, risk_score,
                        primary_driver, secondary_drivers, alert_source,
                        payer, notes, status
                    ) VALUES (
                        %s, %s, %s, %s::risk_tier, %s,
                        %s, %s, 'High Glucose No Insulin'::alert_source,
                        %s, %s, 'Unassigned'::care_cycle_status
                    )
                    """,
                    (
                        row.member_id,
                        row.member_id,
                        row.member_id,
                        tier,
                        round(risk_score, 2),
                        primary,
                        secondary,
                        row.line_of_business,
                        row.clinical_summary,
                    ),
                )
            conn.commit()
        print(f"  {len(risk_df)} high-risk member alerts seeded.")

    # --- HEDIS care gap alerts (non-compliant members) ---
    hedis_df = spark.sql(f"""
        SELECT m.member_id, m.measure_name, m.is_compliant, m.line_of_business,
               e.avg_risk_score
        FROM (
            SELECT member_id, line_of_business, measure_name, is_compliant,
                   ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY measure_name) as rn
            FROM {CATALOG}.analytics.gold_hedis_member
            WHERE is_compliant = 0
        ) m
        LEFT JOIN (
            SELECT line_of_business, AVG(avg_risk_score) as avg_risk_score
            FROM {CATALOG}.members.gold_enrollment_summary
            GROUP BY line_of_business
        ) e ON m.line_of_business = e.line_of_business
        WHERE m.rn = 1
        LIMIT 80
    """).collect()

    with get_connection(w) as conn:
        with conn.cursor() as cur:
            for row in hedis_df:
                tier = "Elevated"
                primary = f"HEDIS Care Gap: {row.measure_name} — Non-compliant"
                secondary = [f"LOB: {row.line_of_business}"]

                cur.execute(
                    """
                    INSERT INTO risk_stratification_alerts (
                        patient_id, mrn, member_id, risk_tier, risk_score,
                        primary_driver, secondary_drivers, alert_source,
                        payer, status
                    ) VALUES (
                        %s, %s, %s, %s::risk_tier, %s,
                        %s, %s, 'SDOH Risk'::alert_source,
                        %s, 'Unassigned'::care_cycle_status
                    )
                    """,
                    (
                        row.member_id,
                        row.member_id,
                        row.member_id,
                        tier,
                        35.0,
                        primary,
                        secondary,
                        row.line_of_business,
                    ),
                )
            conn.commit()
        print(f"  {len(hedis_df)} HEDIS care gap alerts seeded.")

    # --- High-denial members (from gold_denial_analysis) ---
    denial_df = spark.sql(f"""
        SELECT d.line_of_business, d.denial_category,
               SUM(d.denial_count) as total_denials,
               ROUND(SUM(d.total_denied_amount), 2) as total_denied_amt
        FROM {CATALOG}.analytics.gold_denial_analysis d
        GROUP BY d.line_of_business, d.denial_category
        HAVING SUM(d.denial_count) > 500
        ORDER BY total_denials DESC
        LIMIT 20
    """).collect()

    with get_connection(w) as conn:
        with conn.cursor() as cur:
            for row in denial_df:
                tier = "High" if row.total_denials > 1000 else "Elevated"
                primary = f"{row.total_denials} denials ({row.denial_category}) — ${row.total_denied_amt:,.0f} denied"
                secondary = [f"LOB: {row.line_of_business}", f"Category: {row.denial_category}"]

                cur.execute(
                    """
                    INSERT INTO risk_stratification_alerts (
                        patient_id, mrn, risk_tier, risk_score,
                        primary_driver, secondary_drivers, alert_source,
                        payer, status
                    ) VALUES (
                        %s, %s, %s::risk_tier, %s,
                        %s, %s, 'ED High Utilizer'::alert_source,
                        %s, 'Unassigned'::care_cycle_status
                    )
                    """,
                    (
                        f"LOB-{row.line_of_business[:10]}-{row.denial_category[:10]}",
                        f"DENIAL-{row.line_of_business[:10]}",
                        tier,
                        min(99.9, row.total_denials / 50),
                        primary,
                        secondary,
                        row.line_of_business,
                    ),
                )
            conn.commit()
        print(f"  {len(denial_df)} denial pattern alerts seeded.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Full setup: create instance, schema, seed data."""
    w = WorkspaceClient()

    create_instance(w)
    create_database(w)
    run_schema_ddl(w)
    seed_care_managers(w)

    # seed_alerts_from_gold requires Spark — run in a notebook
    # spark = SparkSession.builder.getOrCreate()
    # seed_alerts_from_gold(w, spark)

    print("\nSetup complete!")
    print(f"  Instance: {INSTANCE_NAME}")
    print(f"  Database: {DATABASE_NAME}")
    print("  Next: Run seed_alerts_from_gold() in a notebook with Spark context.")


if __name__ == "__main__":
    main()

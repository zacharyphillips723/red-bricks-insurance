# Databricks notebook source
# MAGIC %md
# MAGIC # Seed FWA Lakebase from Gold Tables
# MAGIC
# MAGIC Reads investigation cases from FWA gold tables and inserts them into the
# MAGIC Lakebase `fwa_cases` database along with audit log entries.
# MAGIC
# MAGIC **Prerequisites**: Lakebase instance `fwa-investigations` must be AVAILABLE
# MAGIC with the schema already deployed.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Unity Catalog Name")

import random
import uuid
import psycopg
from databricks.sdk import WorkspaceClient

random.seed(42)

INSTANCE_NAME = "fwa-investigations"
DATABASE_NAME = "fwa_cases"
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

w = WorkspaceClient()

def get_connection():
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

# Verify connection
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fraud_investigators")
        print(f"Fraud investigators: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM fwa_investigations")
        print(f"Existing investigations: {cur.fetchone()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Investigations from Gold Tables

# COMMAND ----------

# Get investigator IDs for assignment
investigators = []
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT investigator_id, display_name FROM fraud_investigators WHERE is_active = TRUE")
        investigators = cur.fetchall()

print(f"Available investigators: {len(investigators)}")

# COMMAND ----------

cases_df = spark.sql(f"""
    SELECT investigation_id, investigation_type, target_type, target_id, target_name,
           fraud_types, severity, status, estimated_overpayment, claims_involved_count,
           investigation_summary, evidence_summary, rules_risk_score, ml_risk_score,
           created_date
    FROM {CATALOG}.fwa.silver_fwa_investigation_cases
    ORDER BY rules_risk_score DESC
""").collect()

with get_connection() as conn:
    with conn.cursor() as cur:
        count = 0
        for row in cases_df:
            # Assign investigator (round-robin for non-Open statuses)
            assigned_id = None
            if row.status != "Open":
                inv = investigators[count % len(investigators)]
                assigned_id = str(inv[0])

            rules_score = row.rules_risk_score or 0.5
            ml_score = row.ml_risk_score or 0.5
            composite = round(0.6 * rules_score + 0.4 * ml_score, 3)
            est_overpayment = float(row.estimated_overpayment or 0)

            # Compute confirmed/recovered amounts for closed & recovery cases
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
                """
                INSERT INTO fwa_investigations (
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
                    %s, %s, %s,
                    %s,
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
                    est_overpayment,
                    confirmed_overpayment,
                    recovered_amount,
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
    print(f"{count} investigation cases seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Evidence Records for Top Investigations

# COMMAND ----------

# Seed evidence records from flagged claims for top 20 investigations
top_inv = spark.sql(f"""
    SELECT i.investigation_id, i.target_id, i.target_type
    FROM {CATALOG}.fwa.silver_fwa_investigation_cases i
    ORDER BY i.rules_risk_score DESC
    LIMIT 20
""").collect()

with get_connection() as conn:
    with conn.cursor() as cur:
        evidence_count = 0
        for inv in top_inv:
            # Get flagged claims for this target
            if inv.target_type == "provider":
                claims = spark.sql(f"""
                    SELECT signal_id, claim_id, fraud_type, fraud_score, evidence_summary, estimated_overpayment
                    FROM {CATALOG}.fwa.silver_fwa_signals
                    WHERE provider_npi = '{inv.target_id}'
                    LIMIT 10
                """).collect()
            elif inv.target_type == "member":
                claims = spark.sql(f"""
                    SELECT signal_id, claim_id, fraud_type, fraud_score, evidence_summary, estimated_overpayment
                    FROM {CATALOG}.fwa.silver_fwa_signals
                    WHERE member_id = '{inv.target_id}'
                    LIMIT 10
                """).collect()
            else:
                claims = []

            for claim in claims:
                cur.execute(
                    """
                    INSERT INTO investigation_evidence (
                        investigation_id, evidence_type, reference_id, description, detail_json
                    ) VALUES (%s, 'claim', %s, %s, %s::jsonb)
                    """,
                    (
                        inv.investigation_id,
                        claim.claim_id,
                        claim.evidence_summary,
                        f'{{"signal_id": "{claim.signal_id}", "fraud_type": "{claim.fraud_type}", "fraud_score": {claim.fraud_score}, "estimated_overpayment": {claim.estimated_overpayment}}}',
                    ),
                )
                evidence_count += 1
        conn.commit()
    print(f"{evidence_count} evidence records seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fwa_investigations")
        total_inv = cur.fetchone()[0]
        cur.execute("SELECT status::text, count(*) FROM fwa_investigations GROUP BY status ORDER BY count(*) DESC")
        statuses = cur.fetchall()
        cur.execute("SELECT severity::text, count(*) FROM fwa_investigations GROUP BY severity ORDER BY count(*) DESC")
        severities = cur.fetchall()
        cur.execute("SELECT count(*) FROM investigation_audit_log")
        total_audit = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM investigation_evidence")
        total_evidence = cur.fetchone()[0]

print(f"\nTotal investigations: {total_inv}")
print(f"Audit log entries: {total_audit}")
print(f"Evidence records: {total_evidence}")
print("\nBy Status:")
for status, cnt in statuses:
    print(f"  {status}: {cnt}")
print("\nBy Severity:")
for sev, cnt in severities:
    print(f"  {sev}: {cnt}")

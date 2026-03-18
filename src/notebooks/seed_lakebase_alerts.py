# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Lakebase Alerts from Gold Tables
# MAGIC
# MAGIC Reads high-risk members, HEDIS care gaps, and denial patterns from gold tables
# MAGIC and inserts them as alerts into the Lakebase `red_bricks_alerts` database.
# MAGIC
# MAGIC **Prerequisites**: Lakebase instance `red-bricks-command-center` must be AVAILABLE
# MAGIC with the schema already deployed.

# COMMAND ----------

import uuid
import psycopg
from databricks.sdk import WorkspaceClient

INSTANCE_NAME = "red-bricks-command-center"
DATABASE_NAME = "red_bricks_alerts"
CATALOG = "catalog_insurance_vpx9o6"
SCHEMA = "red_bricks_insurance_dev"

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
        cur.execute("SELECT count(*) FROM care_managers")
        print(f"Care managers: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM risk_stratification_alerts")
        print(f"Existing alerts: {cur.fetchone()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed High-Risk Member Alerts (RAF > 2.0)

# COMMAND ----------

risk_df = spark.sql(f"""
    SELECT member_id, raf_score, hcc_codes, hcc_count,
           line_of_business, risk_rank, clinical_summary
    FROM {CATALOG}.{SCHEMA}.gold_member_risk_narrative
    WHERE risk_rank <= 100
    ORDER BY risk_rank
""").collect()

with get_connection() as conn:
    with conn.cursor() as cur:
        count = 0
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
                    row.member_id, row.member_id, row.member_id,
                    tier, round(risk_score, 2),
                    primary, secondary,
                    row.line_of_business, row.clinical_summary,
                ),
            )
            count += 1
        conn.commit()
    print(f"{count} high-risk member alerts seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed HEDIS Care Gap Alerts

# COMMAND ----------

hedis_df = spark.sql(f"""
    SELECT m.member_id, m.measure_name, m.is_compliant, m.line_of_business,
           e.avg_risk_score
    FROM (
        SELECT member_id, line_of_business, measure_name, is_compliant,
               ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY measure_name) as rn
        FROM {CATALOG}.{SCHEMA}.gold_hedis_member
        WHERE is_compliant = 0
    ) m
    LEFT JOIN (
        SELECT line_of_business, AVG(avg_risk_score) as avg_risk_score
        FROM {CATALOG}.{SCHEMA}.gold_enrollment_summary
        GROUP BY line_of_business
    ) e ON m.line_of_business = e.line_of_business
    WHERE m.rn = 1
    LIMIT 80
""").collect()

with get_connection() as conn:
    with conn.cursor() as cur:
        count = 0
        for row in hedis_df:
            primary = f"HEDIS Care Gap: {row.measure_name} — Non-compliant"
            secondary = [f"LOB: {row.line_of_business}"]

            cur.execute(
                """
                INSERT INTO risk_stratification_alerts (
                    patient_id, mrn, member_id, risk_tier, risk_score,
                    primary_driver, secondary_drivers, alert_source,
                    payer, status
                ) VALUES (
                    %s, %s, %s, 'Elevated'::risk_tier, %s,
                    %s, %s, 'SDOH Risk'::alert_source,
                    %s, 'Unassigned'::care_cycle_status
                )
                """,
                (
                    row.member_id, row.member_id, row.member_id,
                    35.0, primary, secondary,
                    row.line_of_business,
                ),
            )
            count += 1
        conn.commit()
    print(f"{count} HEDIS care gap alerts seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Denial Pattern Alerts

# COMMAND ----------

denial_df = spark.sql(f"""
    SELECT d.line_of_business, d.denial_category,
           SUM(d.denial_count) as total_denials,
           ROUND(SUM(d.total_denied_amount), 2) as total_denied_amt
    FROM {CATALOG}.{SCHEMA}.gold_denial_analysis d
    GROUP BY d.line_of_business, d.denial_category
    HAVING SUM(d.denial_count) > 500
    ORDER BY total_denials DESC
    LIMIT 20
""").collect()

with get_connection() as conn:
    with conn.cursor() as cur:
        count = 0
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
                    primary, secondary,
                    row.line_of_business,
                ),
            )
            count += 1
        conn.commit()
    print(f"{count} denial pattern alerts seeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM risk_stratification_alerts")
        total = cur.fetchone()[0]
        cur.execute("SELECT risk_tier::text, count(*) FROM risk_stratification_alerts GROUP BY risk_tier ORDER BY count(*) DESC")
        tiers = cur.fetchall()
        cur.execute("SELECT alert_source::text, count(*) FROM risk_stratification_alerts GROUP BY alert_source ORDER BY count(*) DESC")
        sources = cur.fetchall()

print(f"\nTotal alerts: {total}")
print("\nBy Risk Tier:")
for tier, cnt in tiers:
    print(f"  {tier}: {cnt}")
print("\nBy Alert Source:")
for src, cnt in sources:
    print(f"  {src}: {cnt}")

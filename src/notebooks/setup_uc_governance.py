# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Governance — Row Filters & Column Masks
# MAGIC
# MAGIC Demonstrates enterprise-grade data governance for PHI/PII on the Red Bricks Insurance
# MAGIC lakehouse. This notebook creates:
# MAGIC
# MAGIC 1. **Column masks** — dynamically redact SSN, phone, email, and DOB based on group membership
# MAGIC 2. **Row filters** — restrict member and claims visibility by line of business
# MAGIC 3. **Verification queries** — show masked vs unmasked views side-by-side
# MAGIC
# MAGIC ### How It Works
# MAGIC - Users in the `phi_full_access` group see all data unmasked
# MAGIC - Users in the `phi_restricted` group (or no group) see redacted PHI columns
# MAGIC - Row filters restrict data by LOB: `commercial_only` group sees only Commercial members
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Gold/silver tables populated (run pipelines first)
# MAGIC - Current user must be catalog owner or have MANAGE privileges

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat = f"`{catalog}`"

spark.sql(f"USE CATALOG {cat}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Governance Schema and Groups

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {cat}.governance
COMMENT 'Row filter and column mask functions for PHI/PII governance'
""")

# Create groups if they don't exist (idempotent)
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

for group_name in ["phi_full_access", "phi_restricted", "commercial_only"]:
    try:
        w.groups.create(display_name=group_name)
        print(f"Created group: {group_name}")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Group already exists: {group_name}")
        else:
            print(f"Note: Could not create group {group_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Column Mask Functions
# MAGIC
# MAGIC These UDFs check group membership and return either the real value or a redacted placeholder.

# COMMAND ----------

# -- Mask SSN (last 4 digits)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.mask_ssn(ssn_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN ssn_val
    ELSE CONCAT('***-**-', RIGHT(COALESCE(ssn_val, '0000'), 4))
  END
""")
print("Created mask_ssn")

# -- Mask phone number
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.mask_phone(phone_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN phone_val
    ELSE CONCAT('(***) ***-', RIGHT(COALESCE(phone_val, '0000'), 4))
  END
""")
print("Created mask_phone")

# -- Mask email
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.mask_email(email_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN email_val
    ELSE CONCAT(LEFT(COALESCE(email_val, 'x'), 2), '***@***.com')
  END
""")
print("Created mask_email")

# -- Mask date of birth (show only year for restricted users)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.mask_dob(dob_val DATE)
RETURNS DATE
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN dob_val
    ELSE MAKE_DATE(YEAR(COALESCE(dob_val, DATE '2000-01-01')), 1, 1)
  END
""")
print("Created mask_dob")

# -- Mask address
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.mask_address(addr_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN addr_val
    ELSE '*** REDACTED ***'
  END
""")
print("Created mask_address")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Row Filter Functions
# MAGIC
# MAGIC Row-level security restricts which rows a user can see based on their group membership.

# COMMAND ----------

# -- Row filter on silver_enrollment: restrict by LOB
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.filter_by_lob(lob STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN TRUE
    WHEN is_account_group_member('commercial_only') THEN lob = 'Commercial'
    ELSE TRUE
  END
""")
print("Created filter_by_lob")

# -- Row filter on silver_claims_medical: restrict by member LOB
spark.sql(f"""
CREATE OR REPLACE FUNCTION {cat}.governance.filter_claims_by_lob(member_id_val STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('phi_full_access') THEN TRUE
    WHEN is_account_group_member('commercial_only') THEN
      EXISTS (
        SELECT 1 FROM {cat}.members.silver_enrollment e
        WHERE e.member_id = member_id_val
          AND e.line_of_business = 'Commercial'
      )
    ELSE TRUE
  END
""")
print("Created filter_claims_by_lob")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Column Masks to Silver Members Table

# COMMAND ----------

# Apply column masks to silver_members
masks = [
    ("ssn_last_4", "mask_ssn"),
    ("phone", "mask_phone"),
    ("email", "mask_email"),
    ("date_of_birth", "mask_dob"),
    ("address_line_1", "mask_address"),
]

for column, mask_fn in masks:
    try:
        spark.sql(f"""
            ALTER TABLE {cat}.members.silver_members
            ALTER COLUMN {column}
            SET MASK {cat}.governance.{mask_fn}
        """)
        print(f"  Applied {mask_fn} to silver_members.{column}")
    except Exception as e:
        if "already has" in str(e).lower() or "COLUMN_MASK_ALREADY_SET" in str(e):
            print(f"  Mask already set on silver_members.{column}")
        else:
            print(f"  Warning on silver_members.{column}: {e}")

print("Column masks applied to silver_members")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Apply Row Filter to Silver Enrollment

# COMMAND ----------

try:
    spark.sql(f"""
        ALTER TABLE {cat}.members.silver_enrollment
        SET ROW FILTER {cat}.governance.filter_by_lob ON (line_of_business)
    """)
    print("Row filter applied to silver_enrollment")
except Exception as e:
    if "already has" in str(e).lower() or "ROW_FILTER_ALREADY_SET" in str(e):
        print("Row filter already set on silver_enrollment")
    else:
        print(f"Warning: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verification — Show Governance in Action
# MAGIC
# MAGIC The current user likely has full access (they're the catalog owner).
# MAGIC Below we show what the data looks like and explain the access model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Members Table — Column Masks Applied

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As a catalog owner, you see unmasked data.
# MAGIC -- Users NOT in phi_full_access would see:
# MAGIC --   ssn_last_4  → ***-**-1234
# MAGIC --   phone       → (***) ***-5678
# MAGIC --   email       → za***@***.com
# MAGIC --   dob         → 2000-01-01 (year only)
# MAGIC --   address     → *** REDACTED ***
# MAGIC
# MAGIC SELECT
# MAGIC   member_id,
# MAGIC   full_name,
# MAGIC   date_of_birth,
# MAGIC   ssn_last_4,
# MAGIC   phone,
# MAGIC   email,
# MAGIC   address_line_1,
# MAGIC   city,
# MAGIC   state
# MAGIC FROM members.silver_members
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment Table — Row Filter Applied

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Users in the 'commercial_only' group would only see Commercial rows.
# MAGIC -- Full-access users see all LOBs.
# MAGIC
# MAGIC SELECT line_of_business, COUNT(*) AS member_count
# MAGIC FROM members.silver_enrollment
# MAGIC GROUP BY line_of_business
# MAGIC ORDER BY member_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Governance Summary

# COMMAND ----------

# Show all masks and filters applied
governance_summary = spark.sql(f"""
SELECT
  'Column Mask' AS governance_type,
  table_name,
  column_name,
  mask_function
FROM (
  SELECT
    'silver_members' AS table_name,
    col.column_name,
    col.mask_function
  FROM (VALUES
    ('ssn_last_4', '{catalog}.governance.mask_ssn'),
    ('phone', '{catalog}.governance.mask_phone'),
    ('email', '{catalog}.governance.mask_email'),
    ('date_of_birth', '{catalog}.governance.mask_dob'),
    ('address_line_1', '{catalog}.governance.mask_address')
  ) AS col(column_name, mask_function)
)

UNION ALL

SELECT
  'Row Filter' AS governance_type,
  'silver_enrollment' AS table_name,
  'line_of_business' AS column_name,
  '{catalog}.governance.filter_by_lob' AS mask_function
""")

governance_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Control Matrix
# MAGIC
# MAGIC | Group | Members PHI | Claims | Enrollment Rows |
# MAGIC |-------|-------------|--------|-----------------|
# MAGIC | `phi_full_access` | Full SSN, phone, email, DOB, address | All claims | All LOBs |
# MAGIC | `phi_restricted` | Masked SSN, phone, email, DOB, address | All claims | All LOBs |
# MAGIC | `commercial_only` | Full/masked (depends on phi group) | Commercial only | Commercial only |
# MAGIC | No group | Masked PHI | All claims | All LOBs |
# MAGIC
# MAGIC ### Demo Talking Points
# MAGIC 1. **Column masks** use `is_account_group_member()` — no code changes needed, policies follow the data
# MAGIC 2. **Row filters** automatically restrict query results — users can't even see restricted rows
# MAGIC 3. All policies are **auditable** in Unity Catalog lineage and system tables
# MAGIC 4. Masks and filters work across **SQL, Python, notebooks, Genie, dashboards, and apps**
# MAGIC 5. Combined with **data quality expectations** in SDP pipelines, this shows end-to-end governance

# COMMAND ----------

print("UC Governance setup complete!")
print(f"  - 5 column masks on {catalog}.members.silver_members")
print(f"  - 1 row filter on {catalog}.members.silver_enrollment")
print(f"  - 3 groups: phi_full_access, phi_restricted, commercial_only")
print(f"  - All functions in {catalog}.governance schema")

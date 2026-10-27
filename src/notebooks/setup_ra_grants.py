# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Risk Adjustment app service-principal grants
# MAGIC
# MAGIC RA-specific grants the generic `bootstrap_workspace` loop does not cover:
# MAGIC 1. **Vector Search endpoint** `CAN_USE` so the agent's `search_clinical_notes` tool works.
# MAGIC 2. A **Lakebase role** on the dedicated `red-bricks-ra-memory` instance so chat history,
# MAGIC    agent memory CRUD, and adjudication writes persist.
# MAGIC
# MAGIC Catalog/schema SELECT + warehouse CAN_USE are already granted generically by
# MAGIC `bootstrap_workspace` (the RA app is auto-discovered by name). The FM endpoint grant is
# MAGIC handled by the app's DAB resource (`app_risk_adjustment.yml`).
# MAGIC
# MAGIC Every grant is **best-effort + idempotent**: on a redeploy these already exist, so each
# MAGIC step is wrapped to re-assert without failing the job.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog")
dbutils.widgets.text("app_name", "red-bricks-ra-workbench", "RA app name")
dbutils.widgets.text("instance_name", "red-bricks-ra-memory", "Lakebase instance")
dbutils.widgets.text("vs_endpoint_name", "red-bricks-vs-endpoint", "Vector Search endpoint")

CATALOG = dbutils.widgets.get("catalog")
APP_NAME = dbutils.widgets.get("app_name")
INSTANCE = dbutils.widgets.get("instance_name")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint_name")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ### Resolve the RA app's service principal (created at `bundle deploy`)

# COMMAND ----------
sp_client_id = None
try:
    app = w.apps.get(name=APP_NAME)
    sp_client_id = getattr(app, "service_principal_client_id", None)
    if not sp_client_id:
        sp_id = getattr(app, "service_principal_id", None)
        if sp_id:
            sp_client_id = w.service_principals.get(str(sp_id)).application_id
    print(f"{APP_NAME}: SP client_id = {sp_client_id}")
except Exception as e:
    print(f"Could not resolve app '{APP_NAME}' SP (is the app deployed yet?): {e}")

if not sp_client_id:
    dbutils.notebook.exit("SKIPPED — RA app SP not found; grants will be applied on a later run once the app exists.")

# COMMAND ----------
# MAGIC %md ### 1. Vector Search endpoint CAN_USE

# COMMAND ----------
try:
    ep = w.vector_search_endpoints.get_endpoint(endpoint_name=VS_ENDPOINT)
    ep_id = getattr(ep, "id", None) or getattr(ep, "endpoint_id", None)
    if ep_id:
        w.api_client.do(
            "PATCH", f"/api/2.0/permissions/vector-search-endpoints/{ep_id}",
            body={"access_control_list": [
                {"service_principal_name": sp_client_id, "permission_level": "CAN_USE"}]})
        print(f"Granted CAN_USE on VS endpoint '{VS_ENDPOINT}' (id={ep_id}) to {sp_client_id}")
    else:
        print(f"VS endpoint '{VS_ENDPOINT}' has no resolvable id; skipping VS grant.")
except Exception as e:
    print(f"VS endpoint grant skipped: {e}")

# COMMAND ----------
# MAGIC %md ### 2. Lakebase role on the RA memory instance

# COMMAND ----------
from databricks.sdk.service.database import (DatabaseInstanceRole,
                                             DatabaseInstanceRoleIdentityType)

# 2a. Register the SP as a federated Postgres role on the instance (idempotent).
try:
    w.database.create_database_instance_role(
        instance_name=INSTANCE,
        database_instance_role=DatabaseInstanceRole(
            name=sp_client_id,
            identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL))
    print(f"Registered SP role on '{INSTANCE}' for {sp_client_id}")
except Exception as e:
    print(f"SP role registration (likely already exists): {e}")

# 2b. As instance owner, grant the role the privileges the app needs on schema public.
try:
    import psycopg2
    inst = w.database.get_database_instance(name=INSTANCE)
    cred = w.database.generate_database_credential(instance_names=[INSTANCE], request_id="ra-grants")
    owner = w.current_user.me().user_name
    conn = psycopg2.connect(host=inst.read_write_dns, port=5432, dbname="databricks_postgres",
                            user=owner, password=cred.token, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()
    role = sp_client_id  # PG role name == SP client_id
    for stmt in [
        f'GRANT USAGE, CREATE ON SCHEMA public TO "{role}"',
        f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{role}"',
        f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{role}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO "{role}"',
    ]:
        try:
            cur.execute(stmt)
        except Exception as ge:
            print(f"  grant skipped: {ge}")
    cur.close(); conn.close()
    print(f"Granted public-schema privileges on '{INSTANCE}' to {sp_client_id}")
except Exception as e:
    print(f"Lakebase privilege grant skipped: {e}")

# COMMAND ----------
print("RA grants complete (best-effort).")

# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy App Source Code
# MAGIC
# MAGIC Starts app compute (if stopped) and deploys the latest source code for all
# MAGIC Red Bricks Insurance apps. Runs as the final task in the demo pipeline so
# MAGIC apps are ready to use immediately after a fresh deploy.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Unity Catalog Name")

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

w = WorkspaceClient()

# Resolve the bundle files root from the notebook path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_ws_root = (
    "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0]
    if not _nb_path.startswith("/Workspace")
    else _nb_path.rsplit("/src/notebooks/", 1)[0]
)

# App name → source code subdirectory (relative to bundle root)
APP_SOURCE_MAP = {
    "red-bricks-command-center-app": "app",
    "red-bricks-fwa-portal-app": "app-fwa",
    "red-bricks-pa-portal-app": "app-prior-auth",
}

# Apps with target-suffix names — detect dynamically
for app in w.apps.list():
    name = app.name
    if name.startswith("rb-grp-rpt-"):
        APP_SOURCE_MAP[name] = "app-group-reporting"
    elif name.startswith("rb-uw-sim-"):
        APP_SOURCE_MAP[name] = "app-underwriting-sim"

print(f"Bundle root: {_ws_root}")
print(f"Apps to deploy: {list(APP_SOURCE_MAP.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Compute & Deploy

# COMMAND ----------

def wait_for_compute(app_name: str, timeout: int = 600) -> str:
    """Wait for app compute to be RUNNING. Returns final status."""
    for i in range(timeout // 10):
        app = w.apps.get(app_name)
        status = app.compute_status.state.value if app.compute_status else "UNKNOWN"
        if status == "ACTIVE":
            return "ACTIVE"
        if i % 6 == 0:
            print(f"  [{app_name}] compute: {status} ({i*10}s)")
        time.sleep(10)
    return status


results = {}
deploy_ids = {}

for app_name, source_dir in APP_SOURCE_MAP.items():
    print(f"\n{'='*50}")
    print(f"App: {app_name}")
    print(f"{'='*50}")

    source_path = f"{_ws_root}/{source_dir}"

    # 1. Check compute status and start if needed
    app = w.apps.get(app_name)
    compute_state = app.compute_status.state.value if app.compute_status else "UNKNOWN"
    print(f"  Compute status: {compute_state}")

    if compute_state in ("STOPPED", "ERROR"):
        print(f"  Starting compute...")
        try:
            w.apps.start(app_name)
        except Exception as e:
            print(f"  Start request: {e}")

    # 2. Wait for compute to be active
    if compute_state != "ACTIVE":
        final_state = wait_for_compute(app_name)
        if final_state != "ACTIVE":
            print(f"  WARNING: Compute did not reach ACTIVE ({final_state}). Attempting deploy anyway...")

    # 3. Deploy source code
    print(f"  Deploying from {source_path}...")
    try:
        deploy_wait = w.apps.deploy(
            app_name=app_name,
            app_deployment=AppDeployment(
                source_code_path=source_path,
                mode=AppDeploymentMode.SNAPSHOT,
            ),
        )
        dep_id = deploy_wait.response.deployment_id
        deploy_ids[app_name] = dep_id
        results[app_name] = "DEPLOYED"
        print(f"  Deployment initiated: {dep_id}")
    except Exception as e:
        results[app_name] = f"FAILED: {e}"
        print(f"  Deploy failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Deployments

# COMMAND ----------

print("\nWaiting for all deployments to complete...")
for app_name in APP_SOURCE_MAP:
    if not results.get(app_name, "").startswith("DEPLOYED"):
        continue
    target_dep_id = deploy_ids.get(app_name)
    for i in range(60):  # up to 10 min per app
        app = w.apps.get(app_name)
        # Check pending deployment first (our deploy may not be active yet)
        dep = app.pending_deployment if app.pending_deployment else app.active_deployment
        if dep and dep.status and dep.deployment_id == target_dep_id:
            dep_state = dep.status.state.value
            if dep_state == "SUCCEEDED":
                results[app_name] = "SUCCESS"
                print(f"  {app_name}: deployment SUCCEEDED")
                break
            elif dep_state == "FAILED":
                msg = dep.status.message or ""
                results[app_name] = f"DEPLOY_FAILED: {msg[:200]}"
                print(f"  {app_name}: deployment FAILED — {msg[:200]}")
                break
        time.sleep(10)
    else:
        results[app_name] = "TIMEOUT"
        print(f"  {app_name}: deployment timed out")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("APP DEPLOYMENT SUMMARY")
print("=" * 60)
for app_name, result in results.items():
    status_icon = "OK" if result == "SUCCESS" else "WARN"
    print(f"  [{status_icon}] {app_name}: {result}")

# Get app URLs
for app in w.apps.list():
    if app.name in APP_SOURCE_MAP:
        print(f"  URL: {app.url}")

failed = [k for k, v in results.items() if v not in ("SUCCESS", "DEPLOYED")]
if failed:
    msg = f"{len(failed)} app(s) had deployment issues: {failed}"
    print(f"\nERROR: {msg}")
    raise RuntimeError(msg)
else:
    print(f"\nAll {len(results)} apps deployed successfully.")

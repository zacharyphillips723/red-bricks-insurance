# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Clinical FHIR Data
# MAGIC
# MAGIC Downloads pre-generated Synthea FHIR R4 bundles from a private GitHub repo
# MAGIC and extracts them to the UC Volume. **Skips automatically** if FHIR bundles
# MAGIC already exist in the target volume path.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Databricks secret scope `github` with key `pat` containing a GitHub PAT
# MAGIC   that has read access to the `synthea-fhir-data` repo.
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope github
# MAGIC databricks secrets put-secret github pat --string-value "ghp_..."
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog Name")
catalog = dbutils.widgets.get("catalog")

volume_base = f"/Volumes/{catalog}/raw/raw_sources"
fhir_path = f"{volume_base}/synthea_raw/fhir"

print(f"Catalog:   {catalog}")
print(f"FHIR path: {fhir_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Existing Data

# COMMAND ----------

try:
    files = dbutils.fs.ls(fhir_path)
    patient_bundles = [
        f for f in files
        if not f.name.startswith("hospital")
        and not f.name.startswith("practitioner")
    ]
    if len(patient_bundles) > 100:
        print(f"FHIR data already seeded ({len(patient_bundles)} patient bundles). Skipping download.")
        dbutils.notebook.exit("ALREADY_SEEDED")
    else:
        print(f"Found only {len(patient_bundles)} bundles — re-seeding.")
except Exception:
    print("No existing FHIR data found. Will download from GitHub.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and Extract

# COMMAND ----------

import subprocess
import tarfile
import os

# Read GitHub PAT from Databricks secrets
token = dbutils.secrets.get(scope="github", key="pat")
repo_url = f"https://{token}@github.com/zack-phillips_data/synthea-fhir-data.git"
tarball_name = "synthea_fhir_5k.tar.gz"
clone_dir = "/tmp/synthea-fhir-data"

# Clean up any previous clone
subprocess.run(["rm", "-rf", clone_dir], capture_output=True)

# Shallow clone with LFS (pulls just the tarball, not full history)
print("Cloning synthea-fhir-data repo (Git LFS)...")
subprocess.run(["git", "lfs", "install"], check=True, capture_output=True)
result = subprocess.run(
    ["git", "clone", "--depth", "1", repo_url, clone_dir],
    capture_output=True, text=True,
)
if result.returncode != 0:
    print(f"STDERR: {result.stderr}")
    raise RuntimeError(f"git clone failed (exit {result.returncode}). Check GitHub PAT in secrets scope 'github'.")

tarball_path = os.path.join(clone_dir, tarball_name)
if not os.path.exists(tarball_path):
    raise FileNotFoundError(f"Tarball not found at {tarball_path}. Check repo contents.")

size_gb = os.path.getsize(tarball_path) / (1024 ** 3)
print(f"Downloaded {tarball_name} ({size_gb:.2f} GB)")

# COMMAND ----------

# Extract to volume
print(f"Extracting to {volume_base}/synthea_raw/ ...")
with tarfile.open(tarball_path, "r:gz") as tar:
    tar.extractall(f"{volume_base}/synthea_raw/")

print("Extraction complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

files = dbutils.fs.ls(fhir_path)
patient_bundles = [
    f for f in files
    if not f.name.startswith("hospital")
    and not f.name.startswith("practitioner")
]
print(f"Seeded {len(patient_bundles)} patient FHIR bundles to {fhir_path}")

# Cleanup temp clone
subprocess.run(["rm", "-rf", clone_dir], capture_output=True)

dbutils.notebook.exit(f"SEEDED:{len(patient_bundles)}")

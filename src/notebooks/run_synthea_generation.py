# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Synthea Patient Generation
# MAGIC
# MAGIC Downloads and runs [Synthea](https://github.com/synthetichealth/synthea), an open-source
# MAGIC synthetic patient generator, to produce **5,000 clinically realistic FHIR R4 bundles**
# MAGIC for North Carolina residents. These bundles are the input to the crosswalk step
# MAGIC (Phase 2) which rewrites Synthea UUIDs to existing Red Bricks MBR IDs.
# MAGIC
# MAGIC **Output:** `{volume_base}/synthea_raw/fhir/` — one JSON Bundle per patient
# MAGIC
# MAGIC **Requirements:** Java 11+ (serverless environment v5+ recommended — ships with Java 17)

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
dbutils.widgets.text("num_patients", "5000", "Number of patients")

catalog = dbutils.widgets.get("catalog")
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

# Ensure schema and volume exist before any file operations
# (catalog is expected to already exist — created by workspace admin or FEVM)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.raw.raw_sources")

print(f"Catalog: {catalog}")
print(f"Volume base: {volume_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import subprocess
import shutil
import glob
import json

# Synthea version — pinned for reproducibility
SYNTHEA_VERSION = "3.3.0"
SYNTHEA_JAR_URL = f"https://github.com/synthetichealth/synthea/releases/download/v{SYNTHEA_VERSION}/synthea-with-dependencies.jar"
SYNTHEA_JAR_NAME = f"synthea-{SYNTHEA_VERSION}-with-dependencies.jar"

# Local working directory on the cluster node
LOCAL_WORK_DIR = "/tmp/synthea_workdir"
LOCAL_JAR_PATH = f"{LOCAL_WORK_DIR}/{SYNTHEA_JAR_NAME}"
LOCAL_OUTPUT_DIR = f"{LOCAL_WORK_DIR}/output"

# Volume paths
VOLUME_SYNTHEA_RAW = f"{volume_base}/synthea_raw/fhir"
VOLUME_JAR_CACHE = f"{volume_base}/synthea_raw"  # cache JAR in volume across runs

# Generation parameters
NUM_PATIENTS = int(dbutils.widgets.get("num_patients"))
STATE = "North Carolina"
CITY = None  # None = all cities in the state
SEED = 42
SYNTHEA_THREADS = os.cpu_count() or 4  # use all available vCPUs
COPY_THREADS = 16                       # parallel file copy to volume

print(f"Synthea version: {SYNTHEA_VERSION}")
print(f"Patients: {NUM_PATIENTS}, State: {STATE}, Seed: {SEED}")
print(f"Synthea threads: {SYNTHEA_THREADS}, Copy threads: {COPY_THREADS}")
print(f"Output: {VOLUME_SYNTHEA_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Download Synthea JAR

# COMMAND ----------

os.makedirs(LOCAL_WORK_DIR, exist_ok=True)

# Volume cache path — may not exist on first run if schema/volume haven't been created yet
volume_jar_path = f"{VOLUME_JAR_CACHE}/{SYNTHEA_JAR_NAME}"
_volume_available = os.path.isdir(VOLUME_JAR_CACHE)
if not _volume_available:
    print(f"Volume path {VOLUME_JAR_CACHE} not yet available — will download JAR directly")

if os.path.exists(LOCAL_JAR_PATH):
    print(f"Synthea JAR already present at {LOCAL_JAR_PATH}, skipping download.")
elif _volume_available and os.path.exists(volume_jar_path) and os.path.getsize(volume_jar_path) > 50 * 1024 * 1024:
    # Copy cached JAR from UC Volume to local /tmp (much faster than GitHub download)
    print(f"Copying cached JAR from volume ({volume_jar_path})...")
    import time as _t; _cp_start = _t.time()
    shutil.copy2(volume_jar_path, LOCAL_JAR_PATH)
    print(f"Copied in {_t.time() - _cp_start:.1f}s")
else:
    print(f"Downloading Synthea {SYNTHEA_VERSION} from GitHub releases...")
    result = subprocess.run(
        ["curl", "-L", "-o", LOCAL_JAR_PATH, SYNTHEA_JAR_URL],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to download Synthea JAR: {result.stderr}")
    size_mb = os.path.getsize(LOCAL_JAR_PATH) / (1024 * 1024)
    print(f"Downloaded {LOCAL_JAR_PATH} ({size_mb:.1f} MB)")
    # Cache in UC Volume for future runs (if volume exists)
    if _volume_available:
        shutil.copy2(LOCAL_JAR_PATH, volume_jar_path)
        print(f"Cached JAR to {volume_jar_path} for future runs")

# Verify the JAR was downloaded (check file size — should be ~300+ MB)
jar_size_mb = os.path.getsize(LOCAL_JAR_PATH) / (1024 * 1024)
if jar_size_mb < 50:
    raise RuntimeError(f"JAR file is only {jar_size_mb:.1f} MB — download likely failed or redirected to HTML")
print(f"Synthea JAR size: {jar_size_mb:.1f} MB")

# Check Java version — Synthea 3.3.0 requires Java 11+
java_ver = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=30)
java_ver_str = (java_ver.stderr or java_ver.stdout or "").strip()
java_ver_line = java_ver_str.split(chr(10))[0]
print(f"Java version: {java_ver_line}")

# Parse major version number (works for both "1.8.x" and "11.x"/"17.x" formats)
import re
ver_match = re.search(r'"(\d+)[\._]', java_ver_line)
java_major = int(ver_match.group(1)) if ver_match else 0
if java_major == 1:
    # Java 8 and earlier use "1.8.x" format
    inner = re.search(r'"1\.(\d+)', java_ver_line)
    java_major = int(inner.group(1)) if inner else 0
if java_major < 11:
    raise RuntimeError(
        f"Synthea 3.3.0 requires Java 11+ but this environment has Java {java_major}. "
        "Use serverless environment v5+ (Java 17) or a classic cluster with DBR 16.x+."
    )
print(f"Java major version: {java_major} ✓")

# Smoke test — skip if JAR was loaded from cache (already validated on a prior run)
_jar_was_cached = _volume_available and os.path.exists(volume_jar_path) and os.path.getsize(volume_jar_path) > 50 * 1024 * 1024
if _jar_was_cached:
    print("JAR loaded from cache — skipping smoke test")
else:
    result = subprocess.run(
        ["java", "-jar", LOCAL_JAR_PATH, "-p", "0"],
        capture_output=True, text=True, timeout=120,
    )
    print(f"Smoke test stdout (last 5 lines):")
    for line in (result.stdout or "").strip().split("\n")[-5:]:
        print(f"  {line}")
    if result.returncode != 0:
        print(f"Smoke test stderr: {(result.stderr or '')[:1000]}")
        print(f"Smoke test stdout: {(result.stdout or '')[:1000]}")
        if "UnsupportedClassVersionError" in (result.stderr or ""):
            raise RuntimeError(
                f"Synthea 3.3.0 requires Java 11+ but the JVM reports Java {java_major}. "
                "Use serverless environment v5+ (Java 17) or a classic cluster with DBR 16.x+."
            )
        print("WARNING: Smoke test returned non-zero — continuing anyway")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Configure and Run Synthea
# MAGIC
# MAGIC Key configuration:
# MAGIC - **FHIR R4 JSON + CSV observations** — FHIR bundles for clinical pipeline, CSV for flat lab queries
# MAGIC - **Transaction bundles** — each patient gets a full Bundle with all resources
# MAGIC - **Seed 42** — reproducible output across runs
# MAGIC - **North Carolina** — matches existing Red Bricks member geography
# MAGIC - **Forced lab modules** — Metabolic Panel, Lipid Panel, and CBC run for ALL patients
# MAGIC   (not just epidemiologically selected ones), ensuring rich lab data for demos
# MAGIC - **DiagnosticReport grouping** — related lab observations are grouped into reports
# MAGIC - **10-year history** — balanced for clinical richness vs. generation speed on serverless

# COMMAND ----------

# Clean previous output
if os.path.exists(LOCAL_OUTPUT_DIR):
    shutil.rmtree(LOCAL_OUTPUT_DIR)
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# Write Synthea override properties
# These override the defaults bundled inside the JAR
synthea_props = f"""\
# === Export Settings ===
exporter.fhir.export = true
exporter.fhir.transaction_bundle = true
exporter.csv.export = true
exporter.csv.folder_per_run = true
exporter.ccda.export = false
exporter.cpcds.export = false
exporter.hospital.fhir.export = false
exporter.practitioner.fhir.export = false

# === DiagnosticReport grouping — groups related lab Observations into reports ===
exporter.groups.fhir.export = true

# === Output Location ===
exporter.baseDirectory = {LOCAL_OUTPUT_DIR}

# === Generation Settings ===
generate.payers.insurance_companies.default_file = payers/insurance_companies.csv
exporter.years_of_history = 10

# === Force comprehensive lab panels for ALL patients ===
generate.keep_module.Metabolic Panel = true
generate.keep_module.Lipid Panel = true
generate.keep_module.CBC = true

# === Seed for Reproducibility ===
generate.seed = {SEED}
"""

props_path = f"{LOCAL_WORK_DIR}/synthea.properties"
with open(props_path, "w") as f:
    f.write(synthea_props)

print("Synthea properties written:")
print(synthea_props)

# COMMAND ----------

# Build the Synthea command
# Synthea CLI: java -jar synthea.jar [-s seed] [-p population] [-c config] [state [city]]
synthea_cmd = [
    "java",
    "-Xms2g", "-Xmx4g",           # pre-allocate 2GB heap to reduce GC resizing
    "-XX:+UseG1GC",                # G1 collector — better throughput for large heaps
    "-XX:+ParallelRefProcEnabled", # parallelize reference processing
    "-jar", LOCAL_JAR_PATH,
    "-s", str(SEED),               # random seed
    "-p", str(NUM_PATIENTS),       # population size
    "-t", str(SYNTHEA_THREADS),    # parallel generation threads
    "-c", props_path,              # properties file
    STATE,                         # state (positional, must be last)
]

print(f"Running: {' '.join(synthea_cmd)}")
est_min = max(5, NUM_PATIENTS // 200)  # ~200 patients/min on 4 serverless CPUs
print(f"Generating {NUM_PATIENTS} patients — estimated ~{est_min} minutes on {SYNTHEA_THREADS} CPUs")
print(f"Working directory: {LOCAL_WORK_DIR}")

result = subprocess.run(
    synthea_cmd,
    capture_output=True, text=True,
    timeout=3600,  # 60-minute timeout (serverless 4-CPU nodes need ~40-50 min for 5K patients)
    cwd=LOCAL_WORK_DIR,            # run from workdir so relative paths resolve
)

# Print last 30 lines of stdout (progress/summary)
stdout_lines = (result.stdout or "").strip().split("\n")
print(f"\n--- Synthea stdout (last 30 lines of {len(stdout_lines)} total) ---")
for line in stdout_lines[-30:]:
    print(line)

if result.stderr:
    stderr_lines = result.stderr.strip().split("\n")
    print(f"\n--- Synthea stderr (last 20 lines of {len(stderr_lines)} total) ---")
    for line in stderr_lines[-20:]:
        print(line)

if result.returncode != 0:
    # Include stdout+stderr in the exception so it shows up in run output
    err_detail = f"STDOUT:\n{(result.stdout or '')[-2000:]}\n\nSTDERR:\n{(result.stderr or '')[-2000:]}"
    raise RuntimeError(f"Synthea exited with code {result.returncode}\n{err_detail}")

print(f"\nSynthea completed successfully (exit code {result.returncode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Validate Output

# COMMAND ----------

# Find FHIR JSON bundles
fhir_output_dir = os.path.join(LOCAL_OUTPUT_DIR, "fhir")
if not os.path.isdir(fhir_output_dir):
    raise FileNotFoundError(
        f"Expected FHIR output at {fhir_output_dir}. "
        f"Contents of {LOCAL_OUTPUT_DIR}: {os.listdir(LOCAL_OUTPUT_DIR)}"
    )

bundle_files = glob.glob(os.path.join(fhir_output_dir, "*.json"))
# Exclude hospital and practitioner info files
patient_bundles = [
    f for f in bundle_files
    if not os.path.basename(f).startswith(("hospitalInformation", "practitionerInformation"))
]
print(f"Total FHIR JSON files: {len(bundle_files)}")
print(f"Patient bundles: {len(patient_bundles)}")

if len(patient_bundles) < NUM_PATIENTS * 0.95:
    print(f"WARNING: Expected ~{NUM_PATIENTS} patient bundles, got {len(patient_bundles)}")

# Sample validation — check first bundle structure
sample_path = patient_bundles[0]
with open(sample_path, "r") as f:
    sample = json.load(f)

print(f"\nSample bundle: {os.path.basename(sample_path)}")
print(f"  resourceType: {sample.get('resourceType')}")
print(f"  type: {sample.get('type')}")
entries = sample.get("entry", [])
print(f"  entries: {len(entries)}")

# Count resource types in sample
resource_types = {}
for entry in entries:
    rt = entry.get("resource", {}).get("resourceType", "Unknown")
    resource_types[rt] = resource_types.get(rt, 0) + 1
print(f"  resource types: {json.dumps(resource_types, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Copy to UC Volume

# COMMAND ----------

# Clean previous Synthea data in the volume
try:
    dbutils.fs.rm(f"dbfs:{VOLUME_SYNTHEA_RAW}", recurse=True)
    print(f"Cleaned previous data at {VOLUME_SYNTHEA_RAW}")
except Exception:
    pass

# Create the volume directory
os.makedirs(VOLUME_SYNTHEA_RAW, exist_ok=True)

# Copy bundles to volume AND extract Patient demographics in a single parallel pass.
# This avoids a separate Spark multiline JSON read of 5K complex FHIR bundles.
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

def _copy_and_extract(src_path: str):
    """Copy bundle to volume and extract Patient demographics in one read."""
    dest = os.path.join(VOLUME_SYNTHEA_RAW, os.path.basename(src_path))
    shutil.copy2(src_path, dest)

    # Extract Patient resource (first entry with resourceType == "Patient")
    with open(src_path, "r") as f:
        bundle = json.load(f)
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            names = resource.get("name", [{}])
            addrs = resource.get("address", [{}])
            # Strip trailing digits Synthea appends to names (e.g., "Aaron208" → "Aaron")
            import re
            raw_first = (names[0].get("given") or [""])[0]
            raw_last = names[0].get("family") or ""
            clean_first = re.sub(r'\d+$', '', raw_first)
            clean_last = re.sub(r'\d+$', '', raw_last)
            return {
                "synthea_uuid": resource.get("id"),
                "first_name": clean_first,
                "last_name": clean_last,
                "date_of_birth": resource.get("birthDate"),
                "gender": resource.get("gender"),
                "address_line_1": (addrs[0].get("line") or [""])[0],
                "city": addrs[0].get("city"),
                "state": addrs[0].get("state"),
                "zip_code": addrs[0].get("postalCode"),
            }
    return None

print(f"Copying {len(patient_bundles)} bundles + extracting demographics using {COPY_THREADS} threads...")
copy_start = time.time()
patients_raw = []
copied = 0
with ThreadPoolExecutor(max_workers=COPY_THREADS) as executor:
    futures = {executor.submit(_copy_and_extract, p): p for p in patient_bundles}
    for future in as_completed(futures):
        result = future.result()
        if result:
            patients_raw.append(result)
        copied += 1
        if copied % 1000 == 0:
            elapsed = time.time() - copy_start
            print(f"  Processed {copied}/{len(patient_bundles)} bundles ({elapsed:.0f}s)")

copy_elapsed = time.time() - copy_start
print(f"Copied {copied} bundles + extracted {len(patients_raw)} patients in {copy_elapsed:.0f}s")

# Also copy hospital/practitioner info for reference
for f in bundle_files:
    basename = os.path.basename(f)
    if basename.startswith(("hospitalInformation", "practitionerInformation")):
        shutil.copy2(f, os.path.join(VOLUME_SYNTHEA_RAW, basename))
        print(f"  Copied reference file: {basename}")

# Copy CSV observations to volume for flat lab queries
csv_output_dir = os.path.join(LOCAL_OUTPUT_DIR, "csv")
VOLUME_SYNTHEA_CSV = f"{volume_base}/synthea_raw/csv"
if os.path.isdir(csv_output_dir):
    os.makedirs(VOLUME_SYNTHEA_CSV, exist_ok=True)
    # CSV output uses folder_per_run — find the timestamped subdirectory
    csv_subdirs = [d for d in os.listdir(csv_output_dir) if os.path.isdir(os.path.join(csv_output_dir, d))]
    csv_src = os.path.join(csv_output_dir, csv_subdirs[0]) if csv_subdirs else csv_output_dir
    csv_files = glob.glob(os.path.join(csv_src, "*.csv"))
    for cf in csv_files:
        shutil.copy2(cf, os.path.join(VOLUME_SYNTHEA_CSV, os.path.basename(cf)))
    print(f"Copied {len(csv_files)} CSV files to {VOLUME_SYNTHEA_CSV}")
    # Show observations file size (labs data)
    obs_file = os.path.join(VOLUME_SYNTHEA_CSV, "observations.csv")
    if os.path.exists(obs_file):
        obs_mb = os.path.getsize(obs_file) / (1024 * 1024)
        print(f"  observations.csv: {obs_mb:.1f} MB (lab results)")
else:
    print("No CSV output directory found — skipping CSV copy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Assign MBR IDs & Write Crosswalk
# MAGIC
# MAGIC Demographics were extracted during the copy step above. Now assign MBR IDs
# MAGIC and age-consistent LOB, and write the crosswalk Parquet.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print(f"Assigning MBR IDs to {len(patients_raw)} patients...")

# Assign MBR IDs and age-consistent LOB
import random as _rand
_rand.seed(SEED)

today = datetime.now().date()
crosswalk_rows = []

for idx, row in enumerate(patients_raw):
    member_id = f"MBR{100000 + idx}"

    # Compute age for LOB assignment
    dob_str = row["date_of_birth"]
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        age = (today - dob).days // 365
    except Exception:
        age = 40  # fallback

    # Age-consistent LOB assignment
    if age >= 65:
        lob = "Medicare Advantage"
    elif age < 18:
        lob = "Medicaid"
    else:
        lob = _rand.choices(
            ["Commercial", "Medicaid", "ACA Marketplace"],
            weights=[40, 15, 15],
            k=1,
        )[0]

    # Map Synthea gender to M/F
    gender_raw = row["gender"] or "unknown"
    gender = "M" if gender_raw.lower() == "male" else "F" if gender_raw.lower() == "female" else "U"

    crosswalk_rows.append({
        "synthea_uuid": row["synthea_uuid"],
        "member_id": member_id,
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "date_of_birth": dob_str,
        "gender": gender,
        "address_line_1": row["address_line_1"],
        "city": row["city"],
        "state": row["state"],
        "zip_code": row["zip_code"],
        "line_of_business": lob,
    })

# Write crosswalk Parquet
crosswalk_path = f"{volume_base}/synthea_demographics/crosswalk.parquet"
os.makedirs(f"{volume_base}/synthea_demographics", exist_ok=True)
df_crosswalk = spark.createDataFrame(crosswalk_rows)
df_crosswalk.write.mode("overwrite").parquet(crosswalk_path)

print(f"Wrote {len(crosswalk_rows)} rows to {crosswalk_path}")

# LOB distribution summary
from collections import Counter
lob_counts = Counter(r["line_of_business"] for r in crosswalk_rows)
print("LOB distribution:")
for lob_name, count in sorted(lob_counts.items(), key=lambda x: -x[1]):
    print(f"  {lob_name:25s} {count:,} ({100*count/len(crosswalk_rows):.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Summary Statistics

# COMMAND ----------

# Aggregate resource type counts across all bundles (sample first 100 for speed)
import random as _rand
_rand.seed(42)
sample_files = _rand.sample(patient_bundles, min(100, len(patient_bundles)))

all_resource_types = {}
condition_codes = set()
for bp in sample_files:
    with open(bp, "r") as f:
        bundle = json.load(f)
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        rt = resource.get("resourceType", "Unknown")
        all_resource_types[rt] = all_resource_types.get(rt, 0) + 1
        # Collect condition codes
        if rt == "Condition":
            for coding in resource.get("code", {}).get("coding", []):
                code = coding.get("code", "")
                display = coding.get("display", "")
                if code:
                    condition_codes.add(f"{code}: {display}")

print("=" * 60)
print("SYNTHEA GENERATION COMPLETE")
print("=" * 60)
print(f"Patient bundles generated: {len(patient_bundles)}")
print(f"Output location: {VOLUME_SYNTHEA_RAW}")
print(f"Crosswalk Parquet: {crosswalk_path}")
print(f"\nResource types (sampled from {len(sample_files)} bundles):")
for rt, count in sorted(all_resource_types.items(), key=lambda x: -x[1]):
    print(f"  {rt:30s} {count:,}")

print(f"\nUnique condition codes found (sample): {len(condition_codes)}")
# Show a few HLS-relevant conditions
hls_keywords = ["diabetes", "hypertension", "heart failure", "copd", "chronic kidney", "depression"]
relevant = [c for c in condition_codes if any(k in c.lower() for k in hls_keywords)]
if relevant:
    print("Key HLS conditions found:")
    for c in sorted(relevant)[:15]:
        print(f"  {c}")

# Lab data summary
obs_count = all_resource_types.get("Observation", 0)
diag_count = all_resource_types.get("DiagnosticReport", 0)
print(f"\nLab data (sampled from {len(sample_files)} bundles):")
print(f"  Observations (individual labs): {obs_count:,}")
print(f"  DiagnosticReports (grouped panels): {diag_count:,}")
if obs_count > 0:
    print(f"  Avg observations per patient: {obs_count / len(sample_files):.0f}")

print(f"\nNext step: run_data_generation reads crosswalk for member demographics")

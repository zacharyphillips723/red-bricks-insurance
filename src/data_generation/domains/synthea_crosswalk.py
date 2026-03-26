# Red Bricks Insurance — Synthea Crosswalk
#
# Maps Synthea-generated FHIR R4 patient bundles onto existing Red Bricks
# MBR IDs (MBR100000–MBR104999). Rewrites bundles with MBR IDs, names,
# and addresses, then writes to clinical/fhir_bundles/ for downstream
# parsing by dbignite (parse_fhir_with_dbignite.py).
#
# All heavy I/O is parallelized with ThreadPoolExecutor.

import json
import os
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Crosswalk builder
# ---------------------------------------------------------------------------

def _extract_synthea_patient_info(bundle: dict) -> Optional[dict]:
    """Extract patient demographics from a Synthea FHIR bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            birth_date = resource.get("birthDate", "1970-01-01")
            gender = resource.get("gender", "unknown")
            patient_id = resource.get("id", "")
            try:
                dob = datetime.strptime(birth_date, "%Y-%m-%d").date()
                age = (date(2025, 1, 1) - dob).days // 365
            except (ValueError, TypeError):
                age = 50
            return {
                "synthea_id": patient_id,
                "gender": gender,
                "age": age,
                "birth_date": birth_date,
            }
    return None


# ---------------------------------------------------------------------------
# Bundle rewriter
# ---------------------------------------------------------------------------

def rewrite_bundle(
    bundle: dict,
    crosswalk: Dict[str, str],
    member_lookup: Dict[str, Dict[str, Any]],
    provider_npis: List[str],
) -> Optional[dict]:
    """
    Rewrite a single Synthea FHIR bundle with Red Bricks MBR IDs.

    - Patient.id -> MBR ID
    - Patient.name -> overwrite with member's Faker name
    - Patient.address -> overwrite with member's NC address
    - All subject/patient references -> Patient/MBR ID
    - Encounter practitioner references -> random provider NPI
    """
    # Find the Patient resource and its Synthea ID
    patient_entry = None
    synthea_id = None
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            patient_entry = entry
            synthea_id = resource.get("id")
            break

    if not synthea_id or synthea_id not in crosswalk:
        return None

    mbr_id = crosswalk[synthea_id]
    member = member_lookup.get(mbr_id)
    if not member:
        return None

    # Rewrite Patient resource
    patient = patient_entry["resource"]
    patient["id"] = mbr_id
    patient["identifier"] = [{
        "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                             "code": "MB", "display": "Member Number"}]},
        "system": "urn:oid:2.16.840.1.113883.3.redbricks",
        "value": mbr_id,
    }]
    patient["name"] = [{
        "family": member.get("last_name", "Unknown"),
        "given": [member.get("first_name", "Unknown")],
    }]
    patient["address"] = [{
        "line": [member.get("address_line_1", "")],
        "city": member.get("city", ""),
        "state": member.get("state", "NC"),
        "postalCode": member.get("zip_code", ""),
        "district": member.get("county", ""),
    }]
    patient_entry["fullUrl"] = f"urn:uuid:{mbr_id}"
    patient_entry["request"] = {"method": "PUT", "url": f"Patient/{mbr_id}"}

    # Rewrite all other entries
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        rt = resource.get("resourceType")
        if rt == "Patient":
            continue

        # Rewrite subject/patient references
        if "subject" in resource and "reference" in resource["subject"]:
            resource["subject"]["reference"] = f"Patient/{mbr_id}"
        if "patient" in resource and "reference" in resource["patient"]:
            resource["patient"]["reference"] = f"Patient/{mbr_id}"

        # Rewrite encounter participant (practitioner) to our NPIs
        if rt == "Encounter" and provider_npis:
            npi = random.choice(provider_npis)
            resource["participant"] = [{
                "individual": {
                    "reference": f"Practitioner/{npi}",
                    "display": f"NPI: {npi}",
                }
            }]

        # Rewrite fullUrl and request URL
        if "fullUrl" in entry and synthea_id in entry["fullUrl"]:
            entry["fullUrl"] = entry["fullUrl"].replace(synthea_id, mbr_id)
        if "request" in entry and "url" in entry["request"]:
            entry["request"]["url"] = entry["request"]["url"].replace(synthea_id, mbr_id)

    return bundle


# ---------------------------------------------------------------------------
# Main orchestrator — called from the notebook
# ---------------------------------------------------------------------------

def run_crosswalk(
    members_data: List[Dict[str, Any]],
    synthea_fhir_dir: str,
    output_volume_base: str,
    provider_npis: List[str],
    max_workers: int = 16,
) -> Dict[str, Any]:
    """
    Full crosswalk pipeline (memory-efficient two-pass approach):
      Pass 1: Extract only Patient demographics (lightweight) to build crosswalk
      Pass 2: Batch-parallel rewrite — read bundles, rewrite with MBR IDs, write to fhir_bundles/

    Flat file extraction (encounters, labs, vitals) is handled downstream by
    dbignite in parse_fhir_with_dbignite.py.

    Args:
        members_data: list of member dicts (from members Parquet)
        synthea_fhir_dir: path to Synthea FHIR output (e.g., /Volumes/.../synthea_raw/fhir)
        output_volume_base: path to raw_sources volume (e.g., /Volumes/.../raw_sources)
        provider_npis: list of provider NPI strings
        max_workers: thread pool size

    Returns:
        Summary dict with counts
    """
    import time

    bundle_files = sorted([
        f for f in os.listdir(synthea_fhir_dir)
        if f.endswith(".json")
        and not f.startswith(("hospitalInformation", "practitionerInformation"))
    ])
    print(f"Found {len(bundle_files)} patient bundle files in {synthea_fhir_dir}")

    # --- Pass 1: Fast demographic extraction ---
    print(f"Pass 1: Extracting patient demographics ({max_workers} threads)...")
    t0 = time.time()

    def _fast_extract_demographics(filename: str) -> Optional[Tuple[str, dict]]:
        """Read bundle file and extract Patient demographics."""
        path = os.path.join(synthea_fhir_dir, filename)
        try:
            with open(path, "rb") as f:
                raw = f.read()
            bundle = json.loads(raw)
            info = _extract_synthea_patient_info(bundle)
            if info:
                return (filename, info)
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_fast_extract_demographics, bundle_files))
    patient_demographics = [r for r in results if r is not None]

    print(f"  Extracted demographics from {len(patient_demographics)} patients in {time.time() - t0:.0f}s")

    # --- Build crosswalk from demographics ---
    print("Building demographic crosswalk...")
    t1 = time.time()

    # Sort both populations by (gender, age) for demographic alignment
    member_sort_key = []
    for m in members_data:
        gender = m.get("gender", "M")
        try:
            dob = datetime.strptime(m.get("date_of_birth", "1970-01-01"), "%Y-%m-%d").date()
            age = (date(2025, 1, 1) - dob).days // 365
        except (ValueError, TypeError):
            age = 50
        gender_norm = "female" if gender == "F" else "male"
        member_sort_key.append((gender_norm, age, m["member_id"]))

    member_sort_key.sort(key=lambda x: (x[0], x[1]))
    synthea_info_list = [(fn, info) for fn, info in patient_demographics]
    synthea_info_list.sort(key=lambda x: (x[1]["gender"], x[1]["age"]))

    crosswalk = {}
    n = min(len(member_sort_key), len(synthea_info_list))
    for i in range(n):
        synthea_id = synthea_info_list[i][1]["synthea_id"]
        member_id = member_sort_key[i][2]
        crosswalk[synthea_id] = member_id

    print(f"  Mapped {len(crosswalk)} Synthea patients -> MBR IDs in {time.time() - t1:.1f}s")

    # Build synthea_id -> filename lookup for pre-filtering in Pass 2
    synthea_id_to_file = {}
    for fn, info in patient_demographics:
        sid = info.get("synthea_id")
        if sid:
            synthea_id_to_file[sid] = fn

    # Pre-filter: only process files whose patients are in the crosswalk
    files_to_process = sorted(set(
        synthea_id_to_file[sid] for sid in crosswalk if sid in synthea_id_to_file
    ))
    print(f"  Pre-filtered: {len(files_to_process)} files to process "
          f"(skipping {len(bundle_files) - len(files_to_process)} unmatched bundles)")

    # Build member lookup
    member_lookup = {m["member_id"]: m for m in members_data}

    # --- Pass 2: Batch-parallel rewrite (FHIR bundles only) ---
    print(f"Pass 2: Batch-parallel rewrite ({max_workers} threads, batches of 200)...")
    t2 = time.time()

    # Try to use orjson for faster JSON parsing
    try:
        import orjson as _json_mod
        _json_loads = _json_mod.loads
        _json_dumps = lambda obj: _json_mod.dumps(obj).decode("utf-8")
        print("  Using orjson for fast JSON parsing")
    except ImportError:
        _json_loads = json.loads
        _json_dumps = json.dumps
        print("  Using stdlib json (install orjson for ~3x faster parsing)")

    # Prepare output directory
    fhir_out = os.path.join(output_volume_base, "clinical", "fhir_bundles")
    os.makedirs(fhir_out, exist_ok=True)
    for f in os.listdir(fhir_out):
        os.remove(os.path.join(fhir_out, f))

    bundles_written = 0
    BATCH_SIZE = 200

    def _read_and_rewrite(filename: str) -> Optional[str]:
        """Read one bundle, rewrite with MBR IDs. Returns JSON string."""
        path = os.path.join(synthea_fhir_dir, filename)
        with open(path, "rb") as fin:
            bundle = _json_loads(fin.read())

        rewritten = rewrite_bundle(bundle, crosswalk, member_lookup, provider_npis)
        if rewritten is None:
            return None

        return _json_dumps(rewritten)

    # Process in batches
    for batch_start in range(0, len(files_to_process), BATCH_SIZE):
        batch = files_to_process[batch_start:batch_start + BATCH_SIZE]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(_read_and_rewrite, batch))

        # Write results sequentially (fast — just disk I/O)
        for result in results:
            if result is None:
                continue
            fhir_path = os.path.join(fhir_out, f"bundle_{bundles_written:05d}.json")
            with open(fhir_path, "w") as fout:
                fout.write(result)
            bundles_written += 1

        processed = min(batch_start + BATCH_SIZE, len(files_to_process))
        if processed % 500 == 0 or processed == len(files_to_process):
            elapsed = time.time() - t2
            rate = processed / elapsed if elapsed > 0 else 0
            print(f"  Processed {processed}/{len(files_to_process)} bundles ({rate:.0f}/s)")

    print(f"  Completed in {time.time() - t2:.0f}s")
    print(f"  Rewritten FHIR bundles: {bundles_written}")

    summary = {
        "crosswalk_size": len(crosswalk),
        "bundles_rewritten": bundles_written,
    }
    print(f"\nCrosswalk complete: {json.dumps(summary, indent=2)}")
    return summary

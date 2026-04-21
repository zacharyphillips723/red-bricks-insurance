# Red Bricks Insurance — network adequacy domain.
# Geocodes providers/members, enhances provider directory fields, enriches
# claims with in/out-of-network indicators, and loads CMS reference tables.

import csv
import math
import os
import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .. import reference_data
from ..helpers import random_date_between, weighted_choice

# ---------------------------------------------------------------------------
# Internal: load CSV reference data from ref_data/ directory
# ---------------------------------------------------------------------------

_REF_DIR = os.path.join(os.path.dirname(__file__), "..", "ref_data")


def _load_zip_centroids() -> Dict[str, Dict[str, Any]]:
    """Load NC ZIP centroids into {zip_code: {lat, lon, county, county_fips}}."""
    path = os.path.join(_REF_DIR, "nc_zip_centroids.csv")
    centroids: Dict[str, Dict[str, Any]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            centroids[row["zip_code"]] = {
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "county": row["county"],
                "county_fips": row["county_fips"],
            }
    return centroids


def _load_county_classification() -> List[Dict[str, Any]]:
    """Load NC county classification (FIPS, type, population, density)."""
    path = os.path.join(_REF_DIR, "nc_county_classification.csv")
    rows: List[Dict[str, Any]] = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "county_fips": row["county_fips"],
                "county_name": row["county_name"],
                "county_type": row["county_type"],
                "population": int(row["population"]),
                "density_per_sq_mi": float(row["density_per_sq_mi"]),
                "cbsa_code": row["cbsa_code"],
                "cbsa_name": row["cbsa_name"],
            })
    return rows


def _load_cms_time_distance() -> List[Dict[str, Any]]:
    """Load CMS HSD time/distance standards (specialty x county type)."""
    path = os.path.join(_REF_DIR, "cms_time_distance.csv")
    rows: List[Dict[str, Any]] = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "specialty_type": row["specialty_type"],
                "specialty_category": row["specialty_category"],
                "county_type": row["county_type"],
                "max_distance_miles": int(row["max_distance_miles"]),
                "max_time_minutes": int(row["max_time_minutes"]),
            })
    return rows


# ---------------------------------------------------------------------------
# Geocoding helpers
# ---------------------------------------------------------------------------

def _add_jitter(lat: float, lon: float, max_offset_miles: float = 2.0) -> Tuple[float, float]:
    """Add random offset to lat/lon (simulates address-level variation from ZIP centroid).
    1 degree latitude ≈ 69 miles; 1 degree longitude ≈ 55 miles at NC latitude (~35°N).
    """
    lat_offset = random.uniform(-max_offset_miles, max_offset_miles) / 69.0
    lon_offset = random.uniform(-max_offset_miles, max_offset_miles) / 55.0
    return round(lat + lat_offset, 6), round(lon + lon_offset, 6)


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in miles."""
    R = 3958.8  # Earth radius in miles
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Public: Reference table generators (for pipeline bronze layer)
# ---------------------------------------------------------------------------

def generate_county_classification() -> List[Dict[str, Any]]:
    """Return NC county classification records for writing to volume."""
    return _load_county_classification()


def generate_cms_standards() -> List[Dict[str, Any]]:
    """Return CMS HSD time/distance standards for writing to volume."""
    return _load_cms_time_distance()


# ---------------------------------------------------------------------------
# Public: Geocode providers
# ---------------------------------------------------------------------------

def geocode_providers(
    providers_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Geocode providers and add network adequacy enhancement fields.

    Returns a new list of dicts with:
    - All original provider fields
    - provider_latitude, provider_longitude
    - accepts_new_patients, telehealth_capable, panel_size, panel_capacity
    - appointment_wait_days, credentialing_status, languages_spoken
    - last_claims_date (None for ghost network candidates)
    """
    centroids = _load_zip_centroids()
    cred_names = [c[0] for c in reference_data.CREDENTIALING_STATUSES]
    cred_weights = [c[1] for c in reference_data.CREDENTIALING_STATUSES]

    results: List[Dict[str, Any]] = []
    for prov in providers_data:
        npi = prov.get("npi")
        if not npi or npi == "INVALID":
            continue

        zip_code = prov.get("zip_code", "")
        specialty = prov.get("specialty", "Internal Medicine")

        # Geocode from ZIP centroid + jitter
        centroid = centroids.get(zip_code)
        if centroid:
            lat, lon = _add_jitter(centroid["latitude"], centroid["longitude"])
            county_fips = centroid["county_fips"]
        else:
            # Fallback: random NC location (Raleigh area)
            lat, lon = _add_jitter(35.78, -78.64, max_offset_miles=15.0)
            county_fips = "37183"

        # Telehealth capability (specialty-dependent)
        th_rate = reference_data.TELEHEALTH_RATE_BY_SPECIALTY.get(specialty, 0.30)
        telehealth_capable = random.random() < th_rate

        # Panel size
        panel_range = reference_data.PANEL_SIZE_BY_SPECIALTY.get(specialty, (300, 1200))
        panel_capacity = random.randint(panel_range[0], panel_range[1]) if panel_range[1] > 0 else 0
        # Current panel is 40-100% of capacity
        panel_size = int(panel_capacity * random.uniform(0.40, 1.05)) if panel_capacity > 0 else 0

        # Accepts new patients: mostly yes, but correlated with panel fullness
        panel_pct = panel_size / panel_capacity if panel_capacity > 0 else 0
        accepts = random.random() > (panel_pct * 0.20)  # higher panel → slight chance of no

        # Appointment wait times
        wait_range = reference_data.WAIT_TIME_BY_SPECIALTY.get(specialty, (5, 30))
        wait_days = random.randint(wait_range[0], wait_range[1]) if wait_range[1] > 0 else 0

        # Credentialing
        cred_status = weighted_choice(cred_names, cred_weights)

        # Languages
        langs = ["English"]
        for lang, prob in reference_data.PROVIDER_LANGUAGES[1:]:
            if random.random() < prob:
                langs.append(lang)

        # Ghost network: ~8% chance of no recent claims (will be flagged later)
        if random.random() < 0.08:
            last_claims_date = None
        else:
            last_claims_date = random_date_between(
                date(2024, 6, 1), date(2025, 12, 31)
            ).isoformat()

        results.append({
            "npi": npi,
            "provider_name": prov.get("provider_name", ""),
            "specialty": specialty,
            "cms_specialty_type": reference_data.SPECIALTY_TO_CMS.get(specialty, "Primary Care"),
            "network_status": prov.get("network_status", "In-Network"),
            "county": prov.get("county", ""),
            "county_fips": county_fips,
            "zip_code": zip_code,
            "provider_latitude": lat,
            "provider_longitude": lon,
            "accepts_new_patients": accepts,
            "telehealth_capable": telehealth_capable,
            "panel_size": panel_size,
            "panel_capacity": panel_capacity,
            "appointment_wait_days": wait_days,
            "credentialing_status": cred_status,
            "languages_spoken": "|".join(langs),
            "last_claims_date": last_claims_date,
            "effective_date": prov.get("effective_date"),
            "termination_date": prov.get("termination_date"),
        })
    return results


# ---------------------------------------------------------------------------
# Public: Geocode members
# ---------------------------------------------------------------------------

def geocode_members(
    members_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Geocode members from ZIP centroid + random offset.

    Returns list of dicts with member_id, lat, lon, county_fips, zip_code.
    """
    centroids = _load_zip_centroids()
    results: List[Dict[str, Any]] = []

    for mem in members_data:
        zip_code = mem.get("zip_code", "")
        centroid = centroids.get(zip_code)
        if centroid:
            lat, lon = _add_jitter(centroid["latitude"], centroid["longitude"])
            county_fips = centroid["county_fips"]
        else:
            lat, lon = _add_jitter(35.78, -78.64, max_offset_miles=15.0)
            county_fips = "37183"

        results.append({
            "member_id": mem["member_id"],
            "member_latitude": lat,
            "member_longitude": lon,
            "county": mem.get("county", ""),
            "county_fips": county_fips,
            "zip_code": zip_code,
        })
    return results


# ---------------------------------------------------------------------------
# Public: Enrich claims with in/out-of-network indicators
# ---------------------------------------------------------------------------

def enrich_claims_network(
    medical_claims: List[Dict[str, Any]],
    providers_data: List[Dict[str, Any]],
    geocoded_providers: List[Dict[str, Any]],
    geocoded_members: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Add network_indicator, oon_cost_differential, and nearest in-network
    provider info to medical claims.

    Returns a list of enriched claim dicts (one per claim, subset of fields).
    """
    # Build lookup maps
    prov_status = {p["npi"]: p.get("network_status", "In-Network") for p in providers_data if p.get("npi")}
    prov_geo = {p["npi"]: (p["provider_latitude"], p["provider_longitude"], p["specialty"])
                for p in geocoded_providers}
    mem_geo = {m["member_id"]: (m["member_latitude"], m["member_longitude"])
               for m in geocoded_members}

    # Build in-network providers by CMS specialty for nearest-INN lookup
    inn_by_specialty: Dict[str, List[Tuple[str, float, float]]] = {}
    for gp in geocoded_providers:
        if gp["network_status"] == "In-Network":
            cms_spec = gp["cms_specialty_type"]
            inn_by_specialty.setdefault(cms_spec, []).append(
                (gp["npi"], gp["provider_latitude"], gp["provider_longitude"])
            )

    leakage_names = [r[0] for r in reference_data.LEAKAGE_REASONS]
    leakage_weights = [r[1] for r in reference_data.LEAKAGE_REASONS]

    results: List[Dict[str, Any]] = []
    for claim in medical_claims:
        claim_id = claim.get("claim_id")
        member_id = claim.get("member_id")
        rendering_npi = claim.get("rendering_provider_npi")
        paid_amount = claim.get("paid_amount", 0) or 0

        if not claim_id or not member_id or not rendering_npi or rendering_npi == "INVALID":
            continue

        status = prov_status.get(rendering_npi, "Unknown")
        is_oon = status == "Out-of-Network"

        # Calculate distance from member to rendering provider
        member_loc = mem_geo.get(member_id)
        prov_loc = prov_geo.get(rendering_npi)
        distance_mi = None
        if member_loc and prov_loc:
            distance_mi = round(_haversine_miles(
                member_loc[0], member_loc[1], prov_loc[0], prov_loc[1]
            ), 1)

        # For OON claims: find nearest in-network alternative and cost differential
        nearest_inn_npi = None
        nearest_inn_distance_mi = None
        oon_cost_differential = 0.0
        leakage_reason = None

        if is_oon and member_loc:
            specialty = prov_loc[2] if prov_loc else "Primary Care"
            cms_spec = reference_data.SPECIALTY_TO_CMS.get(specialty, "Primary Care")
            candidates = inn_by_specialty.get(cms_spec, [])

            if candidates:
                # Find nearest in-network provider (sample up to 20 for perf)
                sample = random.sample(candidates, min(20, len(candidates)))
                best_dist = float("inf")
                best_npi = None
                for c_npi, c_lat, c_lon in sample:
                    d = _haversine_miles(member_loc[0], member_loc[1], c_lat, c_lon)
                    if d < best_dist:
                        best_dist = d
                        best_npi = c_npi
                nearest_inn_npi = best_npi
                nearest_inn_distance_mi = round(best_dist, 1)

            # OON cost differential: 30-80% markup
            oon_cost_differential = round(paid_amount * random.uniform(0.30, 0.80), 2)
            leakage_reason = weighted_choice(leakage_names, leakage_weights)

        results.append({
            "claim_id": claim_id,
            "member_id": member_id,
            "rendering_provider_npi": rendering_npi,
            "network_indicator": "OON" if is_oon else "INN",
            "member_to_provider_distance_mi": distance_mi,
            "oon_cost_differential": oon_cost_differential,
            "nearest_inn_npi": nearest_inn_npi,
            "nearest_inn_distance_mi": nearest_inn_distance_mi,
            "leakage_reason": leakage_reason,
            "paid_amount": paid_amount,
            "service_date": claim.get("service_from_date"),
        })
    return results

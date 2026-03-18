# =============================================================================
# Red Bricks Insurance — Clinical Domain: Full Pipeline (Python / SDP)
# =============================================================================
# Python equivalent of the SQL bronze/silver/gold clinical pipeline.
# Uses Auto Loader (cloudFiles) for incremental JSON ingestion and the
# dlt decorator API for expectations and table definitions.
#
# Clinical data arrives as JSON (dbignite-ready format) — unlike the claims
# and pharmacy domains which use Parquet sources.
#
# ~2% of source records contain intentional data quality defects.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _source_volume() -> str:
    """Retrieve the configured source volume path from pipeline settings."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    return spark.conf.get("source_volume")


# =============================================================================
# BRONZE — Raw JSON ingestion via Auto Loader
# =============================================================================

@dlt.table(
    name="bronze_encounters",
    comment="Raw clinical encounter records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.",
)
def bronze_encounters():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{_source_volume()}/clinical/encounters/")
        .select(
            "encounter_id",
            "member_id",
            "provider_npi",
            "date_of_service",
            "encounter_type",
            "visit_type",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


@dlt.table(
    name="bronze_lab_results",
    comment="Raw clinical lab result records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.",
)
def bronze_lab_results():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{_source_volume()}/clinical/lab_results/")
        .select(
            "lab_result_id",
            "member_id",
            "lab_name",
            F.col("value").cast(DoubleType()).alias("value"),
            "unit",
            F.col("reference_range_low").cast(DoubleType()).alias("reference_range_low"),
            F.col("reference_range_high").cast(DoubleType()).alias("reference_range_high"),
            "collection_date",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


@dlt.table(
    name="bronze_vitals",
    comment="Raw clinical vitals records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.",
)
def bronze_vitals():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{_source_volume()}/clinical/vitals/")
        .select(
            "vital_id",
            "member_id",
            "vital_name",
            F.col("value").cast(DoubleType()).alias("value"),
            "measurement_date",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# =============================================================================
# SILVER — Cleansed & validated
# =============================================================================

@dlt.table(
    name="silver_encounters",
    comment="Cleansed clinical encounters with validated IDs, typed dates, and quality tracking on NPI/encounter_type/date formats.",
)
@dlt.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_npi", "provider_npi RLIKE '^[0-9]{10}$'")
@dlt.expect("valid_encounter_type", "encounter_type IN ('office','outpatient','inpatient','emergency','telehealth')")
@dlt.expect("valid_date", "date_of_service IS NOT NULL")
def silver_encounters():
    return (
        dlt.readStream("bronze_encounters")
        .select(
            "encounter_id",
            "member_id",
            "provider_npi",
            F.col("date_of_service").cast(DateType()).alias("date_of_service"),
            "encounter_type",
            "visit_type",
            "source_file",
            "ingestion_timestamp",
        )
    )


@dlt.table(
    name="silver_lab_results",
    comment="Cleansed clinical lab results with typed dates, abnormal flag, and quality tracking on value/date/range constraints.",
)
@dlt.expect_or_drop("valid_lab_id", "lab_result_id IS NOT NULL")
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_value", "value IS NOT NULL AND value >= 0")
@dlt.expect("valid_collection_date", "collection_date IS NOT NULL")
@dlt.expect("valid_reference_range", "reference_range_low <= reference_range_high")
def silver_lab_results():
    return (
        dlt.readStream("bronze_lab_results")
        .select(
            "lab_result_id",
            "member_id",
            "lab_name",
            "value",
            "unit",
            "reference_range_low",
            "reference_range_high",
            F.col("collection_date").cast(DateType()).alias("collection_date"),
            F.when(
                (F.col("value") < F.col("reference_range_low"))
                | (F.col("value") > F.col("reference_range_high")),
                F.lit(True),
            )
            .otherwise(F.lit(False))
            .alias("is_abnormal"),
            "source_file",
            "ingestion_timestamp",
        )
    )


@dlt.table(
    name="silver_vitals",
    comment="Cleansed clinical vitals with typed dates and quality tracking on value positivity and date validity.",
)
@dlt.expect_or_drop("valid_vital_id", "vital_id IS NOT NULL")
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_value", "value > 0")
@dlt.expect("valid_measurement_date", "measurement_date IS NOT NULL")
def silver_vitals():
    return (
        dlt.readStream("bronze_vitals")
        .select(
            "vital_id",
            "member_id",
            "vital_name",
            "value",
            F.col("measurement_date").cast(DateType()).alias("measurement_date"),
            "source_file",
            "ingestion_timestamp",
        )
    )


# =============================================================================
# GOLD — Business aggregations
# =============================================================================

@dlt.table(
    name="gold_encounter_summary",
    comment="Monthly encounter summary by encounter and visit type. Supports utilization analysis and capacity planning.",
)
def gold_encounter_summary():
    return (
        dlt.read("silver_encounters")
        .filter(F.col("date_of_service").isNotNull())
        .withColumn("service_month", F.date_trunc("month", F.col("date_of_service")))
        .groupBy("service_month", "encounter_type", "visit_type")
        .agg(
            F.count("*").alias("encounter_count"),
            F.countDistinct("member_id").alias("unique_members"),
            F.round(
                F.count("*") / F.countDistinct("member_id"), 2
            ).alias("encounters_per_member"),
        )
    )


@dlt.table(
    name="gold_lab_results_summary",
    comment="Lab result statistics by test name including abnormal rates. Supports population health monitoring and quality-of-care analysis.",
)
def gold_lab_results_summary():
    return (
        dlt.read("silver_lab_results")
        .groupBy("lab_name")
        .agg(
            F.count("*").alias("total_results"),
            F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias("abnormal_count"),
            F.round(
                F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)) / F.count("*"),
                4,
            ).alias("abnormal_rate"),
            F.round(F.avg("value"), 2).alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
        )
    )


@dlt.table(
    name="gold_vitals_summary",
    comment="Population-level vital sign statistics including median approximation. Supports wellness programs and risk stratification.",
)
def gold_vitals_summary():
    return (
        dlt.read("silver_vitals")
        .groupBy("vital_name")
        .agg(
            F.count("*").alias("measurement_count"),
            F.round(F.avg("value"), 2).alias("avg_value"),
            F.round(F.percentile_approx("value", 0.5), 2).alias("median_value"),
            F.round(F.stddev("value"), 2).alias("std_dev"),
        )
    )

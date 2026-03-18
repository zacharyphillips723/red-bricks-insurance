# =============================================================================
# Red Bricks Insurance — Claims Domain: Full SDP Pipeline (Python)
# =============================================================================
# Python equivalent of the SQL bronze/silver/gold claims pipeline.
# Uses the dlt module with @dlt.table() decorators and @dlt.expect_or_drop()
# for data quality enforcement. Approximately 2% of synthetic source rows
# contain intentional defects that expectations will catch and drop.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


# -- Configuration ------------------------------------------------------------

source_volume = spark.conf.get("source_volume")


# =============================================================================
# BRONZE LAYER — Raw Ingestion
# =============================================================================

@dlt.table(
    name="bronze_claims_medical",
    comment="Raw medical claims ingested from source parquet files. No cleansing applied.",
    table_properties={
        "quality": "bronze",
        "domain": "claims",
        "pipelines.autoOptimize.zOrderCols": "claim_id,member_id",
    },
)
def bronze_claims_medical():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{source_volume}/claims_medical/")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dlt.table(
    name="bronze_claims_pharmacy",
    comment="Raw pharmacy claims ingested from source parquet files. No cleansing applied.",
    table_properties={
        "quality": "bronze",
        "domain": "claims",
        "pipelines.autoOptimize.zOrderCols": "claim_id,member_id",
    },
)
def bronze_claims_pharmacy():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{source_volume}/claims_pharmacy/")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =============================================================================
# SILVER LAYER — Cleansed & Validated
# =============================================================================

@dlt.table(
    name="silver_claims_medical",
    comment=(
        "Cleansed medical claims with validated dates, NPI, procedure, "
        "and diagnosis codes. ~2% defective rows are dropped by expectations."
    ),
    table_properties={
        "quality": "silver",
        "domain": "claims",
    },
)
# Critical expectations — drop the row on violation
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_rendering_npi", "rendering_provider_npi RLIKE '^[0-9]{10}$'")
@dlt.expect_or_drop(
    "valid_service_date",
    "service_from_date IS NOT NULL AND CAST(service_from_date AS DATE) IS NOT NULL",
)
@dlt.expect_or_drop("valid_procedure_code", "procedure_code RLIKE '^[0-9]{5}$'")
@dlt.expect_or_drop("valid_diagnosis", "primary_diagnosis_code IS NOT NULL")
# Soft expectations — track but keep the row
@dlt.expect("valid_billed_amount", "billed_amount >= 0")
@dlt.expect("valid_allowed_amount", "allowed_amount >= 0")
@dlt.expect("valid_paid_amount", "paid_amount >= 0")
@dlt.expect("billed_gte_allowed", "billed_amount >= allowed_amount")
def silver_claims_medical():
    return (
        dlt.readStream("bronze_claims_medical")
        .withColumn("service_from_date", F.col("service_from_date").cast(DateType()))
        .withColumn("service_to_date", F.col("service_to_date").cast(DateType()))
        .withColumn("paid_date", F.col("paid_date").cast(DateType()))
        .withColumn("admission_date", F.col("admission_date").cast(DateType()))
        .withColumn("discharge_date", F.col("discharge_date").cast(DateType()))
        .withColumn(
            "service_year_month",
            F.date_trunc("month", F.col("service_from_date")),
        )
    )


@dlt.table(
    name="silver_claims_pharmacy",
    comment=(
        "Cleansed pharmacy claims with validated NPI, NDC, and fill dates. "
        "~2% defective rows are dropped by expectations."
    ),
    table_properties={
        "quality": "silver",
        "domain": "claims",
    },
)
# Critical expectations — drop the row on violation
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_prescriber_npi", "prescriber_npi RLIKE '^[0-9]{10}$'")
@dlt.expect_or_drop(
    "valid_fill_date",
    "fill_date IS NOT NULL AND CAST(fill_date AS DATE) IS NOT NULL",
)
@dlt.expect_or_drop("valid_ndc", "ndc RLIKE '^[0-9]{11}$'")
# Soft expectation — track but keep the row
@dlt.expect("valid_ingredient_cost", "ingredient_cost >= 0")
def silver_claims_pharmacy():
    return (
        dlt.readStream("bronze_claims_pharmacy")
        .withColumn("fill_date", F.col("fill_date").cast(DateType()))
        .withColumn("paid_date", F.col("paid_date").cast(DateType()))
        .withColumn(
            "fill_year_month",
            F.date_trunc("month", F.col("fill_date")),
        )
    )


# =============================================================================
# GOLD LAYER — Business Aggregates
# =============================================================================

@dlt.table(
    name="gold_claims_summary",
    comment=(
        "Monthly medical claims summary by claim type and status. "
        "Includes denial rate and average paid per claim for executive dashboards."
    ),
    table_properties={
        "quality": "gold",
        "domain": "claims",
    },
)
def gold_claims_summary():
    df = dlt.read("silver_claims_medical")
    return (
        df.groupBy("claim_type", "claim_status", "service_year_month")
        .agg(
            F.countDistinct("claim_id").alias("total_claims"),
            F.count("*").alias("total_claim_lines"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("allowed_amount").alias("total_allowed"),
            F.sum("paid_amount").alias("total_paid"),
            F.sum("member_responsibility").alias("total_member_responsibility"),
            F.round(
                F.sum("paid_amount") / F.countDistinct("claim_id"), 2
            ).alias("avg_paid_per_claim"),
            F.round(
                F.sum(F.when(F.col("claim_status") == "denied", 1).otherwise(0))
                / F.countDistinct("claim_id"),
                4,
            ).alias("denial_rate"),
        )
    )


@dlt.table(
    name="gold_pharmacy_summary",
    comment=(
        "Monthly pharmacy claims summary by therapeutic class and formulary tier. "
        "Tracks specialty fill percentage and average cost per fill."
    ),
    table_properties={
        "quality": "gold",
        "domain": "claims",
    },
)
def gold_pharmacy_summary():
    df = dlt.read("silver_claims_pharmacy")
    return (
        df.groupBy("therapeutic_class", "formulary_tier", "fill_year_month")
        .agg(
            F.count("*").alias("total_fills"),
            F.sum("total_cost").alias("total_cost"),
            F.round(F.sum("total_cost") / F.count("*"), 2).alias("avg_cost_per_fill"),
            F.round(
                F.sum(F.when(F.col("is_specialty") == True, 1).otherwise(0))
                / F.count("*"),
                4,
            ).alias("specialty_fill_pct"),
            F.round(
                F.sum(F.when(F.col("formulary_tier") == "generic", 1).otherwise(0))
                / F.count("*"),
                4,
            ).alias("generic_fill_rate"),
        )
        .withColumnRenamed("fill_year_month", "service_year_month")
    )

# =============================================================================
# Red Bricks Insurance — Underwriting Domain: Python SDP Pipeline
# =============================================================================
# Python equivalent of the SQL underwriting pipeline (bronze → silver → gold).
# Uses Spark Declarative Pipelines (SDP) with cloudFiles for incremental
# ingestion and streaming tables for silver-layer cleansing.
# ~2% of source data contains intentional quality defects for demo purposes.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# -----------------------------------------------------------------------------
# Bronze: Raw ingestion from parquet with source lineage metadata
# -----------------------------------------------------------------------------

@dlt.table(
    name="bronze_underwriting",
    comment="Raw underwriting data ingested from parquet source files. Contains source lineage metadata columns.",
)
def bronze_underwriting():
    source_volume = spark.conf.get("source_volume")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{source_volume}/underwriting/")
        .select(
            "member_id",
            "risk_tier",
            "smoker_indicator",
            "bmi_band",
            "occupation_class",
            "medical_history_indicator",
            "underwriting_effective_date",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# -----------------------------------------------------------------------------
# Silver: Cleansed and enriched underwriting data
# -----------------------------------------------------------------------------

@dlt.table(
    name="silver_underwriting",
    comment="Cleansed underwriting data with validated fields, cast dates, and computed risk_factor_count.",
)
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_risk_tier", "risk_tier IN ('Standard', 'Preferred', 'Substandard')")
@dlt.expect("valid_smoker", "smoker_indicator IN ('Y', 'N')")
@dlt.expect("valid_bmi_band", "bmi_band IN ('underweight', 'normal', 'overweight', 'obese')")
@dlt.expect("valid_effective_date", "TRY_CAST(underwriting_effective_date AS DATE) IS NOT NULL")
def silver_underwriting():
    return (
        dlt.readStream("bronze_underwriting")
        .withColumn(
            "underwriting_effective_date",
            F.col("underwriting_effective_date").cast("date"),
        )
        .withColumn(
            "risk_factor_count",
            (
                F.when(F.col("smoker_indicator") == "Y", F.lit(1)).otherwise(F.lit(0))
                + F.when(F.col("bmi_band") == "obese", F.lit(1)).otherwise(F.lit(0))
                + F.when(
                    F.col("occupation_class").isin("Heavy", "Hazardous"), F.lit(1)
                ).otherwise(F.lit(0))
                + F.when(F.col("medical_history_indicator") == True, F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).cast(IntegerType()),
        )
    )


# -----------------------------------------------------------------------------
# Gold: Underwriting population summary
# -----------------------------------------------------------------------------

@dlt.table(
    name="gold_underwriting_summary",
    comment="Underwriting population summary by risk tier, smoker status, and BMI band. Includes member counts, average risk factors, and medical history prevalence.",
)
def gold_underwriting_summary():
    return (
        dlt.read("silver_underwriting")
        .groupBy("risk_tier", "smoker_indicator", "bmi_band")
        .agg(
            F.count("*").alias("member_count"),
            F.round(F.avg("risk_factor_count"), 2).alias("avg_risk_factor_count"),
            F.round(
                F.sum(
                    F.when(F.col("medical_history_indicator") == True, F.lit(1)).otherwise(
                        F.lit(0)
                    )
                )
                * 100.0
                / F.count("*"),
                2,
            ).alias("pct_with_medical_history"),
        )
    )

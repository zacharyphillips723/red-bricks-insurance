# =============================================================================
# Red Bricks Insurance — Risk Adjustment Domain: Python SDP Pipeline
# =============================================================================
# Python equivalent of the SQL risk adjustment pipeline (bronze → silver → gold).
# Covers both member-level and provider-level risk adjustment tables.
# Uses Spark Declarative Pipelines (SDP) with cloudFiles for incremental
# ingestion and streaming tables for silver-layer cleansing.
# ~2% of source data contains intentional quality defects for demo purposes.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

# =============================================================================
# BRONZE LAYER
# =============================================================================

# -----------------------------------------------------------------------------
# Bronze: Member-level risk adjustment scores and HCC codes
# -----------------------------------------------------------------------------

@dlt.table(
    name="bronze_risk_adjustment_member",
    comment="Raw member-level risk adjustment data including RAF scores and HCC codes, ingested from parquet.",
)
def bronze_risk_adjustment_member():
    source_volume = spark.conf.get("source_volume")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{source_volume}/risk_adjustment_member/")
        .select(
            "member_id",
            "model_year",
            "raf_score",
            "hcc_codes",
            "measurement_period_start",
            "measurement_period_end",
            "measurement_date",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# -----------------------------------------------------------------------------
# Bronze: Provider-level risk adjustment attribution
# -----------------------------------------------------------------------------

@dlt.table(
    name="bronze_risk_adjustment_provider",
    comment="Raw provider-level risk adjustment attribution data ingested from parquet.",
)
def bronze_risk_adjustment_provider():
    source_volume = spark.conf.get("source_volume")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{source_volume}/risk_adjustment_provider/")
        .select(
            "provider_npi",
            "member_id",
            "raf_score",
            "attribution_date",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# =============================================================================
# SILVER LAYER
# =============================================================================

# -----------------------------------------------------------------------------
# Silver: Member-level risk adjustment with derived metrics
# -----------------------------------------------------------------------------

@dlt.table(
    name="silver_risk_adjustment_member",
    comment="Cleansed member-level risk adjustment data with derived HCC count and high-risk flag.",
)
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_raf_score", "raf_score >= 0 AND raf_score <= 10")
@dlt.expect("valid_model_year", "model_year >= 2020 AND model_year <= 2030")
@dlt.expect("valid_measurement_date", "TRY_CAST(measurement_date AS DATE) IS NOT NULL")
def silver_risk_adjustment_member():
    return (
        dlt.readStream("bronze_risk_adjustment_member")
        .withColumn("measurement_period_start", F.col("measurement_period_start").cast("date"))
        .withColumn("measurement_period_end", F.col("measurement_period_end").cast("date"))
        .withColumn("measurement_date", F.col("measurement_date").cast("date"))
        .withColumn(
            "hcc_count",
            F.when(
                (F.col("hcc_codes").isNotNull()) & (F.trim(F.col("hcc_codes")) != ""),
                F.size(F.split(F.col("hcc_codes"), ",")),
            )
            .otherwise(F.lit(0))
            .cast(IntegerType()),
        )
        .withColumn(
            "is_high_risk",
            F.when(F.col("raf_score") > 2.0, F.lit(True))
            .otherwise(F.lit(False))
            .cast(BooleanType()),
        )
    )


# -----------------------------------------------------------------------------
# Silver: Provider-level risk adjustment attribution
# -----------------------------------------------------------------------------

@dlt.table(
    name="silver_risk_adjustment_provider",
    comment="Cleansed provider-level risk adjustment attribution with validated NPI and cast dates.",
)
@dlt.expect_or_drop("valid_npi", "provider_npi IS NOT NULL AND provider_npi RLIKE '^[0-9]{10}$'")
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_raf_score", "raf_score >= 0 AND raf_score <= 10")
@dlt.expect("valid_attribution_date", "TRY_CAST(attribution_date AS DATE) IS NOT NULL")
def silver_risk_adjustment_provider():
    return (
        dlt.readStream("bronze_risk_adjustment_provider")
        .withColumn("attribution_date", F.col("attribution_date").cast("date"))
    )


# =============================================================================
# GOLD LAYER
# =============================================================================

# -----------------------------------------------------------------------------
# Gold: Population-level risk score distribution by model year
# -----------------------------------------------------------------------------

@dlt.table(
    name="gold_risk_scores",
    comment="Annual risk adjustment summary including average/median RAF scores, high-risk prevalence, and HCC burden by model year.",
)
def gold_risk_scores():
    return (
        dlt.read("silver_risk_adjustment_member")
        .groupBy("model_year")
        .agg(
            F.count("*").alias("total_members"),
            F.round(F.avg("raf_score"), 4).alias("avg_raf_score"),
            F.round(F.percentile_approx("raf_score", 0.5), 4).alias("median_raf_score"),
            F.sum(
                F.when(F.col("is_high_risk") == True, F.lit(1)).otherwise(F.lit(0))
            ).alias("high_risk_member_count"),
            F.round(
                F.sum(
                    F.when(F.col("is_high_risk") == True, F.lit(1)).otherwise(F.lit(0))
                )
                * 100.0
                / F.count("*"),
                2,
            ).alias("high_risk_pct"),
            F.round(F.avg("hcc_count"), 2).alias("avg_hcc_count"),
        )
    )


# -----------------------------------------------------------------------------
# Gold: Provider risk profile with attributed member metrics
# -----------------------------------------------------------------------------

@dlt.table(
    name="gold_provider_risk_profile",
    comment="Provider risk profile showing attributed member counts, average RAF scores, and high-risk member prevalence per provider NPI.",
)
def gold_provider_risk_profile():
    providers = dlt.read("silver_risk_adjustment_provider")
    members = dlt.read("silver_risk_adjustment_member")

    return (
        providers.alias("p")
        .join(members.alias("m"), F.col("p.member_id") == F.col("m.member_id"), "left")
        .groupBy("p.provider_npi")
        .agg(
            F.countDistinct("p.member_id").alias("attributed_member_count"),
            F.round(F.avg("p.raf_score"), 4).alias("avg_raf_score"),
            F.sum(
                F.when(F.col("m.is_high_risk") == True, F.lit(1)).otherwise(F.lit(0))
            ).alias("high_risk_member_count"),
            F.round(
                F.sum(
                    F.when(F.col("m.is_high_risk") == True, F.lit(1)).otherwise(F.lit(0))
                )
                * 100.0
                / F.countDistinct("p.member_id"),
                2,
            ).alias("high_risk_pct"),
        )
    )

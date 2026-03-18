# =============================================================================
# Red Bricks Insurance — Providers Domain Pipeline (Python)
# =============================================================================
# Python equivalent of the providers SQL pipeline using the dlt module.
# Bronze: Auto Loader ingestion from parquet.
# Silver: Cleansed with data quality expectations (~2% intentional defects).
# Gold: Network adequacy aggregations for analytics.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# -- Source volume path (configured via pipeline settings) --------------------
SOURCE_VOLUME = spark.conf.get("source_volume")

# =============================================================================
# BRONZE LAYER
# =============================================================================


@dlt.table(
    name="bronze_providers",
    comment="Raw provider directory records ingested from parquet source files. No transformations applied.",
)
def bronze_providers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{SOURCE_VOLUME}/providers/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =============================================================================
# SILVER LAYER
# =============================================================================


@dlt.table(
    name="silver_providers",
    comment="Cleansed provider directory with validated NPI, specialty, and network status. Invalid NPI rows are dropped; other violations are tracked.",
)
@dlt.expect_or_drop("valid_npi_not_null", "npi IS NOT NULL")
@dlt.expect_or_drop("valid_npi_format", "npi RLIKE '^[0-9]{10}$'")
@dlt.expect("valid_specialty", "specialty IS NOT NULL")
@dlt.expect("valid_network_status", "network_status IN ('In-Network', 'Out-of-Network')")
def silver_providers():
    return (
        dlt.readStream("bronze_providers")
        .withColumn("effective_date", F.col("effective_date").cast(DateType()))
        .withColumn("termination_date", F.col("termination_date").cast(DateType()))
        .withColumn(
            "is_active",
            F.when(
                F.col("termination_date").isNull() | (F.col("termination_date") >= F.current_date()),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .select(
            "npi",
            "provider_first_name",
            "provider_last_name",
            "provider_name",
            "credential",
            "specialty",
            "taxonomy_code",
            "tax_id",
            "group_name",
            "network_status",
            "effective_date",
            "termination_date",
            "address_line_1",
            "city",
            "state",
            "zip_code",
            "county",
            "phone",
            "is_active",
            "source_file",
            "ingestion_timestamp",
        )
    )


# =============================================================================
# GOLD LAYER
# =============================================================================


@dlt.table(
    name="gold_provider_directory",
    comment="Provider network summary by specialty, network status, county, and active flag. Includes average providers per group for network adequacy analysis.",
)
def gold_provider_directory():
    providers = dlt.read("silver_providers")

    return (
        providers.groupBy("specialty", "network_status", "county", "is_active")
        .agg(
            F.countDistinct("npi").alias("provider_count"),
            F.countDistinct("group_name").alias("group_count"),
        )
        .withColumn(
            "avg_providers_per_group",
            F.round(F.col("provider_count") / F.col("group_count"), 2),
        )
    )

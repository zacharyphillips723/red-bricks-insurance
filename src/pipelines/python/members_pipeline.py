# =============================================================================
# Red Bricks Insurance — Members Domain Pipeline (Python)
# =============================================================================
# Python equivalent of the members SQL pipeline using the dlt module.
# Bronze: Auto Loader ingestion from parquet.
# Silver: Cleansed with data quality expectations (~2% intentional defects).
# Gold: Business-level aggregations for analytics.
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
    name="bronze_members",
    comment="Raw member demographics ingested from parquet source files. No transformations applied.",
)
def bronze_members():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{SOURCE_VOLUME}/members/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dlt.table(
    name="bronze_enrollment",
    comment="Raw enrollment/eligibility records ingested from parquet source files. No transformations applied.",
)
def bronze_enrollment():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{SOURCE_VOLUME}/enrollment/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =============================================================================
# SILVER LAYER
# =============================================================================


@dlt.table(
    name="silver_members",
    comment="Cleansed member demographics with validated IDs, dates, and gender codes. Critical constraint failures are dropped; soft failures are tracked.",
)
@dlt.expect_or_drop("valid_member_id_not_null", "member_id IS NOT NULL")
@dlt.expect_or_drop("valid_member_id_format", "member_id RLIKE '^MBR[0-9]+$'")
@dlt.expect("valid_dob_castable", "TRY_CAST(date_of_birth AS DATE) IS NOT NULL")
@dlt.expect("valid_dob_not_future", "TRY_CAST(date_of_birth AS DATE) <= current_date()")
@dlt.expect("valid_gender", "gender IN ('M', 'F')")
def silver_members():
    return (
        dlt.readStream("bronze_members")
        .select(
            "member_id",
            "last_name",
            "first_name",
            F.concat_ws(", ", F.col("last_name"), F.col("first_name")).alias("full_name"),
            F.col("date_of_birth").cast(DateType()).alias("date_of_birth"),
            "gender",
            "ssn_last_4",
            "address_line_1",
            "city",
            "state",
            "zip_code",
            "county",
            "phone",
            "email",
            "source_file",
            "ingestion_timestamp",
        )
    )


@dlt.table(
    name="silver_enrollment",
    comment="Cleansed enrollment records with validated LOB, dates, premiums, and risk scores. Null member_id rows are dropped; other violations are tracked.",
)
@dlt.expect_or_drop("valid_member_id", "member_id IS NOT NULL")
@dlt.expect("valid_line_of_business", "line_of_business IN ('Commercial', 'Medicare Advantage', 'Medicaid', 'ACA Marketplace')")
@dlt.expect("valid_start_date", "TRY_CAST(eligibility_start_date AS DATE) IS NOT NULL")
@dlt.expect("valid_premium", "monthly_premium > 0")
@dlt.expect("valid_risk_score", "risk_score >= 0 AND risk_score <= 5.0")
def silver_enrollment():
    return (
        dlt.readStream("bronze_enrollment")
        .withColumn("eligibility_start_date", F.col("eligibility_start_date").cast(DateType()))
        .withColumn("eligibility_end_date", F.col("eligibility_end_date").cast(DateType()))
        .withColumn(
            "coverage_months",
            F.coalesce(
                F.months_between(F.col("eligibility_end_date"), F.col("eligibility_start_date")).cast("int"),
                F.lit(12),
            ),
        )
        .withColumn(
            "is_active",
            F.when(
                F.col("eligibility_end_date").isNull() | (F.col("eligibility_end_date") >= F.current_date()),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .select(
            "member_id",
            "subscriber_id",
            "relationship",
            "line_of_business",
            "plan_type",
            "plan_id",
            "group_number",
            "group_name",
            "eligibility_start_date",
            "eligibility_end_date",
            "monthly_premium",
            "rating_area",
            "risk_score",
            "metal_level",
            "coverage_months",
            "is_active",
            "source_file",
            "ingestion_timestamp",
        )
    )


# =============================================================================
# GOLD LAYER
# =============================================================================


@dlt.table(
    name="gold_member_demographics",
    comment="Member population counts segmented by county, gender, age band, and line of business. Refreshes automatically as silver tables update.",
)
def gold_member_demographics():
    members = dlt.read("silver_members")
    enrollment = dlt.read("silver_enrollment")

    joined = members.join(enrollment, on="member_id", how="inner")

    return (
        joined.withColumn(
            "age",
            F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25),
        )
        .withColumn(
            "age_band",
            F.when(F.col("age") < 18, F.lit("0-17"))
            .when(F.col("age") < 35, F.lit("18-34"))
            .when(F.col("age") < 50, F.lit("35-49"))
            .when(F.col("age") < 65, F.lit("50-64"))
            .otherwise(F.lit("65+")),
        )
        .groupBy("county", "gender", "age_band", "line_of_business")
        .agg(F.countDistinct("member_id").alias("member_count"))
    )


@dlt.table(
    name="gold_enrollment_summary",
    comment="Enrollment KPIs by line of business and plan type: active counts, average premium, average risk score, and churn rate. Refreshes automatically.",
)
def gold_enrollment_summary():
    enrollment = dlt.read("silver_enrollment")

    return (
        enrollment.groupBy("line_of_business", "plan_type")
        .agg(
            F.countDistinct(F.when(F.col("is_active"), F.col("member_id"))).alias("active_member_count"),
            F.countDistinct("member_id").alias("total_member_count"),
            F.round(F.avg("monthly_premium"), 2).alias("avg_premium"),
            F.round(F.avg("risk_score"), 3).alias("avg_risk_score"),
        )
        .withColumn(
            "churn_rate_pct",
            F.round(
                (F.col("total_member_count") - F.col("active_member_count")) * 100.0 / F.col("total_member_count"),
                2,
            ),
        )
    )

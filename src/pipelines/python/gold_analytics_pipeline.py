# =============================================================================
# Red Bricks Insurance — Gold Analytics Pipeline (Python / SDP)
# =============================================================================
# Cross-domain gold analytics: financial metrics, quality measures, risk
# adjustment research, and AI-powered classification tables.
#
# This pipeline runs SEPARATELY from domain pipelines. All source references
# use fully-qualified catalog.schema.table names (not LIVE).
#
# Pipeline: gold_analytics
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as _sum,
    avg,
    min as _min,
    max as _max,
    year,
    datediff,
    current_date,
    floor,
    lit,
    when,
    concat,
    concat_ws,
    expr,
    row_number,
    trim,
    explode,
    split,
)
from pyspark.sql.window import Window


# -----------------------------------------------------------------------------
# Helper: read a published table from the catalog
# -----------------------------------------------------------------------------
def _read_table(spark, table_name: str):
    """Read a published silver/gold table using fully-qualified name."""
    catalog = spark.conf.get("catalog")
    schema = spark.conf.get("schema")
    return spark.read.table(f"{catalog}.{schema}.{table_name}")


# =============================================================================
# FINANCIAL METRICS
# =============================================================================


@dlt.table(
    name="gold_pmpm",
    comment=(
        "Per Member Per Month (PMPM) paid and allowed amounts by line of "
        "business and service month. Key metric for actuarial trend analysis."
    ),
)
def gold_pmpm():
    claims = _read_table(spark, "silver_claims_medical")
    enrollment = _read_table(spark, "silver_enrollment")

    joined = claims.join(enrollment, on="member_id", how="inner")

    return (
        joined.groupBy("line_of_business", "service_year_month")
        .agg(
            _sum("paid_amount").alias("total_paid"),
            _sum("allowed_amount").alias("total_allowed"),
            countDistinct("member_id").alias("member_months"),
        )
        .withColumn(
            "pmpm_paid",
            col("total_paid") / when(col("member_months") == 0, lit(None)).otherwise(col("member_months")),
        )
        .withColumn(
            "pmpm_allowed",
            col("total_allowed") / when(col("member_months") == 0, lit(None)).otherwise(col("member_months")),
        )
    )


@dlt.table(
    name="gold_mlr",
    comment=(
        "Medical Loss Ratio (MLR) by line of business and service year. "
        "Combines medical and pharmacy claims against premium revenue."
    ),
)
def gold_mlr():
    medical = _read_table(spark, "silver_claims_medical")
    pharmacy = _read_table(spark, "silver_claims_pharmacy")
    enrollment = _read_table(spark, "silver_enrollment")

    # Medical claims by LOB and year
    medical_agg = (
        medical.join(enrollment, on="member_id", how="inner")
        .withColumn("service_year", year("service_from_date"))
        .groupBy("line_of_business", "service_year")
        .agg(_sum("paid_amount").alias("medical_claims_paid"))
    )

    # Pharmacy claims by LOB and year
    pharmacy_agg = (
        pharmacy.join(enrollment, on="member_id", how="inner")
        .withColumn("service_year", year("fill_date"))
        .groupBy("line_of_business", "service_year")
        .agg(_sum("plan_paid").alias("pharmacy_claims_paid"))
    )

    # Premiums by LOB and year
    premiums = (
        enrollment.withColumn("service_year", year("eligibility_start_date"))
        .groupBy("line_of_business", "service_year")
        .agg(
            _sum(col("monthly_premium") * col("coverage_months")).alias("total_premiums")
        )
    )

    # Combine
    combined = (
        medical_agg.join(
            pharmacy_agg,
            on=["line_of_business", "service_year"],
            how="full",
        )
        .join(premiums, on=["line_of_business", "service_year"], how="left")
        .fillna(0, subset=["medical_claims_paid", "pharmacy_claims_paid"])
        .withColumn(
            "total_claims_paid",
            col("medical_claims_paid") + col("pharmacy_claims_paid"),
        )
        .withColumn(
            "mlr",
            col("total_claims_paid")
            / when(col("total_premiums") == 0, lit(None)).otherwise(col("total_premiums")),
        )
        .withColumn(
            "target_mlr",
            when(
                col("line_of_business").isin("Medicare Advantage", "Medicaid"),
                lit(0.85),
            ).otherwise(lit(0.80)),
        )
    )

    return combined


@dlt.table(
    name="gold_ibnr_estimate",
    comment=(
        "Incurred But Not Reported (IBNR) exposure analysis by service month. "
        "Shows payment lag distribution and completion factors for reserve estimation."
    ),
)
def gold_ibnr_estimate():
    claims = _read_table(spark, "silver_claims_medical").filter(
        col("paid_date").isNotNull() & col("service_from_date").isNotNull()
    )

    claims_with_lag = claims.withColumn(
        "lag_days", datediff("paid_date", "service_from_date")
    )

    return claims_with_lag.groupBy("service_year_month").agg(
        count("*").alias("total_claims"),
        avg("lag_days").alias("avg_lag_days"),
        _sum(when(col("lag_days") < 30, 1).otherwise(0)).alias("claims_under_30_days"),
        _sum(when((col("lag_days") >= 30) & (col("lag_days") < 90), 1).otherwise(0)).alias("claims_30_to_90"),
        _sum(when((col("lag_days") >= 90) & (col("lag_days") < 180), 1).otherwise(0)).alias("claims_90_to_180"),
        _sum(when(col("lag_days") >= 180, 1).otherwise(0)).alias("claims_over_180"),
        (
            _sum(when(col("lag_days") >= 90, 1).otherwise(0)).cast("double")
            / when(count("*") == 0, lit(None)).otherwise(count("*"))
        ).alias("pct_over_90"),
        (
            _sum(when(col("lag_days") < 90, 1).otherwise(0)).cast("double")
            / when(count("*") == 0, lit(None)).otherwise(count("*"))
        ).alias("completion_factor"),
    )


# =============================================================================
# QUALITY METRICS
# =============================================================================
# NOTE: These are simplified HEDIS proxies for demo purposes. Production HEDIS
# requires certified measure engines with full specification logic.
# =============================================================================


@dlt.table(
    name="gold_hedis_member",
    comment=(
        "Simplified HEDIS-like quality measures per member in long format. "
        "Includes diabetes care, cancer screenings, and preventive visits. "
        "These are proxy measures for demo purposes."
    ),
)
def gold_hedis_member():
    claims = _read_table(spark, "silver_claims_medical")
    enrollment = _read_table(spark, "silver_enrollment")
    members = _read_table(spark, "silver_members")
    labs = _read_table(spark, "silver_lab_results")

    # --- Diabetes Care: HbA1c Testing ---
    diabetic_members = (
        claims.filter(col("primary_diagnosis_code").startswith("E11"))
        .select("member_id", year("service_from_date").alias("measurement_year"))
        .distinct()
    )

    hba1c_tested = (
        labs.filter(col("lab_name") == "HbA1c")
        .select(col("member_id"), year("collection_date").alias("measurement_year"))
        .distinct()
    )

    diabetes_care = (
        diabetic_members.join(enrollment.select("member_id", "line_of_business"), on="member_id")
        .join(hba1c_tested, on=["member_id", "measurement_year"], how="left")
        .withColumn("is_compliant", when(hba1c_tested["member_id"].isNotNull(), lit(1)).otherwise(lit(0)))
        .select("member_id", "line_of_business", "measurement_year", "is_compliant")
        .withColumn("measure_name", lit("Diabetes Care - HbA1c Testing"))
        .distinct()
    )

    # Helper: member age
    members_with_age = members.withColumn(
        "age", floor(datediff(current_date(), col("date_of_birth")) / 365.25)
    )

    # --- Breast Cancer Screening ---
    eligible_bcs = (
        members_with_age.filter((col("gender") == "Female") & col("age").between(50, 74))
        .select("member_id")
        .join(enrollment.select("member_id", "line_of_business"), on="member_id")
    )

    mammo_members = (
        claims.filter(col("procedure_code").isin("77067", "77066", "77065"))
        .select("member_id")
        .distinct()
    )

    bcs = (
        eligible_bcs.join(mammo_members, on="member_id", how="left")
        .withColumn("is_compliant", when(mammo_members["member_id"].isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("measure_name", lit("Breast Cancer Screening"))
        .withColumn("measurement_year", year(current_date()))
        .select("member_id", "line_of_business", "measurement_year", "is_compliant", "measure_name")
        .distinct()
    )

    # --- Colorectal Cancer Screening ---
    eligible_crc = (
        members_with_age.filter(col("age").between(45, 75))
        .select("member_id")
        .join(enrollment.select("member_id", "line_of_business"), on="member_id")
    )

    colonoscopy_members = (
        claims.filter(col("procedure_code") == "45380")
        .select("member_id")
        .distinct()
    )

    crc = (
        eligible_crc.join(colonoscopy_members, on="member_id", how="left")
        .withColumn("is_compliant", when(colonoscopy_members["member_id"].isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("measure_name", lit("Colorectal Cancer Screening"))
        .withColumn("measurement_year", year(current_date()))
        .select("member_id", "line_of_business", "measurement_year", "is_compliant", "measure_name")
        .distinct()
    )

    # --- Preventive Visit ---
    preventive_members = (
        claims.filter(col("procedure_code").isin("99395", "99396"))
        .select("member_id")
        .distinct()
    )

    prev = (
        enrollment.select("member_id", "line_of_business")
        .join(preventive_members, on="member_id", how="left")
        .withColumn("is_compliant", when(preventive_members["member_id"].isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("measure_name", lit("Preventive Visit"))
        .withColumn("measurement_year", year(current_date()))
        .select("member_id", "line_of_business", "measurement_year", "is_compliant", "measure_name")
        .distinct()
    )

    # Union all measures
    return (
        diabetes_care.select("member_id", "line_of_business", "measure_name", "is_compliant", "measurement_year")
        .unionByName(bcs.select("member_id", "line_of_business", "measure_name", "is_compliant", "measurement_year"))
        .unionByName(crc.select("member_id", "line_of_business", "measure_name", "is_compliant", "measurement_year"))
        .unionByName(prev.select("member_id", "line_of_business", "measure_name", "is_compliant", "measurement_year"))
    )


@dlt.table(
    name="gold_hedis_provider",
    comment=(
        "HEDIS compliance rates aggregated by provider and measure. "
        "Joins member-level measures with claims to attribute to rendering provider."
    ),
)
def gold_hedis_provider():
    hedis_member = _read_table(spark, "gold_hedis_member")
    claims = _read_table(spark, "silver_claims_medical")
    providers = _read_table(spark, "silver_providers")

    # Map members to their rendering providers
    member_provider = (
        claims.filter(col("rendering_provider_npi").isNotNull())
        .select("member_id", col("rendering_provider_npi").alias("provider_npi"))
        .distinct()
    )

    return (
        hedis_member.join(member_provider, on="member_id", how="inner")
        .join(providers, member_provider["provider_npi"] == providers["npi"], how="left")
        .groupBy("provider_npi", "specialty", "measure_name")
        .agg(
            countDistinct("member_id").alias("eligible_members"),
            _sum("is_compliant").alias("compliant_members"),
        )
        .withColumn(
            "compliance_rate",
            col("compliant_members").cast("double")
            / when(col("eligible_members") == 0, lit(None)).otherwise(col("eligible_members")),
        )
    )


@dlt.table(
    name="gold_stars_provider",
    comment=(
        "CMS Stars-like composite star rating per provider. Averages compliance "
        "across all HEDIS measures and assigns 1-5 star rating."
    ),
)
def gold_stars_provider():
    hedis_provider = _read_table(spark, "gold_hedis_provider")
    providers = _read_table(spark, "silver_providers")

    return (
        hedis_provider.groupBy("provider_npi")
        .agg(
            countDistinct("measure_name").alias("measure_count"),
            avg("compliance_rate").alias("overall_compliance_rate"),
        )
        .join(
            providers.select("npi", "provider_name", "specialty"),
            col("provider_npi") == col("npi"),
            how="left",
        )
        .withColumn(
            "star_rating",
            when(col("overall_compliance_rate") >= 0.90, 5)
            .when(col("overall_compliance_rate") >= 0.75, 4)
            .when(col("overall_compliance_rate") >= 0.60, 3)
            .when(col("overall_compliance_rate") >= 0.45, 2)
            .otherwise(1),
        )
        .select(
            "provider_npi",
            "provider_name",
            "specialty",
            "measure_count",
            "overall_compliance_rate",
            "star_rating",
        )
    )


# =============================================================================
# RISK ADJUSTMENT METRICS
# =============================================================================


@dlt.table(
    name="gold_risk_adjustment_analysis",
    comment=(
        "Risk adjustment summary by line of business and model year. Includes "
        "RAF score distributions, HCC counts, high-risk prevalence, and "
        "estimated Medicare Advantage revenue."
    ),
)
def gold_risk_adjustment_analysis():
    risk = _read_table(spark, "silver_risk_adjustment_member")
    enrollment = _read_table(spark, "silver_enrollment")

    joined = risk.join(enrollment, on="member_id", how="inner")

    return (
        joined.groupBy("line_of_business", "model_year")
        .agg(
            countDistinct("member_id").alias("member_count"),
            avg("raf_score").alias("avg_raf_score"),
            expr("percentile_approx(raf_score, 0.5)").alias("median_raf_score"),
            _min("raf_score").alias("min_raf_score"),
            _max("raf_score").alias("max_raf_score"),
            _sum("raf_score").alias("total_raf"),
            avg("hcc_count").alias("avg_hcc_count"),
            (
                _sum(when(col("is_high_risk") == True, 1).otherwise(0)).cast("double")
                / when(countDistinct("member_id") == 0, lit(None)).otherwise(countDistinct("member_id"))
            ).alias("pct_high_risk"),
        )
        .withColumn(
            "estimated_annual_revenue",
            when(col("line_of_business") == "Medicare Advantage", col("total_raf") * 12000).otherwise(lit(None)),
        )
    )


@dlt.table(
    name="gold_coding_completeness",
    comment=(
        "HCC coding gap analysis. Identifies members with chronic diagnoses "
        "in claims but missing corresponding HCC codes in risk adjustment data."
    ),
)
def gold_coding_completeness():
    claims = _read_table(spark, "silver_claims_medical")
    risk = _read_table(spark, "silver_risk_adjustment_member")

    # Find members with chronic conditions in claims
    chronic = (
        claims.filter(
            col("primary_diagnosis_code").startswith("E11")
            | col("primary_diagnosis_code").startswith("I50")
            | col("primary_diagnosis_code").startswith("J44")
            | col("primary_diagnosis_code").startswith("N18")
        )
        .select("member_id", col("primary_diagnosis_code").alias("diagnosis_code"))
        .distinct()
        .withColumn(
            "condition_name",
            when(col("diagnosis_code").startswith("E11"), "Diabetes")
            .when(col("diagnosis_code").startswith("I50"), "Heart Failure")
            .when(col("diagnosis_code").startswith("J44"), "COPD")
            .otherwise("CKD"),
        )
        .withColumn(
            "expected_hccs",
            when(col("diagnosis_code").startswith("E11"), "HCC18,HCC19")
            .when(col("diagnosis_code").startswith("I50"), "HCC85")
            .when(col("diagnosis_code").startswith("J44"), "HCC111")
            .otherwise("HCC134,HCC135"),
        )
    )

    # Explode expected HCCs to check each individually
    exploded = chronic.withColumn("expected_hcc", explode(split(col("expected_hccs"), ",")))
    exploded = exploded.withColumn("expected_hcc", trim(col("expected_hcc")))

    # Join with risk adjustment to check if HCC is coded
    result = exploded.join(risk.select("member_id", "hcc_codes"), on="member_id", how="left")

    return (
        result.withColumn(
            "has_hcc_coded",
            when(
                col("hcc_codes").isNotNull() & col("hcc_codes").contains(col("expected_hcc")),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn(
            "coding_gap",
            when(
                col("hcc_codes").isNull() | ~col("hcc_codes").contains(col("expected_hcc")),
                lit(1),
            ).otherwise(lit(0)),
        )
        .select(
            "member_id",
            "diagnosis_code",
            "condition_name",
            "expected_hcc",
            "has_hcc_coded",
            "coding_gap",
        )
    )


# =============================================================================
# AI-POWERED CLASSIFICATION
# =============================================================================


@dlt.table(
    name="gold_denial_classification",
    comment=(
        "AI-classified denial reason categories using Databricks foundation "
        "model. Maps raw denial codes to actionable categories."
    ),
)
def gold_denial_classification():
    claims = _read_table(spark, "silver_claims_medical")

    distinct_denials = (
        claims.filter(col("denial_reason_code").isNotNull())
        .select("denial_reason_code")
        .distinct()
    )

    return distinct_denials.withColumn(
        "denial_category",
        expr(
            """ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                CONCAT(
                    'You are a healthcare claims expert. Classify this claim denial reason code into exactly one category: Administrative, Clinical, Eligibility, or Financial. ',
                    'Code: ', denial_reason_code,
                    '. Respond with only the category name, nothing else.'
                )
            )"""
        ),
    )


@dlt.table(
    name="gold_denial_analysis",
    comment=(
        "Denial analysis by AI-classified category, claim type, and line of "
        "business. Shows denial volumes, financial impact, and distribution."
    ),
)
def gold_denial_analysis():
    claims = _read_table(spark, "silver_claims_medical")
    classification = _read_table(spark, "gold_denial_classification")
    enrollment = _read_table(spark, "silver_enrollment")

    denial_claims = (
        claims.filter(col("denial_reason_code").isNotNull())
        .join(classification, on="denial_reason_code", how="inner")
        .join(enrollment.select("member_id", "line_of_business"), on="member_id", how="left")
    )

    total_denials = denial_claims.count()

    return (
        denial_claims.groupBy("denial_category", "claim_type", "line_of_business")
        .agg(
            count("*").alias("denial_count"),
            _sum("billed_amount").alias("total_denied_amount"),
            avg("billed_amount").alias("avg_denied_amount"),
        )
        .withColumn(
            "pct_of_total_denials",
            col("denial_count").cast("double") / lit(total_denials),
        )
    )


@dlt.table(
    name="gold_member_risk_narrative",
    comment=(
        "AI-generated clinical risk narratives for top 500 high-risk members. "
        "Provides care management-ready summaries based on RAF scores and HCC codes."
    ),
)
def gold_member_risk_narrative():
    risk = _read_table(spark, "silver_risk_adjustment_member")
    enrollment = _read_table(spark, "silver_enrollment")

    # Rank by RAF score and take top 500
    w = Window.orderBy(col("raf_score").desc())

    high_risk = (
        risk.join(enrollment.select("member_id", "line_of_business"), on="member_id", how="inner")
        .withColumn("risk_rank", row_number().over(w))
        .filter(col("risk_rank") <= 500)
    )

    return high_risk.withColumn(
        "clinical_summary",
        expr(
            """ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                CONCAT(
                    'You are a care management analyst. Given this health plan member profile, write a 2-sentence clinical summary for care coordination. ',
                    'RAF Score: ', CAST(raf_score AS STRING),
                    ', HCC Codes: ', COALESCE(hcc_codes, 'None'),
                    ', HCC Count: ', CAST(hcc_count AS STRING),
                    ', Line of Business: ', line_of_business,
                    '. Focus on key risk factors and recommended interventions.'
                )
            )"""
        ),
    ).select(
        "member_id",
        "raf_score",
        "hcc_codes",
        "hcc_count",
        "line_of_business",
        "risk_rank",
        "clinical_summary",
    )

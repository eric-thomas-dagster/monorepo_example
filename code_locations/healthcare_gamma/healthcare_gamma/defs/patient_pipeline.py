import dagster as dg
import pandas as pd


@dg.asset(
    group_name="healthcare_gamma",
    kinds={"ehr", "on_prem"},
    description="Raw patient records from on-premises EHR system"
)
def raw_patient_records(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Ingest patient records from legacy on-prem EHR system."""
    context.log.info("Demo mode: generating mock patient records")
    return pd.DataFrame({
        "patient_id": [f"PAT_{i:06d}" for i in range(1, 1001)],
        "age": [18 + (i % 80) for i in range(1000)],
        "diagnosis_code": [f"ICD10_{i % 100:03d}" for i in range(1000)],
        "visit_date": pd.date_range("2024-01-01", periods=1000, freq="H"),
        "visit_type": ["INPATIENT" if i % 5 == 0 else "OUTPATIENT" if i % 5 == 1 else "EMERGENCY" if i % 5 == 2 else "TELEHEALTH" for i in range(1000)],
        "provider_id": [f"DOC_{i % 50:03d}" for i in range(1000)]
    })


@dg.asset(
    group_name="healthcare_gamma",
    kinds={"python", "data_quality"},
    deps=[raw_patient_records],
    description="HIPAA-compliant patient data with PII redaction"
)
def hipaa_compliant_records(context: dg.AssetExecutionContext, raw_patient_records: pd.DataFrame) -> pd.DataFrame:
    """Apply HIPAA compliance transformations and PII redaction."""
    context.log.info("Applying HIPAA compliance rules and PII redaction")

    # Simulate HIPAA compliance transformations
    df = raw_patient_records.copy()
    df["patient_id_hash"] = df["patient_id"].apply(lambda x: f"HASH_{hash(x) % 1000000:06d}")
    df = df.drop(columns=["patient_id"])
    df["age_bucket"] = df["age"].apply(lambda x: "18-30" if x < 30 else "30-50" if x < 50 else "50-70" if x < 70 else "70+")

    return df


@dg.asset(
    group_name="healthcare_gamma",
    kinds={"python", "analytics"},
    deps=[hipaa_compliant_records],
    description="Patient population health analytics and risk stratification"
)
def population_health_analytics(context: dg.AssetExecutionContext, hipaa_compliant_records: pd.DataFrame) -> pd.DataFrame:
    """Generate population health analytics and risk stratification."""
    context.log.info("Computing population health metrics")

    # Aggregate by age bucket and visit type
    analytics = hipaa_compliant_records.groupby(["age_bucket", "visit_type"]).agg({
        "patient_id_hash": "count",
        "diagnosis_code": "nunique"
    }).reset_index()

    analytics.columns = ["age_bucket", "visit_type", "patient_count", "unique_diagnoses"]
    analytics["avg_risk_score"] = [0.3 + (i % 10) * 0.07 for i in range(len(analytics))]

    return analytics

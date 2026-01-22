"""Schedules for healthcare_gamma data pipelines."""

import dagster as dg

# Daily schedule for healthcare analytics
daily_healthcare_pipeline = dg.ScheduleDefinition(
    name="daily_healthcare_pipeline",
    target=dg.AssetSelection.keys(
        "raw_patient_records",
        "hipaa_compliant_records",
        "population_health_analytics"
    ),
    cron_schedule="0 3 * * *",  # 3 AM daily
    description="Daily healthcare analytics - HIPAA compliance checks and population health metrics"
)

"""Schedules for fintech_alpha data pipelines."""

import dagster as dg

# Daily schedule for fintech data pipeline
# This demonstrates how portfolio companies run on their own schedules,
# which then triggers downstream shared_analytics assets via automation conditions
daily_fintech_schedule = dg.ScheduleDefinition(
    name="daily_fintech_pipeline",
    target=dg.AssetSelection.keys(
        "s3_raw_raw_customer_transactions",
        "matillion_customer_aggregation",
        "transformed_customer_enrichment",
        "analytics_churn_prediction",
        "metrics_business_metrics"
    ),
    cron_schedule="0 2 * * *",  # 2 AM daily
    description="Daily fintech data pipeline: S3 → Matillion → dbt → Databricks → Azure DW (triggers shared_analytics via automation conditions)"
)

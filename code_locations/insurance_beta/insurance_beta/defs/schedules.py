"""Schedules for insurance_beta data pipelines."""

import dagster as dg

# Hourly schedule for insurance claims processing (high-frequency due to streaming nature)
# NOTE: streaming_insurance_claims is NOT included because it's an observable source asset (not materializable).
# The sensor (insurance_claims_observation_sensor) emits observations on it, making it turn green.
# With sensor-based orchestration (claims_freshness_trigger_sensor), this schedule serves as a fallback.
hourly_claims_processing = dg.ScheduleDefinition(
    name="hourly_claims_processing",
    target=dg.AssetSelection.keys(
        "enriched_claims",
        "fraud_predictions_claims_fraud"
    ),
    cron_schedule="0 * * * *",  # Every hour
    description="Hourly claims processing fallback: enrichment → fraud detection (sensor triggers on fresh Kafka data)"
)

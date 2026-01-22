"""Sensors for Streaming Data-Driven Orchestration.

This module contains sensors for:
1. Observing the external Kafka stream health (emits observations)
2. Triggering downstream processing when fresh data is available
"""

import dagster as dg
import random


@dg.sensor(
    name="insurance_claims_observation_sensor",
    minimum_interval_seconds=60,  # Check every minute
    description="Monitors external Kafka/Event Hub claims stream health and emits observations"
)
def insurance_claims_observation_sensor(context: dg.SensorEvaluationContext):
    """Sensor that monitors external Kafka/Event Hub stream health.

    This sensor:
    - Checks external stream health (lag, throughput, availability) every 60s
    - Emits observations on the streaming_insurance_claims asset
    - Makes the asset turn "green" based on observations (not materialization)
    - Represents monitoring of a stream managed by external/legacy infrastructure
    """
    context.log.info("Checking external Kafka/Event Hub claims stream status...")

    # In demo mode, simulate checking external stream metrics
    # In production, you'd query Kafka broker, check consumer groups, etc.
    messages_per_second = 450 + random.randint(-50, 100)
    consumer_lag_seconds = random.randint(5, 30)
    partition_count = 8
    messages_available = random.randint(5000, 15000)
    stream_health = "healthy" if consumer_lag_seconds < 60 else "degraded"

    context.log.info(
        f"Stream 'prod.insurance.claims.v1': {messages_per_second} msg/s, "
        f"{consumer_lag_seconds}s lag, {messages_available} msgs available"
    )

    # Emit observation about the Kafka/Event Hub stream
    # This makes streaming_insurance_claims turn green in the UI
    return dg.SensorResult(
        asset_events=[
            dg.AssetObservation(
                asset_key=dg.AssetKey("streaming_insurance_claims"),
                metadata={
                    "messages_per_second": messages_per_second,
                    "consumer_lag_seconds": consumer_lag_seconds,
                    "partition_count": partition_count,
                    "messages_available": messages_available,
                    "stream_health": stream_health,
                    "kafka_topic": "prod.insurance.claims.v1",
                    "event_hub_namespace": "insurance-beta-eventhub",
                }
            )
        ]
    )


@dg.sensor(
    name="claims_freshness_trigger_sensor",
    minimum_interval_seconds=120,  # Check every 2 minutes
    asset_selection=dg.AssetSelection.keys("enriched_claims"),
    description="Triggers claims enrichment when fresh streaming data is available from external Kafka"
)
def claims_freshness_trigger_sensor(context: dg.SensorEvaluationContext):
    """Sensor that triggers downstream processing when fresh streaming data is available.

    This demonstrates data-driven orchestration: instead of running on a fixed schedule,
    we trigger processing based on the actual availability of fresh data in the external stream.

    In production, this would:
    - Check latest observation metadata from external_claims_stream
    - Look at consumer lag and message availability
    - Only trigger if lag is low and fresh messages are available
    """
    context.log.info("Checking if fresh claims data is available for processing...")

    # In production, you could check the latest observation of external_claims_stream:
    # latest_observation = context.instance.get_latest_observation(AssetKey("external_claims_stream"))
    # if latest_observation.metadata["consumer_lag_seconds"] < 30:
    #     trigger processing

    # For demo, simulate checking stream freshness
    messages_available = random.randint(500, 2000)
    consumer_lag = random.randint(5, 45)

    # Only trigger if we have fresh data and lag is reasonable
    should_trigger = messages_available > 1000 and consumer_lag < 30

    if should_trigger:
        context.log.info(
            f"Fresh data available ({messages_available} messages, {consumer_lag}s lag), "
            f"triggering enrichment"
        )
        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    asset_selection=[dg.AssetKey("enriched_claims")],
                    tags={
                        "trigger": "fresh_streaming_data",
                        "messages_available": str(messages_available),
                        "consumer_lag_seconds": str(consumer_lag)
                    }
                )
            ]
        )
    else:
        context.log.info(f"Not enough fresh data ({messages_available} messages, {consumer_lag}s lag)")
        return dg.SensorResult(
            run_requests=[],
            skip_reason=f"Insufficient fresh data: {messages_available} messages, {consumer_lag}s lag"
        )

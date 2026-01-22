"""External Asset for Kafka/Event Hub Stream.

This represents an external Kafka stream that is managed outside of Dagster.
We observe it via sensors but do not materialize it directly. This is a true
external asset that demonstrates monitoring infrastructure you don't own.
"""

import dagster as dg


# External asset - represents Kafka/Event Hub stream managed outside Dagster
# This asset is observable only - sensors emit observations about its health
streaming_insurance_claims = dg.AssetSpec(
    key="streaming_insurance_claims",
    description="External Kafka/Event Hub stream for real-time insurance claims (managed outside Dagster)",
    group_name="insurance_beta",
    kinds={"kafka", "azure_event_hub", "streaming"},
    metadata={
        "source": "External Kafka/Azure Event Hub",
        "topic": "prod.insurance.claims.v1",
        "broker": "kafka.insurance-beta.internal:9092",
        "event_hub_namespace": "insurance-beta-eventhub",
        "managed_by": "External Infrastructure Team",
        "dagster.observability_type": "external",
        "info": "This is an external asset. It turns green when sensors emit observations about the stream health, not from Dagster materialization."
    }
)

"""Observable Source Asset for External Kafka/Event Hub Stream.

This represents the external Kafka stream as an observable source asset.
It cannot be materialized directly - it only becomes "fresh" when sensors
emit observations about it.
"""

import dagster as dg


# Observable source asset - represents external Kafka/Event Hub stream
# This asset CANNOT be materialized - it only gets observations from sensors
streaming_insurance_claims = dg.SourceAsset(
    key=dg.AssetKey("streaming_insurance_claims"),
    description="External Kafka/Event Hub stream for real-time insurance claims (observable, not materializable)",
    group_name="insurance_beta",
    tags={
        "dagster/kind/kafka": "",
        "dagster/kind/azure_event_hub": "",
        "dagster/kind/streaming": ""
    },
    metadata={
        "source": "Kafka/Azure Event Hub",
        "topic": "prod.insurance.claims.v1",
        "broker": "kafka.insurance-beta.internal:9092",
        "event_hub_namespace": "insurance-beta-eventhub",
        "observable": True,
        "info": "This asset represents an external stream. It turns green when sensors emit observations, not from materialization."
    }
)

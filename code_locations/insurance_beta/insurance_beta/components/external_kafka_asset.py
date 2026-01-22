import dagster as dg
from typing import Optional


class ExternalKafkaAsset(dg.Component, dg.Model, dg.Resolvable):
    """External Kafka/Event Hub Asset Component.

    Creates an external asset (AssetSpec) representing a Kafka or Event Hub stream
    that is managed outside of Dagster. This asset is observable only - it cannot
    be materialized by Dagster.

    Use this pattern when you have existing streaming infrastructure that you want
    to monitor and incorporate into your lineage graph without managing it directly.
    """

    asset_key: str
    topic: str
    broker: Optional[str] = None
    event_hub_namespace: Optional[str] = None
    description: Optional[str] = None
    group_name: str = "insurance_beta"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = self.asset_key
        topic = self.topic
        broker = self.broker
        event_hub_namespace = self.event_hub_namespace
        description = self.description or f"External Kafka/Event Hub stream: {topic}"
        group_name = self.group_name

        # Create external asset spec - represents infrastructure we don't own
        external_asset = dg.AssetSpec(
            key=asset_key,
            description=description,
            group_name=group_name,
            kinds={"kafka", "azure_event_hub", "streaming"},
            metadata={
                "source": "External Kafka/Azure Event Hub",
                "topic": topic,
                "broker": broker or "N/A",
                "event_hub_namespace": event_hub_namespace or "N/A",
                "managed_by": "External Infrastructure Team",
                "dagster.observability_type": "external",
                "info": (
                    "This is an external asset. It turns green when sensors emit "
                    "observations about the stream health, not from Dagster materialization."
                )
            }
        )

        return dg.Definitions(assets=[external_asset])

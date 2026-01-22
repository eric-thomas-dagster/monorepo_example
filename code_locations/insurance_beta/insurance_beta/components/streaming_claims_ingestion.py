import dagster as dg
import pandas as pd
from typing import Optional

class StreamingClaimsIngestion(dg.Component, dg.Model, dg.Resolvable):
    """Streaming Claims Ingestion Component.

    Ingests real-time insurance claims from streaming sources (Kafka/Event Hub).
    Supports both production mode (actual stream) and demo mode (mock streaming data).
    """

    stream_topic: str
    demo_mode: bool = True
    kafka_broker: Optional[str] = None
    event_hub_namespace: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        stream_topic = self.stream_topic
        kafka_broker = self.kafka_broker
        event_hub_namespace = self.event_hub_namespace

        @dg.asset(
            name=f"streaming_{stream_topic}",
            group_name="insurance_beta",
            kinds={"kafka", "azure_event_hub", "streaming"},
            description=f"Real-time insurance claims from {stream_topic} stream",
            metadata={
                "source": "Kafka/EventHub",
                "topic": stream_topic,
                "broker": kafka_broker,
                "namespace": event_hub_namespace
            }
        )
        def streaming_claims_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: generating mock streaming claims for {stream_topic}")
                # Simulate streaming claims data
                return pd.DataFrame({
                    "claim_id": [f"CLM_{i:06d}" for i in range(1, 501)],
                    "policy_id": [f"POL_{(i % 200):05d}" for i in range(1, 501)],
                    "claim_type": ["AUTO" if i % 4 == 0 else "HOME" if i % 4 == 1 else "LIFE" if i % 4 == 2 else "HEALTH" for i in range(500)],
                    "claim_amount": [1000.0 + (i * 47.3) % 50000 for i in range(500)],
                    "claim_status": ["SUBMITTED"] * 500,
                    "timestamp": pd.date_range("2024-01-01", periods=500, freq="min"),
                    "claimant_age": [25 + (i % 50) for i in range(500)],
                    "incident_location": [f"REGION_{i % 10}" for i in range(500)]
                })
            else:
                context.log.info(f"Production mode: consuming from {stream_topic}")
                pass  # Production streaming consumption would go here

        return dg.Definitions(assets=[streaming_claims_data])

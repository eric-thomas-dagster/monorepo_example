import dagster as dg
import random
from typing import Optional

class StreamingObservationSensor(dg.Component, dg.Model, dg.Resolvable):
    """Streaming Observation Sensor Component.

    Creates a sensor that observes external streaming sources (Kafka/Event Hub).
    Does NOT materialize the stream - just monitors it and emits observations.

    This represents the pattern for monitoring existing streaming infrastructure
    that you don't want to migrate into Dagster immediately.
    """

    stream_name: str
    stream_topic: str
    check_interval_seconds: int = 60
    demo_mode: bool = True
    kafka_broker: Optional[str] = None
    event_hub_namespace: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        stream_name = self.stream_name
        stream_topic = self.stream_topic
        kafka_broker = self.kafka_broker
        event_hub_namespace = self.event_hub_namespace
        check_interval = self.check_interval_seconds

        @dg.sensor(
            name=f"{stream_name}_observation_sensor",
            minimum_interval_seconds=check_interval,
            description=f"Observes external {stream_topic} Kafka/Event Hub stream and emits health metrics"
        )
        def streaming_observation_sensor(context: dg.SensorEvaluationContext):
            """Sensor that monitors external Kafka/Event Hub stream.

            This sensor:
            - Checks external stream health (lag, throughput, availability)
            - Emits observations with metrics
            - Does NOT materialize the stream data
            - Represents streams managed by legacy infrastructure
            """
            if demo_mode:
                context.log.info(f"Demo mode: Observing external stream {stream_topic}")
                # Simulate checking external stream metrics
                messages_per_second = 450 + random.randint(-50, 100)
                consumer_lag_seconds = random.randint(5, 30)
                partition_count = 8
                messages_available = random.randint(5000, 15000)
                stream_health = "healthy" if consumer_lag_seconds < 60 else "degraded"
            else:
                context.log.info(f"Production mode: Querying Kafka broker for {stream_topic}")
                # Production would query actual Kafka metrics:
                # from confluent_kafka.admin import AdminClient
                # admin = AdminClient({'bootstrap.servers': kafka_broker})
                # consumer_groups = admin.list_consumer_groups()
                # ... get lag, throughput, etc.
                pass

            context.log.info(
                f"Stream '{stream_topic}': {messages_per_second} msg/s, "
                f"{consumer_lag_seconds}s lag, {messages_available} msgs available"
            )

            # Emit observation about the Kafka/Event Hub stream
            # This creates a time-series record of stream health
            # The observation is associated with streaming_insurance_claims asset
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
                            "kafka_topic": stream_topic,
                            "broker": kafka_broker or event_hub_namespace,
                        }
                    )
                ]
            )

        return dg.Definitions(sensors=[streaming_observation_sensor])

import dagster as dg
import random
from typing import List


class StreamingFreshnessSensor(dg.Component, dg.Model, dg.Resolvable):
    """Streaming Freshness Trigger Sensor Component.

    Creates a sensor that monitors external streaming data freshness and triggers
    downstream processing when fresh data is available. This is data-driven
    orchestration - we only process when there's actually new data to consume.

    This complements the observation sensor (which monitors health) by actually
    triggering work based on data availability.
    """

    sensor_name: str
    target_assets: List[str]
    check_interval_seconds: int = 120
    demo_mode: bool = True

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sensor_name = self.sensor_name
        target_assets = self.target_assets
        check_interval = self.check_interval_seconds
        demo_mode = self.demo_mode

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=check_interval,
            asset_selection=dg.AssetSelection.keys(*target_assets),
            description=f"Triggers {', '.join(target_assets)} when fresh streaming data is available"
        )
        def streaming_freshness_sensor(context: dg.SensorEvaluationContext):
            """Sensor that triggers downstream processing when fresh data is available.

            This demonstrates data-driven orchestration: instead of running on a
            fixed schedule, we trigger processing based on actual data availability.
            """
            context.log.info("Checking if fresh streaming data is available for processing...")

            if demo_mode:
                # For demo, simulate checking stream freshness
                messages_available = random.randint(500, 2000)
                consumer_lag = random.randint(5, 45)
            else:
                # In production, check the latest observation metadata:
                # latest_observation = context.instance.get_latest_observation(
                #     AssetKey("streaming_insurance_claims")
                # )
                # messages_available = latest_observation.metadata["messages_available"]
                # consumer_lag = latest_observation.metadata["consumer_lag_seconds"]
                pass

            # Only trigger if we have fresh data and lag is reasonable
            should_trigger = messages_available > 1000 and consumer_lag < 30

            if should_trigger:
                context.log.info(
                    f"Fresh data available ({messages_available} messages, {consumer_lag}s lag), "
                    f"triggering processing"
                )
                return dg.SensorResult(
                    run_requests=[
                        dg.RunRequest(
                            asset_selection=[dg.AssetKey(asset) for asset in target_assets],
                            tags={
                                "trigger": "fresh_streaming_data",
                                "messages_available": str(messages_available),
                                "consumer_lag_seconds": str(consumer_lag)
                            }
                        )
                    ]
                )
            else:
                context.log.info(
                    f"Not enough fresh data ({messages_available} messages, {consumer_lag}s lag)"
                )
                return dg.SensorResult(
                    run_requests=[],
                    skip_reason=f"Insufficient fresh data: {messages_available} messages, {consumer_lag}s lag"
                )

        return dg.Definitions(sensors=[streaming_freshness_sensor])

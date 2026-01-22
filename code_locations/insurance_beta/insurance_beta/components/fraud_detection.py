import dagster as dg
import pandas as pd
from typing import Optional

class FraudDetection(dg.Component, dg.Model, dg.Resolvable):
    """Fraud Detection Component.

    Runs ML-based fraud detection on enriched claims using Databricks.
    Supports both production mode (actual Databricks ML job) and demo mode (mock predictions).
    """

    model_name: str
    upstream_asset: str
    demo_mode: bool = True
    databricks_workspace_url: Optional[str] = None
    model_serving_endpoint: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        model_name = self.model_name
        upstream_asset = self.upstream_asset
        databricks_workspace_url = self.databricks_workspace_url
        model_serving_endpoint = self.model_serving_endpoint

        @dg.asset(
            name=f"fraud_predictions_{model_name}",
            group_name="insurance_beta",
            kinds={"databricks", "ml", "pyspark"},
            deps=[upstream_asset],
            description=f"Fraud detection predictions from {model_name} model",
            metadata={
                "platform": "Databricks",
                "model": model_name,
                "workspace": databricks_workspace_url,
                "endpoint": model_serving_endpoint
            }
        )
        def fraud_detection_results(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: generating fraud predictions for {model_name}")
                # Simulate fraud detection results
                return pd.DataFrame({
                    "claim_id": [f"CLM_{i:06d}" for i in range(1, 501)],
                    "fraud_probability": [0.05 + (i % 100) * 0.009 for i in range(500)],
                    "fraud_risk_category": ["LOW" if i % 100 < 70 else "MEDIUM" if i % 100 < 90 else "HIGH" for i in range(500)],
                    "anomaly_score": [0.1 + (i % 50) * 0.018 for i in range(500)],
                    "flagged_for_review": [i % 100 >= 85 for i in range(500)],
                    "model_confidence": [0.75 + (i % 20) * 0.01 for i in range(500)],
                    "prediction_timestamp": pd.Timestamp.now(),
                    "model_version": ["fraud_detect_v3.2"] * 500,
                    "suspicious_patterns": [[] if i % 100 < 85 else ["HIGH_AMOUNT", "DUPLICATE_CLAIM"] if i % 2 == 0 else ["UNUSUAL_LOCATION", "POLICY_CHANGE"] for i in range(500)]
                })
            else:
                context.log.info(f"Production mode: running Databricks fraud detection {model_name}")
                pass  # Production Databricks ML inference would go here

        return dg.Definitions(assets=[fraud_detection_results])

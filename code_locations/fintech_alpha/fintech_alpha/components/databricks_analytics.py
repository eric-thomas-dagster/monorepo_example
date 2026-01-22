import dagster as dg
import pandas as pd
from typing import Optional

class DatabricksAnalytics(dg.Component, dg.Model, dg.Resolvable):
    """Databricks Analytics Component.

    Runs ML models and advanced analytics on Databricks platform.
    Supports both production mode (actual Databricks job) and demo mode (mock ML output).
    """

    job_name: str
    upstream_asset: str
    demo_mode: bool = True
    databricks_workspace_url: Optional[str] = None
    cluster_id: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        job_name = self.job_name
        upstream_asset = self.upstream_asset
        databricks_workspace_url = self.databricks_workspace_url
        cluster_id = self.cluster_id

        @dg.asset(
            name=f"analytics_{job_name}",
            kinds={"databricks", "pyspark", "ml"},
            group_name="fintech_alpha",
            deps=[upstream_asset],
            description=f"Databricks {job_name} ML model and analytics results",
            metadata={
                "platform": "Databricks",
                "job": job_name,
                "workspace": databricks_workspace_url,
                "cluster": cluster_id
            }
        )
        def databricks_ml_results(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: generating mock ML results for {job_name}")
                # Simulate ML model predictions
                return pd.DataFrame({
                    "customer_id": [f"CUST_{i:04d}" for i in range(100)],
                    "churn_probability": [0.05 + (i % 20) * 0.04 for i in range(100)],
                    "lifetime_value_prediction": [50000.0 + (i * 1000) for i in range(100)],
                    "recommended_action": ["RETAIN" if i % 3 == 0 else "UPSELL" if i % 3 == 1 else "MONITOR" for i in range(100)],
                    "confidence_score": [0.75 + (i % 10) * 0.02 for i in range(100)],
                    "model_version": ["v2.3.1"] * 100,
                    "prediction_timestamp": pd.date_range("2024-01-01", periods=100, freq="D")
                })
            else:
                context.log.info(f"Production mode: running Databricks job {job_name}")
                pass  # Production Databricks job submission would go here

        return dg.Definitions(assets=[databricks_ml_results])

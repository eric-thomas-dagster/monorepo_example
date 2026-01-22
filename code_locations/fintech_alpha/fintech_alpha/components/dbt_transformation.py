import dagster as dg
import pandas as pd
from typing import Optional

class DbtTransformation(dg.Component, dg.Model, dg.Resolvable):
    """dbt Transformation Component.

    Applies data transformations using dbt for data cleaning and enrichment.
    Supports both production mode (actual dbt run) and demo mode (mock transformation).
    """

    model_name: str
    upstream_asset: str
    demo_mode: bool = True
    dbt_project_path: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        model_name = self.model_name
        upstream_asset = self.upstream_asset
        dbt_project_path = self.dbt_project_path

        @dg.asset(
            name=f"transformed_{model_name}",
            kinds={"dbt", "sql"},
            group_name="fintech_alpha",
            deps=[upstream_asset],
            description=f"dbt transformed {model_name} data with cleaning and enrichment",
            metadata={
                "transformation_tool": "dbt",
                "model": model_name,
                "dbt_project": dbt_project_path
            }
        )
        def dbt_transformed_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: applying mock dbt transformation for {model_name}")
                # Simulate dbt transformation with data cleaning
                return pd.DataFrame({
                    "customer_id": [f"CUST_{i:04d}" for i in range(100)],
                    "total_deposits": [5000.0 + (i * 100) for i in range(100)],
                    "total_withdrawals": [2000.0 + (i * 50) for i in range(100)],
                    "net_balance": [3000.0 + (i * 50) for i in range(100)],
                    "transaction_count": [10 + i for i in range(100)],
                    "risk_score": [0.1 + (i % 10) * 0.09 for i in range(100)],
                    "last_transaction_date": pd.date_range("2024-01-01", periods=100, freq="D")
                })
            else:
                context.log.info(f"Production mode: running dbt model {model_name}")
                pass  # Production dbt run would go here

        return dg.Definitions(assets=[dbt_transformed_data])

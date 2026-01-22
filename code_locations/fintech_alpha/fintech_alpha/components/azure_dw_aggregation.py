import dagster as dg
import pandas as pd
from typing import Optional

class AzureDWAggregation(dg.Component, dg.Model, dg.Resolvable):
    """Azure Data Warehouse Aggregation Component.

    Creates business metrics and aggregations in Azure Synapse Analytics / Azure DW.
    Supports both production mode (actual Azure DW queries) and demo mode (mock aggregations).
    """

    aggregation_name: str
    upstream_asset: str
    demo_mode: bool = True
    azure_dw_server: Optional[str] = None
    database: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        aggregation_name = self.aggregation_name
        upstream_asset = self.upstream_asset
        azure_dw_server = self.azure_dw_server
        database = self.database

        @dg.asset(
            name=f"metrics_{aggregation_name}",
            kinds={"azure", "synapse", "sql"},
            group_name="fintech_alpha",
            deps=[upstream_asset],
            description=f"Azure DW {aggregation_name} business metrics and aggregations",
            metadata={
                "platform": "Azure Synapse Analytics",
                "aggregation": aggregation_name,
                "server": azure_dw_server,
                "database": database
            }
        )
        def azure_business_metrics(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: generating mock business metrics for {aggregation_name}")
                # Simulate business metrics aggregation
                return pd.DataFrame({
                    "business_unit": ["FINTECH_ALPHA", "INSURANCE_BETA", "HEALTHCARE_GAMMA", "RETAIL_DELTA"],
                    "total_revenue": [12500000.0, 8900000.0, 15200000.0, 6700000.0],
                    "customer_count": [5400, 3200, 6800, 2100],
                    "avg_customer_value": [2314.81, 2781.25, 2235.29, 3190.48],
                    "high_risk_customers": [324, 192, 408, 126],
                    "churn_rate": [0.045, 0.037, 0.052, 0.061],
                    "quarter": ["2024-Q1"] * 4,
                    "last_updated": [pd.Timestamp.now()] * 4
                })
            else:
                context.log.info(f"Production mode: running Azure DW aggregation {aggregation_name}")
                pass  # Production Azure DW query would go here

        return dg.Definitions(assets=[azure_business_metrics])

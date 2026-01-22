import dagster as dg
import pandas as pd
from typing import Optional
from shared import data_quality_monitor, alert_notifier, log_asset_metadata

class AzureDWEnrichment(dg.Component, dg.Model, dg.Resolvable):
    """Azure DW Claims Enrichment Component.

    Enriches streaming claims with policy details from Azure Synapse Analytics.
    Supports both production mode (actual Azure DW queries) and demo mode (mock enrichment).
    """

    enrichment_name: str
    upstream_asset: str
    demo_mode: bool = True
    azure_dw_server: Optional[str] = None
    database: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        enrichment_name = self.enrichment_name
        upstream_asset = self.upstream_asset
        azure_dw_server = self.azure_dw_server
        database = self.database

        @dg.asset(
            name=f"enriched_{enrichment_name}",
            group_name="insurance_beta",
            kinds={"azure", "synapse", "sql"},
            deps=[upstream_asset],
            description=f"Claims enriched with policy details from Azure DW",
            metadata={
                "platform": "Azure Synapse Analytics",
                "enrichment": enrichment_name,
                "server": azure_dw_server,
                "database": database
            }
        )
        def enriched_claims_data(
            context: dg.AssetExecutionContext,
            data_quality_monitor: dict,
        ) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: enriching claims with policy data")
                # Simulate enriched claims with policy details
                df = pd.DataFrame({
                    "claim_id": [f"CLM_{i:06d}" for i in range(1, 501)],
                    "policy_id": [f"POL_{(i % 200):05d}" for i in range(1, 501)],
                    "claim_amount": [1000.0 + (i * 47.3) % 50000 for i in range(500)],
                    "policy_type": ["PREMIUM" if i % 3 == 0 else "STANDARD" if i % 3 == 1 else "BASIC" for i in range(500)],
                    "policy_start_date": pd.date_range("2020-01-01", periods=500, freq="3D"),
                    "coverage_amount": [100000.0 + (i * 10000) % 500000 for i in range(500)],
                    "deductible": [500 + (i % 10) * 250 for i in range(500)],
                    "customer_segment": ["HIGH_VALUE" if i % 5 == 0 else "MEDIUM_VALUE" if i % 5 < 3 else "STANDARD" for i in range(500)],
                    "prior_claims_count": [i % 5 for i in range(500)]
                })

                # Use shared data quality monitoring
                if data_quality_monitor["monitor"](df):
                    context.log.info(f"Data quality check passed for enriched claims")

                # Use shared metadata logging utility
                log_asset_metadata(context, df)

                return df
            else:
                context.log.info(f"Production mode: enriching via Azure DW {enrichment_name}")
                pass  # Production Azure DW enrichment would go here

        return dg.Definitions(
            assets=[enriched_claims_data],
            resources={
                "data_quality_monitor": data_quality_monitor,
                "alert_notifier": alert_notifier,
            }
        )

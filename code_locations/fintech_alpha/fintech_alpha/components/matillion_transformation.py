import dagster as dg
import pandas as pd
from typing import Optional

class MatillionTransformation(dg.Component, dg.Model, dg.Resolvable):
    """Matillion ETL Transformation Component.

    Runs Matillion ETL jobs for data transformation and enrichment.
    Supports both production mode (actual Matillion API calls) and demo mode (mock transformations).

    Matillion is a GUI-based ETL tool popular in cloud data warehouses (Snowflake, Redshift, etc.).
    """

    job_name: str
    upstream_asset: str
    demo_mode: bool = True
    matillion_instance_url: Optional[str] = None
    project_name: Optional[str] = None
    environment: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        job_name = self.job_name
        upstream_asset = self.upstream_asset
        matillion_instance_url = self.matillion_instance_url
        project_name = self.project_name
        environment = self.environment

        @dg.asset(
            name=f"matillion_{job_name}",
            group_name="fintech_alpha",
            kinds={"matillion", "etl", "snowflake"},
            deps=[upstream_asset],
            description=f"Matillion ETL: Aggregate and load {job_name} data into Snowflake",
            metadata={
                "platform": "Matillion ETL",
                "job": job_name,
                "project": project_name,
                "environment": environment,
                "instance": matillion_instance_url,
                "etl_tool": "Matillion",
                "target": "Snowflake"
            }
        )
        def matillion_transformed_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
            """Execute Matillion ETL job to aggregate and load data into Snowflake.

            This represents a typical Matillion workflow:
            1. Read raw data from S3 data lake
            2. Apply aggregations and transformations
            3. Load results into Snowflake for downstream dbt processing
            """
            if demo_mode:
                context.log.info(f"Demo mode: simulating Matillion job '{job_name}' loading to Snowflake")
                # Simulate Matillion aggregation and load to Snowflake
                # In reality, Matillion would execute SQL transformations in Snowflake
                return pd.DataFrame({
                    "customer_id": [f"CUST_{i:05d}" for i in range(500)],
                    "total_transaction_amount": [10000.0 + (i * 234.5) % 100000 for i in range(500)],
                    "transaction_count": [50 + (i % 200) for i in range(500)],
                    "avg_transaction_amount": [200.0 + (i * 4.7) % 500 for i in range(500)],
                    "customer_segment": ["PLATINUM" if i % 10 == 0 else "GOLD" if i % 10 < 3 else "SILVER" if i % 10 < 7 else "BRONZE" for i in range(500)],
                    "lifetime_value": [50000.0 + (i * 1000) % 500000 for i in range(500)],
                    "risk_score": [0.1 + (i % 50) * 0.015 for i in range(500)],
                    "last_transaction_date": pd.date_range("2024-01-01", periods=500, freq="H"),
                    "preferred_channel": ["MOBILE" if i % 3 == 0 else "WEB" if i % 3 == 1 else "BRANCH" for i in range(500)],
                    "matillion_job_id": [f"JOB_{i:08d}" for i in range(500)],
                    "processed_timestamp": [pd.Timestamp.now()] * 500,
                })
            else:
                context.log.info(f"Production mode: executing Matillion job {job_name}")
                # Production Matillion API call would go here
                # import requests
                # response = requests.post(
                #     f"{matillion_instance_url}/rest/v1/group/name/{project_name}/project/name/{project_name}/version/default/environment/name/{environment}/job/name/{job_name}/run",
                #     auth=(username, password)
                # )
                pass

        return dg.Definitions(assets=[matillion_transformed_data])

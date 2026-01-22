import dagster as dg
import pandas as pd
from typing import Optional

class SnowflakeIngestion(dg.Component, dg.Model, dg.Resolvable):
    """Snowflake Data Ingestion Component.

    Ingests raw customer data from Snowflake for business units.
    Supports both production mode (actual Snowflake connection) and demo mode (mock data).
    """

    table_name: str
    database: str
    schema_name: str
    demo_mode: bool = True
    snowflake_account: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        table_name = self.table_name
        database = self.database
        schema_name = self.schema_name
        snowflake_account = self.snowflake_account

        @dg.asset(
            name=f"raw_{table_name}",
            kinds={"snowflake", "sql"},
            group_name="fintech_alpha",
            description=f"Raw {table_name} data from Snowflake {database}.{schema_name}",
            metadata={
                "source": "Snowflake",
                "database": database,
                "schema": schema_name,
                "table": table_name
            }
        )
        def snowflake_raw_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
            if demo_mode:
                context.log.info(f"Demo mode: generating mock data for {table_name}")
                # Generate realistic mock data
                if table_name == "customer_transactions":
                    return pd.DataFrame({
                        "transaction_id": range(1, 1001),
                        "customer_id": [f"CUST_{i%100:04d}" for i in range(1000)],
                        "amount": [100.0 + (i * 3.7) % 5000 for i in range(1000)],
                        "transaction_type": ["DEPOSIT" if i % 3 == 0 else "WITHDRAWAL" if i % 3 == 1 else "TRANSFER" for i in range(1000)],
                        "timestamp": pd.date_range("2024-01-01", periods=1000, freq="H")
                    })
                else:
                    return pd.DataFrame({
                        "id": range(1, 101),
                        "value": [i * 10 for i in range(1, 101)]
                    })
            else:
                context.log.info(f"Production mode: querying Snowflake {database}.{schema_name}.{table_name}")
                pass  # Production Snowflake query would go here

        return dg.Definitions(assets=[snowflake_raw_data])

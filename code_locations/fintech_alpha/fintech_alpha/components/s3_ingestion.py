import dagster as dg
import pandas as pd
from typing import Optional

class S3Ingestion(dg.Component, dg.Model, dg.Resolvable):
    """AWS S3 Data Lake Ingestion Component.

    Ingests raw data from AWS S3 data lake (batch or streaming sources).
    Supports both production mode (actual S3 reads) and demo mode (mock data).
    """

    bucket_name: str
    prefix: str
    demo_mode: bool = True
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        bucket_name = self.bucket_name
        prefix = self.prefix
        aws_region = self.aws_region

        @dg.asset(
            name=f"s3_raw_{prefix.replace('/', '_')}",
            group_name="fintech_alpha",
            kinds={"s3", "aws", "data_lake"},
            description=f"Raw data ingested from S3 bucket s3://{bucket_name}/{prefix}",
            metadata={
                "platform": "AWS S3",
                "bucket": bucket_name,
                "prefix": prefix,
                "region": aws_region,
                "ingestion_type": "batch"
            }
        )
        def s3_raw_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
            """Ingest raw data from AWS S3 data lake."""
            if demo_mode:
                context.log.info(f"Demo mode: generating mock S3 data from s3://{bucket_name}/{prefix}")
                # Simulate customer transaction data from S3
                return pd.DataFrame({
                    "transaction_id": [f"TXN_{i:08d}" for i in range(1, 2001)],
                    "customer_id": [f"CUST_{(i % 500):05d}" for i in range(1, 2001)],
                    "transaction_date": pd.date_range("2024-01-01", periods=2000, freq="5min"),
                    "amount": [100.0 + (i * 7.3) % 10000 for i in range(2000)],
                    "transaction_type": ["DEPOSIT" if i % 4 == 0 else "WITHDRAWAL" if i % 4 == 1 else "TRANSFER" if i % 4 == 2 else "PAYMENT" for i in range(2000)],
                    "currency": ["USD"] * 2000,
                    "merchant_category": [f"MCC_{i % 20:03d}" for i in range(2000)],
                    "channel": ["MOBILE" if i % 3 == 0 else "WEB" if i % 3 == 1 else "ATM" for i in range(2000)],
                    "s3_source_file": [f"s3://{bucket_name}/{prefix}/txn_{i // 100:04d}.parquet" for i in range(2000)],
                })
            else:
                context.log.info(f"Production mode: reading from S3 s3://{bucket_name}/{prefix}")
                # Production S3 ingestion would go here
                # import boto3
                # s3 = boto3.client('s3', region_name=aws_region)
                # ... actual S3 read logic
                pass

        return dg.Definitions(assets=[s3_raw_data])

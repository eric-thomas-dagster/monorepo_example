# Multi-Code Location Dagster Demo

A demonstration of managing multiple independent data pipelines in a single Dagster workspace using the mono-repo pattern.

## What This Demonstrates

- **Multiple code locations** - 4 independent code locations in one workspace
- **Cross-location dependencies** - Assets in one location depending on assets from other locations
- **Shared resources pattern** - Common resources imported by all code locations from a shared package
- **Reusable components** - Component-based architecture for building data pipelines
- **External asset observability** - Monitoring Kafka/Event Hub streams managed outside Dagster
- **Data-driven orchestration** - Sensors and automation conditions reacting to data availability

## Code Locations

### fintech_alpha
Financial services pipeline demonstrating AWS S3 → Matillion → dbt → Databricks → Azure DW flow.

**Key assets:**
- S3 data lake ingestion
- Matillion ETL transformations
- dbt model transformations
- Databricks ML predictions
- Azure DW business metrics

### insurance_beta
Insurance claims processing with streaming data and external asset monitoring.

**Key assets:**
- External Kafka/Event Hub stream (AssetSpec - observable only)
- Azure DW enrichment
- Databricks fraud detection

**Sensors:**
- Observation sensor - monitors external stream health every 60s
- Freshness sensor - triggers processing when fresh data is available

### healthcare_gamma
Healthcare data pipeline with HIPAA compliance.

**Key assets:**
- On-prem EHR ingestion
- HIPAA compliance transformations
- Population health analytics

### shared_analytics
Cross-company analytics demonstrating **cross-location lineage**.

**Key assets:**
- `consolidated_portfolio_metrics` - Consolidates data from all 3 business units
- `cross_company_risk_dashboard` - ML predictions from fintech and insurance
- `cross_sell_opportunities` - Cross-company opportunities
- `executive_summary_report` - Executive reporting

All assets use `AutomationCondition.eager()` to automatically materialize when upstream assets update.

## Getting Started

### Prerequisites

- Python 3.10-3.14
- [uv](https://docs.astral.sh/uv/) installed

### Local Development

```bash
# Install dependencies
uv sync

# Run the demo
./run_demo.sh
```

This starts:
- **Dagster Daemon** - Executes schedules, sensors, and automation conditions
- **Dagster Webserver** - UI at http://localhost:3000

Press `Ctrl+C` to stop both processes.

### What to Explore

1. **Code Locations** - View all 4 locations in the UI
2. **Global Asset Lineage** - See cross-location dependencies in Assets → View global asset lineage
3. **Automation** - Check Overview → Automation to see schedules, sensors, and automation conditions
4. **Materialize Assets** - Run a portfolio company asset and watch shared_analytics auto-trigger
5. **Observations** - Check the observation sensor emitting health metrics on the external Kafka stream

## Deploying to Dagster+

This demo is designed for **Dagster+ Serverless with PEX** deployment. For Docker-based deployments, see the Docker deployment section below.

### Prerequisites

- Dagster+ organization
- Dagster+ API token (from UI → Organization Settings → Tokens)

### Quick Deploy

```bash
# Set environment variables
export DAGSTER_CLOUD_ORGANIZATION="your-org-name"
export DAGSTER_CLOUD_API_TOKEN="your-api-token"

# Deploy all 4 code locations
./deploy_to_dagster_plus.sh
```

This deploys using **PEX builds** (faster than Docker):
- fintech-alpha
- insurance-beta
- healthcare-gamma
- shared-analytics

View at: `https://your-org.dagster.cloud/prod`

### Manual Deploy (Single Location)

```bash
cd code_locations/fintech_alpha

# Option 1: Using dg CLI
dg plus deploy configure serverless
dg plus deploy

# Option 2: Using legacy CLI
dagster-cloud serverless deploy-python-executable \
    --organization YOUR_ORG \
    --api-token "YOUR_API_TOKEN" \
    --deployment prod \
    --location-file dagster_cloud.yaml \
    --location-name fintech-alpha
```

### Docker Deployments (Serverless or Hybrid)

⚠️ **Important for Docker:** When using Docker instead of PEX, you must include the `shared` package in your Docker images. PEX automatically bundles local dependencies, but Docker requires explicit copying and installation.

**Why this matters:**
- All code locations depend on the `shared` package via `pyproject.toml`
- PEX builds automatically include it
- Docker builds need to explicitly COPY and install it

#### Option 1: Single Dockerfile per Code Location

Each code location needs a Dockerfile that copies and installs the `shared` package:

```dockerfile
# code_locations/fintech_alpha/Dockerfile
FROM python:3.12-slim

WORKDIR /opt/dagster/app

# Install uv
RUN pip install uv

# Copy shared package (required dependency)
COPY ../shared /opt/dagster/shared

# Copy this code location
COPY . /opt/dagster/app

# Install shared, then this code location
RUN cd /opt/dagster/shared && uv pip install --system . && \
    cd /opt/dagster/app && uv pip install --system .

ENV DAGSTER_MODULE_NAME=fintech_alpha.definitions

CMD ["dagster", "code-server", "start", "-m", "${DAGSTER_MODULE_NAME}"]
```

**Build and deploy:**
```bash
# Build from repo root (important for COPY context)
docker build -f code_locations/fintech_alpha/Dockerfile \
    -t your-registry/fintech-alpha:latest .

# Push to registry
docker push your-registry/fintech-alpha:latest

# Deploy using dg CLI
cd code_locations/fintech_alpha
dg plus deploy configure serverless  # or: hybrid --agent-platform k8s
dg plus deploy build-and-push
```

#### Option 2: Single Dockerfile for All Locations (Simpler)

Use one Dockerfile that installs the entire mono-repo:

```dockerfile
# Dockerfile (at repo root)
FROM python:3.12-slim

WORKDIR /opt/dagster/app

RUN pip install uv

# Copy entire mono-repo
COPY . /opt/dagster/app

# Install everything (all code locations + shared)
RUN uv pip install --system -e .

# Module name will be set at runtime
CMD ["sh", "-c", "dagster code-server start -m ${DAGSTER_MODULE_NAME}"]
```

**Deploy with different module names:**
- Set `DAGSTER_MODULE_NAME` to `fintech_alpha.definitions`, `insurance_beta.definitions`, etc.
- In `dagster_cloud.yaml`, specify the module name for each location

See `Dockerfile.example` and `code_locations/insurance_beta/Dockerfile.example` for complete examples.

## Demo Mode

All components run in **demo mode** by default:
- No external credentials required
- Generates realistic mock data
- Perfect for local testing and demos

To use with real infrastructure, set `demo_mode: false` in the component YAML files under `code_locations/*/defs/` and provide actual credentials.

## Shared Resources Pattern

This demo demonstrates sharing common resources across multiple code locations using the `shared` package.

### How It Works

The `shared/` directory is a regular Python package (not a code location) that exports reusable resources:

```python
# Other code locations import from shared
from shared import data_quality_monitor, alert_notifier, log_asset_metadata

# Use in asset definitions
@dg.asset
def my_asset(context, data_quality_monitor: dict):
    # Use shared resource
    if data_quality_monitor["monitor"](df):
        context.log.info("Quality check passed")
```

### Shared Resources Available

- **data_quality_monitor** - Centralized data quality checking across all code locations
- **alert_notifier** - Common alerting for critical pipeline issues
- **log_asset_metadata()** - Utility function for consistent metadata logging

### Used By

- `healthcare_gamma` - Uses all shared resources in patient pipeline
- `insurance_beta` - Uses data quality monitoring in claims enrichment

This pattern allows you to:
- Define common resources once, use them everywhere
- Ensure consistent monitoring and alerting across all business units
- Reduce code duplication
- Centralize resource configuration

## Component Architecture

This demo uses Dagster's component system for reusable pipeline patterns:

**Component classes** (reusable):
- `components/*.py` - Define reusable component logic

**Component instances** (configured):
- `defs/*/defs.yaml` - Configure specific instances with parameters

Example:
```yaml
# defs/external_kafka_stream/defs.yaml
type: insurance_beta.components.external_kafka_asset.ExternalKafkaAsset

attributes:
  asset_key: streaming_insurance_claims
  topic: prod.insurance.claims.v1
  broker: kafka.insurance-beta.internal:9092
```

## Key Features

### External Assets
The `streaming_insurance_claims` asset demonstrates monitoring infrastructure you don't own:
- Defined with `AssetSpec` (not materialized by Dagster)
- Sensors emit observations about stream health
- Shows in lineage graph as an external dependency

### Cross-Location Lineage
The `shared_analytics` location depends on assets from all 3 business units, demonstrating how data flows across organizational boundaries in a single workspace.

### Automation Conditions
Assets in `shared_analytics` use `AutomationCondition.eager()` to automatically materialize when any upstream asset updates - no manual triggering required.

## Resources

- [Dagster Docs](https://docs.dagster.io/)
- [Multi-Code Location Guide](https://docs.dagster.io/concepts/code-locations)
- [Component Library](https://docs.dagster.io/guides/build/components)

---

**Built with Dagster 1.12.10**

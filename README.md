# Multi-Code Location Dagster Demo

A demonstration of managing multiple independent data pipelines in a single Dagster workspace using the mono-repo pattern.

## What This Demonstrates

- **Multiple code locations** - 5 independent code locations in one workspace
- **Cross-location dependencies** - Assets in one location depending on assets from other locations
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

### shared
Common utilities and resources available to all code locations.

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

1. **Code Locations** - View all 5 locations in the UI
2. **Global Asset Lineage** - See cross-location dependencies in Assets → View global asset lineage
3. **Automation** - Check Overview → Automation to see schedules, sensors, and automation conditions
4. **Materialize Assets** - Run a portfolio company asset and watch shared_analytics auto-trigger
5. **Observations** - Check the observation sensor emitting health metrics on the external Kafka stream

## Deploying to Dagster+ (Serverless)

This demo is designed for **Dagster+ Serverless** deployment. For Hybrid or other deployment types, you'll need to make the necessary configuration changes.

### Prerequisites

- Dagster+ organization
- Dagster+ API token (from UI → Organization Settings → Tokens)

### Quick Deploy

```bash
# Set environment variables
export DAGSTER_CLOUD_ORGANIZATION="your-org-name"
export DAGSTER_CLOUD_API_TOKEN="your-api-token"

# Deploy all 5 code locations
./deploy_to_dagster_plus.sh
```

This deploys using **PEX builds** (faster than Docker):
- fintech-alpha
- insurance-beta
- healthcare-gamma
- shared
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

## Demo Mode

All components run in **demo mode** by default:
- No external credentials required
- Generates realistic mock data
- Perfect for local testing and demos

To use with real infrastructure, set `demo_mode: false` in the component YAML files under `code_locations/*/defs/` and provide actual credentials.

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

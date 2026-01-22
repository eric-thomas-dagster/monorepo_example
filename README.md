# Multi-Code Location Dagster Demo

## Executive Summary

This demo showcases **a single Dagster deployment managing data orchestration across multiple business units**, demonstrating how to standardize and scale data operations across an entire organization.

### Key Value Propositions

1. **Single Deployment, Multiple Business Units** - One Dagster instance orchestrates pipelines for 3+ independent teams
2. **Streaming-First Architecture** - 70% streaming workloads with external assets for Kafka/Event Hub streams managed outside Dagster
3. **Hybrid Cloud (AWS + Azure + On-Prem)** - S3, Synapse, Databricks, and legacy systems in one unified platform
4. **Modern ETL Stack** - Matillion, dbt, Databricks, and Snowflake orchestration
5. **Airflow Replacement** - Modern, asset-centric orchestration replacing fragmented Airflow instances
6. **Shared Observability** - Unified lineage and monitoring across all business units
7. **Rapid Onboarding** - Reusable components accelerate new team integration

---

## Architecture Overview

### Workspace Structure

```
monorepo_example (Workspace)
├── code_locations/
│   ├── fintech_alpha/      # Fintech Business Unit
│   ├── insurance_beta/     # Insurance Business Unit
│   ├── healthcare_gamma/   # Healthcare Business Unit
│   ├── shared/             # Common Utilities & Resources
│   └── shared_analytics/   # 🔗 Cross-Company Analytics (NEW!)
└── workspace.yaml          # Orchestrates all code locations
```

Each code location represents an independent business unit with their own:
- Technology stack
- Data pipelines
- Business logic
- Compliance requirements

Yet all managed through **one unified Dagster deployment**.

#### 🔗 Cross-Code Location Lineage

**shared_analytics** demonstrates the killer feature: **cross-location lineage**. It creates consolidated analytics by depending on assets from all three business units:

- `consolidated_portfolio_metrics` ← depends on assets from fintech_alpha, insurance_beta, healthcare_gamma
- `cross_company_risk_dashboard` ← combines ML predictions from fintech & insurance
- `cross_sell_opportunities` ← identifies opportunities across all companies
- `executive_summary_report` ← top-level executive report

---

## Business Units

### 🏦 Fintech Alpha (code_locations/fintech_alpha)

**Scenario:** High-growth fintech company processing customer transactions

**Tech Stack (Hybrid AWS + Azure):**
- **AWS S3** (Data Lake) ⭐ NEW
- **Matillion** (ETL/Transformations) ⭐ NEW
- Snowflake (Data Warehouse)
- dbt (Transformations)
- Databricks (ML/Analytics)
- Azure Synapse (Business Metrics)

**Pipeline Flow:**
```
AWS S3 Raw Data → Matillion ETL (loads to Snowflake) → dbt Transformations → Databricks ML → Azure DW Metrics
```

**Assets:**
- `s3_raw_raw_customer_transactions` - Ingest from AWS S3 data lake ⭐ NEW
- `matillion_customer_aggregation` - Matillion ETL: aggregate S3 data and load to Snowflake ⭐ NEW
- `transformed_customer_enrichment` - dbt transformations on Matillion output
- `analytics_churn_prediction` - Databricks ML churn predictions
- `metrics_business_metrics` - Azure DW business KPIs

**Highlights:**
- **Hybrid Cloud Architecture** - Demonstrates AWS (S3) + Azure (Synapse) together
- **Multiple ETL Tools** - Shows Matillion + dbt working in tandem
- **Data Lake → Warehouse** - Modern data architecture pattern
- **Complete Lineage** - Full end-to-end visibility from raw data lake to business metrics

**Lineage Visualization:**
```
┌─────────────────────────────────────────────────────────────────────┐
│                        Fintech Alpha Pipeline                        │
└─────────────────────────────────────────────────────────────────────┘

   AWS S3 Data Lake
          │
          ├─> s3_raw_raw_customer_transactions
          │
          ▼
   Matillion ETL (GUI-based)
          │
          ├─> matillion_customer_aggregation (loads to Snowflake)
          │
          ▼
   dbt Transformations (code-based)
          │
          ├─> transformed_customer_enrichment
          │
          ▼
   Databricks ML
          │
          ├─> analytics_churn_prediction
          │
          ▼
   Azure DW Aggregation
          │
          └─> metrics_business_metrics
```

---

### 🏥 Insurance Beta (code_locations/insurance_beta)

**Scenario:** Insurance company with **streaming-first** real-time claims processing (70% streaming, 30% batch)

**Tech Stack:**
- Kafka/Azure Event Hub (Streaming)
- Azure Synapse Analytics (Enrichment)
- Databricks (Fraud Detection ML)

**Pipeline Flow:**
```
Kafka/Event Hub Stream
         ↓
    streaming_insurance_claims (Ingest from Kafka/Event Hub)
         ↓
    enriched_claims (Enrich with Azure DW policy data)
         ↓
    fraud_predictions_claims_fraud (Databricks ML)
```

**External Asset (AssetSpec):** ⭐
- `streaming_insurance_claims` - External Kafka/Event Hub stream (managed outside Dagster, observable only)
  - Defined using `dg.AssetSpec` to represent infrastructure you don't own
  - Turns green from sensor observations, not from Dagster materialization
  - Represents stream managed by external/legacy infrastructure team

**Materializable Assets:**
- `enriched_claims` - Claims enriched with Azure DW policy data (depends on streaming_insurance_claims)
- `fraud_predictions_claims_fraud` - Databricks ML fraud detection

**Sensors:** ⭐ NEW
- `insurance_claims_observation_sensor` - Monitors Kafka every 60s, emits observations on `streaming_insurance_claims` (lag, throughput, health status)
- `claims_freshness_trigger_sensor` - Triggers `enriched_claims` processing when fresh Kafka data is available

**Highlights:**
- **External Asset** - `streaming_insurance_claims` is defined with `AssetSpec` to represent external infrastructure
- **Observation-Driven Freshness** - Asset turns green from sensor observations every 60s
- **Stream Health Monitoring** - Track consumer lag, throughput, and health metrics
- **Dual Orchestration** - Scheduled (hourly fallback) AND sensor-driven (reactive to fresh data)
- **Streaming-First Architecture** - 70% streaming workloads
- **Real-time fraud detection** - Sub-minute latency from claim to fraud score
- Replaces rigid batch Airflow DAGs with reactive streaming orchestration

---

### 🏥 Healthcare Gamma (code_locations/healthcare_gamma)

**Scenario:** Healthcare provider with HIPAA-compliant patient data

**Tech Stack:**
- On-premises EHR Systems
- Python (HIPAA Compliance)
- Analytics (Population Health)

**Pipeline Flow:**
```
On-Prem EHR → HIPAA Compliance → Population Health Analytics
```

**Assets:**
- `raw_patient_records` - On-prem EHR ingestion
- `hipaa_compliant_records` - PII redaction & compliance
- `population_health_analytics` - Risk stratification

**Highlights:**
- HIPAA compliance automation
- Legacy on-prem integration
- Data privacy & security

---

### 🔧 Shared (code_locations/shared)

**Reusable resources available to all portfolio companies:**
- Data quality monitoring
- Alert notifications
- Common utilities
- Shared schedules & sensors

**Benefits:**
- Consistency across customers
- Reduced development time
- Centralized governance

---

### 📊 Shared Analytics (code_locations/shared_analytics) ⭐ NEW

**Scenario:** Enterprise-level analytics consolidating data across ALL portfolio companies

**Pipeline Flow:**
```
[Fintech + Insurance + Healthcare Assets]
              ↓
    Consolidated Metrics
              ↓
       Risk Dashboard
              ↓
    Executive Summary
```

**Assets:**
- `consolidated_portfolio_metrics` - Enterprise-wide metrics
- `cross_company_risk_dashboard` - Unified risk view across companies
- `cross_sell_opportunities` - Cross-portfolio business opportunities
- `executive_summary_report` - Weekly executive briefing

**🔗 Cross-Location Dependencies:**
This code location **depends on 7 assets** from other code locations:
- From fintech_alpha: `metrics_business_metrics`, `analytics_churn_prediction`, `transformed_customer_enrichment`
- From insurance_beta: `fraud_predictions_claims_fraud`, `enriched_claims`
- From healthcare_gamma: `population_health_analytics`, `hipaa_compliant_records`

**Value Proposition:**
- Demonstrates **true cross-team collaboration**
- Shows **unified data lineage** across the entire portfolio
- Proves Organizations can provide **value-added analytics** across all customers
- Highlights **cross-sell opportunities** between companies

---

## 🤖 Smart Automation & Orchestration

### Automation Conditions (Data-Driven Triggers)

**shared_analytics** uses **automation conditions** to automatically materialize when upstream data is ready:

```python
automation_condition=dg.AutomationCondition.eager()
# Automatically runs when ALL parent assets are updated
```

**How it works:**
1. Portfolio companies run on their own schedules:
   - `fintech_alpha`: Daily at 2 AM
   - `insurance_beta`: Hourly (streaming data)
   - `healthcare_gamma`: Daily at 3 AM

2. When portfolio assets finish, `shared_analytics` **automatically detects** the updates

3. Consolidated analytics materialize **reactively** without manual triggers

**Benefits:**
- ✅ **Data-driven orchestration** - responds to data availability, not just time
- ✅ **No coordination required** - portfolio companies work independently
- ✅ **Automatic propagation** - changes flow through the lineage automatically
- ✅ **Intelligent scheduling** - only runs when new data is actually available

### Schedules Overview

| Code Location | Schedule | Frequency | Assets Materialized | Purpose |
|--------------|----------|-----------|---------------------|---------|
| fintech_alpha | `daily_fintech_pipeline` | Daily 2 AM | S3 → Matillion → dbt → Databricks → Azure DW | Complete pipeline run |
| insurance_beta | `hourly_claims_processing` | Hourly | enriched_claims → fraud_predictions | Fraud detection (fallback for sensor) |
| healthcare_gamma | `daily_healthcare_pipeline` | Daily 3 AM | EHR → HIPAA → analytics | HIPAA compliance & analytics |
| shared_analytics | **Automation Conditions** | **When data ready** | All 4 consolidated assets | **Smart reactive processing** |

### Demo Value

This demonstrates how Dagster replaces **rigid Airflow DAGs** with **intelligent, data-aware orchestration**:

**Before (Airflow):**
- Hard-coded schedules for every pipeline
- Complex cross-DAG dependencies
- Wasted runs when upstream data hasn't changed
- Manual coordination between teams

**After (Dagster):**
- Portfolio companies work independently
- Shared analytics reacts automatically
- Only runs when new data is available
- Zero coordination overhead

---

## 🌊 Streaming-First Architecture with Asset Observations

This demo emphasizes **streaming workloads** (70%) as the primary use case, with batch processing (30%) as secondary.

### External Assets: Monitoring Infrastructure You Don't Own

Many organizations have **existing streaming pipelines** managed outside of Dagster. Dagster's **external assets with sensors** let you:

1. **Monitor without managing** - Observe external Kafka/Flink/Spark Streaming jobs
2. **Track health metrics** - Consumer lag, throughput, data freshness
3. **Trigger downstream work** - React when fresh data is available
4. **Maintain lineage** - Show dependencies on external infrastructure in your lineage graph

### Insurance Beta Example

**External Asset Definition (using AssetSpec):**
```python
streaming_insurance_claims = dg.AssetSpec(
    key="streaming_insurance_claims",
    description="External Kafka/Event Hub stream (managed outside Dagster)",
    group_name="insurance_beta",
    kinds={"kafka", "azure_event_hub", "streaming"},
    metadata={
        "source": "External Kafka/Azure Event Hub",
        "topic": "prod.insurance.claims.v1",
        "managed_by": "External Infrastructure Team"
    }
)
```

**Sensor with Asset Observations:**
```python
@dg.sensor(minimum_interval_seconds=60)
def insurance_claims_observation_sensor(context):
    # Check external Kafka stream health
    messages_per_second = check_kafka_throughput()
    consumer_lag = check_kafka_consumer_lag()

    # Emit observation → makes asset turn green
    return SensorResult(asset_events=[
        AssetObservation(
            asset_key=AssetKey("streaming_insurance_claims"),
            metadata={
                "messages_per_second": messages_per_second,
                "consumer_lag_seconds": consumer_lag,
                "stream_health": "healthy"
            }
        )
    ])
```

**Data-Driven Trigger Sensor:**
```python
@dg.sensor(minimum_interval_seconds=120)
def claims_freshness_trigger_sensor(context):
    # Check if fresh data is available
    if fresh_data_available():
        # Trigger downstream processing
        return SensorResult(run_requests=[
            RunRequest(asset_selection=[AssetKey("enriched_claims")])
        ])
```

### Key Benefits

- ✅ **Don't rip and replace** - Keep existing infrastructure, monitor with external assets
- ✅ **Gradual migration** - Observe now with `AssetSpec`, migrate later when ready
- ✅ **Unified observability** - See external + Dagster-managed assets in one lineage graph
- ✅ **Data-driven orchestration** - Trigger processing based on actual data availability
- ✅ **Proper external asset modeling** - Use `AssetSpec` to represent infrastructure you don't own

---

## 🎨 Asset Groups & Visual Organization

### What are Asset Groups?

Asset groups organize assets visually in the Dagster UI, making it easier to understand complex lineage graphs with many assets across multiple code locations.

### Grouping Strategy

In this demo, **every asset is assigned to an asset group matching its code location name**:

| Asset Group | Code Location | Number of Assets | Purpose |
|------------|---------------|------------------|---------|
| `fintech_alpha` | fintech_alpha | 4 | Fintech company pipelines |
| `insurance_beta` | insurance_beta | 3 | Insurance company pipelines |
| `healthcare_gamma` | healthcare_gamma | 3 | Healthcare company pipelines |
| `shared_analytics` | shared_analytics | 4 | Cross-company consolidated analytics |

### Benefits

**Before Asset Groups:**
- 14+ assets in a single lineage view
- Hard to tell which asset belongs to which company
- Difficult to trace cross-location dependencies

**After Asset Groups:**
- Assets visually clustered by company
- Clear boundaries between code locations
- Easy to identify cross-company data flows
- Improved demo storytelling

### Viewing Asset Groups in the UI

1. **Global Asset Lineage** - Go to "Assets" → "View global asset lineage"
   - Assets are color-coded and clustered by group
   - Cross-group dependencies are clearly visible

2. **Asset Group Pages** - Click on any group name to see only that group's assets
   - Focus on a single company's pipelines
   - Understand a specific portfolio company's architecture

3. **Cross-Location Dependencies** - Look for edges between different colored clusters
   - All `shared_analytics` assets depend on assets from other groups
   - Demonstrates true multi-tenant collaboration

### Technical Implementation

All assets include the `group_name` parameter:

```python
@dg.asset(
    group_name="fintech_alpha",  # Matches code location name
    kinds={"s3", "aws", "data_lake"},
    description="Raw data ingested from S3 bucket"
)
def s3_raw_raw_customer_transactions(...):
    ...
```

---


## Getting Started

### Prerequisites

- Python 3.10-3.14
- [uv](https://docs.astral.sh/uv/) installed

### Installation

```bash
# Install all dependencies
uv sync

# This installs:
# - Root workspace package
# - All 4 code location packages
# - Dagster + integrations
```

### Running the Demo

**Option 1: Quick Start (Recommended)**
```bash
./run_demo.sh
```

This starts both:
- **Dagster Daemon** - Executes schedules, sensors, and automation conditions
- **Dagster Webserver** - UI at http://localhost:3000

Press `Ctrl+C` to stop both processes cleanly.

**Option 2: Manual Start (Separate Terminals)**

Terminal 1 - Start the daemon:
```bash
export DAGSTER_HOME="$(pwd)/.dagster_home"
mkdir -p "$DAGSTER_HOME"
uv run dagster-daemon run -w workspace.yaml
```

Terminal 2 - Start the webserver:
```bash
export DAGSTER_HOME="$(pwd)/.dagster_home"
uv run dagster-webserver -w workspace.yaml
# Navigate to http://localhost:3000
```

**Important Notes:**
- ⚠️ **Daemon is REQUIRED** for schedules and automation conditions to work
- ⚠️ **DAGSTER_HOME must be set** - The daemon needs this to store metadata (automatically handled by `run_demo.sh`)
- Do NOT use `dg dev` for multi-code location workspaces
- Always use `dagster-webserver -w workspace.yaml` and `dagster-daemon run -w workspace.yaml`

### Viewing Code Locations

In the Dagster UI:
1. Click the "Code Locations" tab
2. See all 5 locations (fintech_alpha, insurance_beta, healthcare_gamma, shared, shared_analytics)
3. Each location shows its assets independently

### Exploring Automation

In the Dagster UI:
1. **First, verify the daemon is running:**
   - Go to **"Deployment"** → Check that all daemons show "Running" status
   - If not running, make sure you started with `./run_demo.sh` or run `dagster-daemon run -w workspace.yaml`

2. Click **"Overview"** → **"Automation"** to see all schedules and automation conditions
3. See the daily/hourly schedules for portfolio companies
4. See automation conditions on shared_analytics assets (will show as "eager")
5. Enable the schedules if you want to test automation (they're off by default)

### Viewing Cross-Location Lineage

```bash
# 1. Go to Assets → "View global asset lineage"
# 2. You'll see the full dependency graph across all 5 code locations
# 3. Click on any shared_analytics asset to see its cross-location dependencies
# 4. Trace data flow: Portfolio Raw Data → Enrichment → ML → Consolidated → Executive Report
```

### Materializing Assets

```bash
# Materialize all assets for a portfolio company
# Or materialize individual assets
# When you materialize portfolio assets, shared_analytics will auto-trigger via automation conditions!
```

---

## Demo Mode

All components support **demo mode** (enabled by default):
- ✓ No external credentials required
- ✓ Realistic mock data generation
- ✓ Full pipeline execution locally
- ✓ Perfect for sales demos

To switch to production mode:
- Set `demo_mode: false` in component YAML files
- Provide actual connection credentials
- Components will execute real queries/jobs

---

## 🚀 Deploying to Dagster+

### Prerequisites

Before deploying, you need:
- A Dagster+ organization
- A Dagster+ API token (get from Dagster+ UI → Organization Settings → Tokens)

### Quick Deploy (All 5 Code Locations) - Serverless PEX Only

⚠️ **Important:** This deployment script only works for **Dagster+ Serverless with PEX builds**. If you're using Hybrid deployment or want Docker-based deployments, see the sections below.

```bash
# Set required environment variables
export DAGSTER_CLOUD_ORGANIZATION="your-org-name"
export DAGSTER_CLOUD_API_TOKEN="your-api-token"

# Deploy all 5 code locations
./deploy_to_dagster_plus.sh
```

This script deploys all 5 code locations to Dagster+ Serverless using **PEX builds**:
- `fintech-alpha` - Fintech pipeline
- `insurance-beta` - Insurance streaming pipeline
- `healthcare-gamma` - Healthcare pipeline
- `shared` - Shared utilities
- `shared-analytics` - Consolidated analytics

**PEX Build Benefits:**
- ✅ **Faster** - No Docker image build required
- ✅ **Simpler** - Direct Python executable deployment
- ✅ **Portable** - Self-contained Python environment
- ✅ **Perfect for demos** - Quick iterations

### Manual Deploy (Single Code Location) - Serverless PEX

If you need to deploy just one code location:

```bash
cd code_locations/fintech_alpha

dagster-cloud serverless deploy-python-executable \
    --organization YOUR_ORG \
    --api-token "YOUR_API_TOKEN" \
    --deployment prod \
    --location-file dagster_cloud.yaml \
    --location-name fintech-alpha
```

### Hybrid Deployment

For **Dagster+ Hybrid** (running on your own infrastructure):

1. **Set up Hybrid agent** in your infrastructure (Kubernetes, ECS, Docker, etc.)
   - Follow the [Hybrid deployment guide](https://docs.dagster.io/dagster-plus/deployment/hybrid)

2. **Deploy with PEX:**
```bash
cd code_locations/fintech_alpha

dagster-cloud hybrid deploy-python-executable \
    --organization YOUR_ORG \
    --api-token "YOUR_API_TOKEN" \
    --deployment prod \
    --location-file dagster_cloud.yaml \
    --location-name fintech-alpha
```

3. **Deploy with Docker:**
```bash
cd code_locations/fintech_alpha

# Build Docker image
docker build -t fintech-alpha:latest .

# Deploy to Dagster+ Hybrid
dagster-cloud hybrid deploy-docker \
    --organization YOUR_ORG \
    --api-token "YOUR_API_TOKEN" \
    --deployment prod \
    --location-file dagster_cloud.yaml \
    --location-name fintech-alpha \
    --image fintech-alpha:latest
```

### Serverless with Docker

For **Dagster+ Serverless with Docker images** (instead of PEX):

```bash
cd code_locations/fintech_alpha

# Build and push Docker image to a registry (ECR, GCR, DockerHub, etc.)
docker build -t your-registry/fintech-alpha:latest .
docker push your-registry/fintech-alpha:latest

# Deploy to Dagster+ Serverless
dagster-cloud serverless deploy-docker \
    --organization YOUR_ORG \
    --api-token "YOUR_API_TOKEN" \
    --deployment prod \
    --location-file dagster_cloud.yaml \
    --location-name fintech-alpha \
    --image your-registry/fintech-alpha:latest
```

### Configuration Files

Each code location has its own `dagster_cloud.yaml`:
- `code_locations/fintech_alpha/dagster_cloud.yaml`
- `code_locations/insurance_beta/dagster_cloud.yaml`
- `code_locations/healthcare_gamma/dagster_cloud.yaml`
- `code_locations/shared/dagster_cloud.yaml`
- `code_locations/shared_analytics/dagster_cloud.yaml`

### View Deployment

After deployment, view at:
```
https://YOUR_ORG.dagster.cloud/prod
```

---

## Extending the Demo

### Adding a New Portfolio Company

1. **Create code location directory:**
```bash
mkdir -p code_locations/retail_delta/retail_delta/{components,defs}
```

2. **Create pyproject.toml:**
```toml
[project]
name = "retail_delta"
dependencies = ["dagster==1.12.10", ...]

[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "retail_delta"
```

3. **Add to workspace.yaml:**
```yaml
load_from:
  - python_package:
      package_name: retail_delta
      location_name: retail_delta
      working_directory: code_locations/retail_delta
```

4. **Create assets & components**

5. **Sync and run:**
```bash
uv sync
uv run dagster-webserver -w workspace.yaml
```

### Adding New Technology Integrations

Example: Adding Fivetran integration
```bash
# Add to relevant code location's pyproject.toml
dependencies = [
    "dagster-fivetran",
    ...
]

# Create component using dagster-fivetran
# Update defs.yaml
```

---

## Project Structure

```
monorepo_example/
├── code_locations/
│   ├── fintech_alpha/
│   │   ├── fintech_alpha/
│   │   │   ├── components/          # Custom components
│   │   │   │   ├── snowflake_ingestion.py
│   │   │   │   ├── dbt_transformation.py
│   │   │   │   ├── databricks_analytics.py
│   │   │   │   └── azure_dw_aggregation.py
│   │   │   ├── defs/                # Component configurations
│   │   │   │   ├── snowflake_transactions/defs.yaml
│   │   │   │   ├── dbt_customer_enrichment/defs.yaml
│   │   │   │   ├── databricks_churn_prediction/defs.yaml
│   │   │   │   └── azure_business_metrics/defs.yaml
│   │   │   └── definitions.py
│   │   └── pyproject.toml
│   ├── insurance_beta/
│   │   ├── insurance_beta/
│   │   │   ├── components/
│   │   │   │   ├── streaming_claims_ingestion.py
│   │   │   │   ├── azure_dw_enrichment.py
│   │   │   │   └── fraud_detection.py
│   │   │   ├── defs/
│   │   │   │   ├── claims_stream/defs.yaml
│   │   │   │   ├── claims_enrichment/defs.yaml
│   │   │   │   └── fraud_analysis/defs.yaml
│   │   │   └── definitions.py
│   │   └── pyproject.toml
│   ├── healthcare_gamma/
│   │   ├── healthcare_gamma/
│   │   │   ├── defs/
│   │   │   │   └── patient_pipeline.py
│   │   │   └── definitions.py
│   │   └── pyproject.toml
│   └── shared/
│       ├── shared/
│       │   ├── defs/
│       │   │   └── shared_resources.py
│       │   └── definitions.py
│       └── pyproject.toml
├── workspace.yaml              # Multi-location configuration
├── pyproject.toml              # Root workspace config
└── README.md                   # This file
```

---

## Support & Resources

### Dagster Documentation
- [Dagster Docs](https://docs.dagster.io/)
- [Multi-Code Location Guide](https://docs.dagster.io/concepts/code-locations)
- [Component Library](https://docs.dagster.io/guides/build/components)

---

## Next Steps

1. **Run the demo** - `./run_demo.sh` or `uv run dagster-webserver -w workspace.yaml` (Navigate to http://localhost:3000)
2. **Explore code locations** - Review each business unit's setup in the UI
3. **Materialize assets** - See the data lineage in action
4. **View automation** - Check schedules, sensors, and automation conditions

---

**Built with Dagster 1.12.10**

For questions or support, please refer to the Dagster documentation.

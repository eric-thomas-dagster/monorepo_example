# Quick Start Guide

## Run the Demo

```bash
# Install dependencies
uv sync

# Start the demo
./run_demo.sh

# Open http://localhost:3000 in your browser
```

## What You'll See

**5 Code Locations** representing different business units:

1. **fintech_alpha** - Fintech company with Snowflake → dbt → Databricks → Azure pipeline
2. **insurance_beta** - Insurance company with streaming claims → enrichment → fraud detection
3. **healthcare_gamma** - Healthcare provider with EHR → HIPAA compliance → analytics
4. **shared** - Common utilities and resources
5. **shared_analytics** - 🔗 **Cross-company analytics with lineage across all companies!**

**14 Total Assets** with **cross-location dependencies** demonstrating:
- Multi-cloud orchestration (AWS, Azure)
- Batch + streaming workloads
- ML pipelines
- Compliance automation
- Unified observability

## Key Features to Demo

✅ **Single deployment** managing multiple customers
✅ **Airflow replacement** with modern asset-centric approach
✅ **Cross-company lineage** and observability
✅ **Reusable components** for rapid onboarding
✅ **Demo mode** - runs locally without credentials
✅ **🤖 Smart automation** - shared_analytics automatically runs when data is ready

## Common Commands

```bash
# Start the UI
./run_demo.sh
# OR
uv run dagster-webserver -w workspace.yaml

# Validate workspace (optional)
uv run python validate_workspace.py
```

## See Automation in Action

1. **Go to Overview → Automation** to see schedules and automation conditions
2. **Materialize a portfolio company asset** (e.g., from fintech_alpha)
3. **Watch shared_analytics assets automatically trigger** via automation conditions
4. **Check the run history** to see the cascade effect

Example flow:
```
You materialize: metrics_business_metrics (fintech_alpha)
     ↓
Auto-triggers: consolidated_portfolio_metrics (shared_analytics)
     ↓
Auto-triggers: executive_summary_report (shared_analytics)
```

This demonstrates **data-driven orchestration** - no manual coordination needed!

## Important Notes

⚠️ **Do NOT use `dg dev`** - this is a multi-code location workspace, use `dagster-webserver -w workspace.yaml` instead

✅ **All assets run in demo mode** by default - no external credentials needed

📖 **Full documentation** in [README.md](README.md)

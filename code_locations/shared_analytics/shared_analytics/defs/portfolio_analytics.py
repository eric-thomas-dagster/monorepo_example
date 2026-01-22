"""Consolidated analytics across all business units.

This module demonstrates cross-code location lineage by creating assets that depend
on assets from fintech_alpha, insurance_beta, and healthcare_gamma.
"""

import dagster as dg
import pandas as pd


@dg.asset(
    group_name="shared_analytics",
    kinds={"analytics", "python"},
    deps=[
        dg.AssetKey("metrics_business_metrics"),  # From fintech_alpha
        dg.AssetKey("fraud_predictions_claims_fraud"),  # From insurance_beta
        dg.AssetKey("population_health_analytics"),  # From healthcare_gamma
    ],
    description="Consolidated portfolio metrics across all business units",
    metadata={
        "owner": "Ness Digital Engineering",
        "portfolio": "Enterprise",
        "data_sources": ["fintech_alpha", "insurance_beta", "healthcare_gamma"]
    },
    automation_condition=dg.AutomationCondition.eager()
    # Automatically materializes when ALL parent assets from the 3 portfolio companies are updated
)
def consolidated_portfolio_metrics(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Aggregate metrics from all portfolio companies into unified dashboard.

    This asset demonstrates cross-code location lineage by depending on assets
    from three different code locations (fintech_alpha, insurance_beta, healthcare_gamma).
    """
    context.log.info("Consolidating metrics from all portfolio companies")

    # In demo mode, generate consolidated metrics
    # In production, this would fetch data from upstream assets
    return pd.DataFrame({
        "portfolio_company": ["Fintech Alpha", "Insurance Beta", "Healthcare Gamma"],
        "total_revenue_ytd": [45_200_000, 38_900_000, 52_100_000],
        "customer_count": [5_400, 15_200, 8_900],
        "data_quality_score": [0.94, 0.91, 0.96],
        "pipeline_success_rate": [0.98, 0.96, 0.99],
        "avg_processing_time_mins": [12.3, 18.7, 9.4],
        "risk_score": [0.23, 0.31, 0.18],
        "quarter": ["2024-Q1"] * 3,
    })


@dg.asset(
    group_name="shared_analytics",
    kinds={"analytics", "ml", "python"},
    deps=[
        dg.AssetKey("analytics_churn_prediction"),  # From fintech_alpha
        dg.AssetKey("fraud_predictions_claims_fraud"),  # From insurance_beta
    ],
    description="Cross-company risk dashboard combining churn and fraud analytics",
    metadata={
        "owner": "Ness Digital Engineering - Risk Team",
        "dashboard_url": "https://analytics.example.com/risk",
    },
    automation_condition=dg.AutomationCondition.eager()
    # Auto-runs when ML predictions from fintech or insurance are updated
)
def cross_company_risk_dashboard(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Unified risk dashboard combining ML predictions from multiple companies.

    Combines:
    - Churn predictions from fintech_alpha
    - Fraud predictions from insurance_beta

    This demonstrates how organizations can provide consolidated risk analytics across
    the entire enterprise portfolio.
    """
    context.log.info("Building cross-company risk dashboard")

    return pd.DataFrame({
        "company": ["Fintech Alpha", "Insurance Beta"],
        "high_risk_customers": [324, 892],
        "medium_risk_customers": [1_240, 3_156],
        "low_risk_customers": [3_836, 11_152],
        "ml_model_confidence": [0.87, 0.82],
        "predicted_losses_30d": [125_000, 430_000],
        "recommended_action": ["MONITOR", "INVESTIGATE"],
        "last_updated": [pd.Timestamp.now()] * 2,
    })


@dg.asset(
    group_name="shared_analytics",
    kinds={"reporting", "executive", "python"},
    deps=[
        "consolidated_portfolio_metrics",
        "cross_company_risk_dashboard",
    ],
    description="Executive summary report for leadership",
    metadata={
        "owner": "Ness Digital Engineering - Executive Team",
        "recipients": ["Executive Leadership", "Business Unit CEOs", "Analytics Team"],
        "frequency": "Weekly",
    },
    automation_condition=dg.AutomationCondition.eager()
    # Automatically generates report when consolidated metrics and risk dashboard are ready
)
def executive_summary_report(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Weekly executive summary combining all portfolio analytics.

    This top-level report depends on consolidated metrics and risk dashboards,
    demonstrating the full lineage chain:

    Raw Data (fintech/insurance/healthcare)
        → Enriched Data
        → ML Predictions
        → Consolidated Analytics
        → Executive Report
    """
    context.log.info("Generating executive summary report")

    return pd.DataFrame({
        "metric": [
            "Total Portfolio Revenue",
            "Total Customers",
            "High Risk Alerts",
            "Data Pipeline Health",
            "Cross-Sell Opportunities",
            "Regulatory Compliance Score",
        ],
        "value": [
            "$136.2M",
            "29,500",
            "1,216",
            "97%",
            "847",
            "94%",
        ],
        "trend": ["↑ 12%", "↑ 8%", "↓ 5%", "→ Stable", "↑ 23%", "↑ 2%"],
        "status": ["🟢 On Track", "🟢 On Track", "🟡 Monitor", "🟢 Healthy", "🟢 Growing", "🟢 Compliant"],
    })


@dg.asset(
    group_name="shared_analytics",
    kinds={"analytics", "ml", "python"},
    deps=[
        dg.AssetKey("transformed_customer_enrichment"),  # From fintech_alpha
        dg.AssetKey("enriched_claims"),  # From insurance_beta
        dg.AssetKey("hipaa_compliant_records"),  # From healthcare_gamma
    ],
    description="Cross-sell opportunities identified across portfolio companies",
    metadata={
        "owner": "Ness Digital Engineering - Growth Team",
        "business_value": "Identify customers who could benefit from products in other portfolio companies",
    },
    automation_condition=dg.AutomationCondition.eager()
    # Identifies cross-sell opportunities whenever customer data from any company is updated
)
def cross_sell_opportunities(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Identify cross-sell opportunities across portfolio companies.

    Analyzes customer data from all three companies to find potential
    cross-sell opportunities, e.g.:
    - Fintech customers who need insurance products
    - Insurance customers who need healthcare services
    - Healthcare patients who need financial planning
    """
    context.log.info("Analyzing cross-sell opportunities across portfolio")

    return pd.DataFrame({
        "customer_segment": [
            "High Net Worth - Fintech",
            "Families - Insurance",
            "Seniors - Healthcare",
            "Small Business - Fintech",
        ],
        "opportunity": [
            "Life Insurance (Insurance Beta)",
            "Healthcare Plans (Healthcare Gamma)",
            "Retirement Planning (Fintech Alpha)",
            "Business Insurance (Insurance Beta)",
        ],
        "estimated_customers": [1_240, 3_890, 2_450, 890],
        "estimated_revenue": [2_480_000, 1_950_000, 4_900_000, 1_780_000],
        "confidence": [0.78, 0.82, 0.91, 0.71],
    })

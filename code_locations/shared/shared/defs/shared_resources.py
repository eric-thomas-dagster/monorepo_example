"""Shared resources and utilities available to all portfolio companies."""

import dagster as dg


@dg.resource(description="Shared data quality monitoring resource")
def data_quality_monitor():
    """Common data quality monitoring resource used across all portfolio companies."""
    return {"monitoring": "enabled", "threshold": 0.95}


@dg.resource(description="Shared alert notifier for critical issues")
def alert_notifier():
    """Common alerting resource for critical pipeline issues."""
    return {"slack_webhook": "https://hooks.slack.com/services/YOUR_ALERTS", "enabled": True}

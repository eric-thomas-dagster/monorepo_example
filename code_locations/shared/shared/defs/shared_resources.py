"""Shared resources and utilities available to all code locations.

This module provides common resources that can be imported and used across
multiple code locations, demonstrating the shared utilities pattern in
multi-location Dagster deployments.
"""

import dagster as dg
from typing import Any


@dg.resource
def data_quality_monitor(context: dg.InitResourceContext) -> dict[str, Any]:
    """Common data quality monitoring resource used across all code locations.

    This resource provides centralized data quality checks and monitoring,
    ensuring consistent quality standards across all business units.
    """
    quality_threshold = 0.95
    enable_alerts = True

    context.log.info(
        f"Data quality monitor initialized with threshold: {quality_threshold}"
    )
    return {
        "quality_threshold": quality_threshold,
        "enable_alerts": enable_alerts,
        "monitor": lambda df: len(df) > 0,  # Simple quality check
    }


@dg.resource
def alert_notifier(context: dg.InitResourceContext) -> dict[str, Any]:
    """Common alerting resource for critical pipeline issues.

    Provides centralized alerting across all code locations. In production,
    this would send notifications to Slack, PagerDuty, etc.
    """
    enable_notifications = False  # Disabled by default for demo

    def send_alert(message: str, severity: str = "info"):
        """Send an alert notification."""
        if enable_notifications:
            context.log.info(f"[ALERT - {severity.upper()}] {message}")
            # In production: send to Slack, PagerDuty, etc.
        else:
            context.log.debug(f"Alert (notifications disabled): {message}")

    return {
        "send_alert": send_alert,
        "enabled": enable_notifications,
    }


def log_asset_metadata(context: dg.AssetExecutionContext, df) -> None:
    """Utility function to log common asset metadata.

    This shared utility can be imported and used by any code location
    to provide consistent metadata logging.
    """
    context.log.info(f"Asset materialized with {len(df)} rows")
    context.add_output_metadata({
        "num_rows": len(df),
        "preview": dg.MetadataValue.md(df.head().to_markdown()),
    })

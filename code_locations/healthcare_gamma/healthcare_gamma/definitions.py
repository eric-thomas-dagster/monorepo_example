from pathlib import Path

from dagster import Definitions, load_from_defs_folder
from shared import data_quality_monitor, alert_notifier


defs = Definitions.merge(
    load_from_defs_folder(path_within_project=Path(__file__).parent),
    Definitions(
        resources={
            "data_quality_monitor": data_quality_monitor,
            "alert_notifier": alert_notifier,
        }
    )
)

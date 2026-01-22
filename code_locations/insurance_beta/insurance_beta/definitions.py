from pathlib import Path

from dagster import Definitions, load_from_defs_folder


defs = Definitions.merge(
    load_from_defs_folder(path_within_project=Path(__file__).parent)
)

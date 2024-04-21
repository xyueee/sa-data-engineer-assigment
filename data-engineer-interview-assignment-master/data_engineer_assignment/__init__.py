from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "source_database": DuckDBResource(database="db/source.db"),
        "data_warehouse": DuckDBResource(database="db/dwh.db")
    }
)

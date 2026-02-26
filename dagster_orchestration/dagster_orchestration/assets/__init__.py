import os

from dagster import AssetExecutionContext
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance
from dagster_dbt import DbtCliResource, dbt_assets as dbt_assets_decorator

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
DBT_MANIFEST_PATH = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")

AIRBYTE_HOST = os.getenv("AIRBYTE_HOST")
AIRBYTE_PORT = os.getenv("AIRBYTE_PORT")
AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD")

resources = {
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ),
    "airbyte_instance": AirbyteResource(
        host=AIRBYTE_HOST,
        port=AIRBYTE_PORT,
        username=AIRBYTE_USERNAME,
        password=AIRBYTE_PASSWORD,
    )
}

@dbt_assets_decorator(
    manifest=DBT_MANIFEST_PATH,
)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

airbyte_assets = (
    load_assets_from_airbyte_instance(resources["airbyte_instance"], key_prefix=["raw_data"])
)
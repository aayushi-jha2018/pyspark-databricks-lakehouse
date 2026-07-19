"""
Shared SparkSession factory.

All pipeline stages (bronze, silver, gold) pull their SparkSession from here
so that configuration lives in exactly one place.

Note on Delta Lake: on an actual Databricks workspace, every write below
would target Delta tables (`.format("delta")`) and you'd get ACID commits,
time travel, and MERGE/upsert support for free. Here, the same bronze ->
silver -> gold logic writes Parquet instead - see the README's design notes
for why. The transformation code itself is identical either way; only the
write format would change.
"""
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "pyspark-databricks-lakehouse") -> SparkSession:
    """Build a local SparkSession suitable for running or testing the pipeline."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

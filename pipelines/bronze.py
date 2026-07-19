"""
Bronze layer: raw ingestion.

Reads the raw source file(s) as-is (schema-on-read, minimal casting) and
writes them straight through to storage. The goal of this layer is
traceability: whatever landed in the source system is what lands here,
including duplicates, nulls, and malformed values, so later layers can
always be recomputed from scratch without going back to the source system.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Every column is read as a string on purpose. Bronze should never fail or
# silently coerce a bad value just because a source system sent something
# unexpected (e.g. "N/A" in a quantity column) - that decision belongs to
# the silver layer, where we can log/reject it explicitly.
RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("region", StringType(), True),
])


def read_raw_orders(spark: SparkSession, path: str) -> DataFrame:
    """Read the raw orders CSV using a fixed, permissive (all-string) schema."""
    return (
        spark.read.option("header", True)
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def write_bronze(df: DataFrame, path: str) -> None:
    """Persist the raw dataframe to the bronze area, append-only."""
    df.write.format("parquet").mode("append").save(path)


def run_bronze(spark: SparkSession, source_path: str, bronze_path: str) -> DataFrame:
    """Read the source CSV and land it in bronze. Returns the raw dataframe."""
    raw_df = read_raw_orders(spark, source_path)
    write_bronze(raw_df, bronze_path)
    return raw_df

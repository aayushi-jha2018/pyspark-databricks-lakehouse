"""
Silver layer: cleaning and conformance.

Takes the raw bronze data and produces a de-duplicated, type-cast,
null-filtered dataset. Anything that depends on "is this row usable" lives
here; anything that depends on aggregation belongs in gold.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def clean_orders(bronze_df: DataFrame) -> DataFrame:
    """Cast types, drop unusable rows, and de-duplicate by order_id."""
    typed = (
        bronze_df
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
    )

    valid = typed.where(
        F.col("order_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0)
    )

    deduped = valid.dropDuplicates(["order_id"])

    return deduped.withColumn("order_total", F.col("quantity") * F.col("unit_price"))


def write_silver(df: DataFrame, path: str) -> None:
    """Overwrite the silver table with the freshly cleaned dataset."""
    df.write.format("parquet").mode("overwrite").save(path)


def run_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> DataFrame:
    bronze_df = spark.read.format("parquet").load(bronze_path)
    silver_df = clean_orders(bronze_df)
    write_silver(silver_df, silver_path)
    return silver_df

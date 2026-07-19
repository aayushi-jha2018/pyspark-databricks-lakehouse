"""
Gold layer: business aggregates.

Produces the metrics that downstream consumers (dashboards, analysts)
actually query, e.g. revenue by region and by product. Consumers should
query gold only - bronze and silver are implementation detail.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def revenue_by_region(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("region")
        .agg(
            F.sum("order_total").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def revenue_by_product(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("product")
        .agg(
            F.sum("order_total").alias("total_revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def write_gold(df: DataFrame, path: str) -> None:
    df.write.format("parquet").mode("overwrite").save(path)


def run_gold(
    spark: SparkSession,
    silver_path: str,
    gold_region_path: str,
    gold_product_path: str,
) -> None:
    silver_df = spark.read.format("parquet").load(silver_path)
    write_gold(revenue_by_region(silver_df), gold_region_path)
    write_gold(revenue_by_product(silver_df), gold_product_path)

"""
CLI entry point: runs the full bronze -> silver -> gold pipeline against a
local directory, so you can see the whole thing work end to end without
needing an actual Databricks workspace.

Usage:
    python main.py --source data/sample/orders_raw.csv --output-dir lake --reset
"""
import argparse
import shutil
from pathlib import Path

from pipelines.spark_session import get_spark_session
from pipelines.bronze import run_bronze
from pipelines.silver import run_silver
from pipelines.gold import run_gold, revenue_by_region, revenue_by_product


def parse_args():
    parser = argparse.ArgumentParser(description="Run the bronze/silver/gold demo pipeline.")
    parser.add_argument("--source", default="data/sample/orders_raw.csv")
    parser.add_argument("--output-dir", default="lake")
    parser.add_argument("--reset", action="store_true", help="Delete any existing lake directory first.")
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    if args.reset and output_dir.exists():
        shutil.rmtree(output_dir)

    bronze_path = str(output_dir / "bronze" / "orders")
    silver_path = str(output_dir / "silver" / "orders")
    gold_region_path = str(output_dir / "gold" / "revenue_by_region")
    gold_product_path = str(output_dir / "gold" / "revenue_by_product")

    spark = get_spark_session()
    try:
        run_bronze(spark, args.source, bronze_path)
        silver_df = run_silver(spark, bronze_path, silver_path)
        run_gold(spark, silver_path, gold_region_path, gold_product_path)

        print("\nRevenue by region:")
        revenue_by_region(silver_df).show(truncate=False)

        print("Revenue by product:")
        revenue_by_product(silver_df).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

# pyspark-databricks-lakehouse

A batch data pipeline built with PySpark, structured as a Databricks-style
medallion architecture: bronze (raw), silver (cleaned/conformed), and gold
(business-ready aggregates). It's designed to demonstrate how I structure
Spark pipelines on Databricks - schema-on-read ingestion, explicit data
quality rules, and a clean separation between "what landed" and "what the
business queries."

## The problem this models

A typical Databricks ingestion job pulls orders from an upstream system on a
schedule. The raw feed is messy in realistic ways: duplicate rows from
at-least-once delivery, missing quantities, negative values from source bugs,
and occasional nulls in optional fields. The pipeline needs to preserve the
raw feed untouched (for audits and reprocessing), clean it into something
trustworthy, and then produce the aggregates analysts actually use - without
mixing those three concerns into one script.

## Layout

```
pipelines/
  spark_session.py   # shared SparkSession factory
  bronze.py          # raw ingestion (schema-on-read, append-only)
  silver.py          # type casting, validation, de-duplication
  gold.py            # revenue_by_region / revenue_by_product aggregates
data/sample/
  orders_raw.csv     # small sample feed with intentionally dirty rows
tests/
  conftest.py        # session-scoped SparkSession fixture
  test_bronze.py
  test_silver.py
  test_gold.py
main.py              # CLI: runs bronze -> silver -> gold end to end
```

## Setup

```bash
python -m venv venv
source venv/bin/activate  # venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Requires a JDK (17 works; the CI workflow uses Temurin 17) since PySpark
runs on the JVM under the hood.

## Usage

Run the full pipeline against the bundled sample data:

```bash
python main.py --source data/sample/orders_raw.csv --output-dir lake --reset
```

This lands data through bronze, silver, and gold under `lake/`, then prints
the two gold aggregates to the console.

Run the tests:

```bash
python -m pytest tests/ -v
```

## What's happening under the hood

Bronze reads the CSV with every column typed as a string and a fixed schema,
then appends it as-is. Nothing is cast, dropped, or validated here - the
point of bronze is that it can be replayed at any time and will always match
what the source system sent.

Silver casts `quantity` and `unit_price` to numeric types, parses
`order_ts`, then filters out rows that fail basic sanity checks (missing or
non-positive quantity, missing or negative price) and de-duplicates on
`order_id`. It's the layer responsible for deciding what's "real."

Gold aggregates the silver data into the two views the business actually
consumes: revenue and order counts by region, and revenue and units sold by
product.

## Design notes

**Why Parquet instead of Delta Lake tables.** The architecture here follows
Delta Lake's medallion pattern (bronze/silver/gold), and on an actual
Databricks workspace every one of these writes would target Delta tables to
get ACID commits, time travel, and MERGE-based upserts. Locally, delta-spark
resolves its Maven package over the network on every fresh environment,
which added a few minutes to every CI run and occasionally failed outright
depending on Maven Central availability that day. Since the transformation
logic is identical either way, I kept the demo on Parquet for a fast, fully
reproducible CI run, and left the Delta migration as a one-line format
change (`write.format("delta")` instead of `"parquet"`) - noted inline in
`pipelines/spark_session.py`.

**Why schema-on-read, all-string, at bronze.** Casting early means a single
malformed value from the source can crash ingestion. Reading everything as
a string and pushing casting/validation into silver means bronze can never
fail because of bad data - it can only fail because of infrastructure, which
is the property you actually want from a raw ingestion layer.

**Why a session-scoped Spark fixture in tests.** Starting a SparkSession
means starting a JVM, which is the slowest part of any PySpark test suite by
a wide margin. Sharing one session across all tests (via a `scope="session"`
pytest fixture) keeps the whole suite running in seconds instead of minutes.

## Production notes

On Databricks this would run as a scheduled Job, with bronze/silver/gold as
separate tasks so a silver failure doesn't force bronze to re-ingest, Delta
tables in place of the Parquet paths used here, and Unity Catalog managing
access to each layer (typically: only the platform team writes bronze/silver,
analysts get read access to gold only). The de-duplication logic in silver
would become a MERGE statement keyed on `order_id` rather than
`dropDuplicates`, so re-running a day's ingestion doesn't require
recomputing the whole table.

## License

MIT

from pipelines.silver import clean_orders

COLUMNS = ["order_id", "customer_id", "product", "quantity", "unit_price", "order_ts", "region"]


def test_clean_orders_drops_invalid_rows(spark):
    rows = [
        ("ORD1", "C1", "Widget", "2", "9.99", "2026-01-01 10:00:00", "US-East"),
        ("ORD2", "C2", "Widget", None, "9.99", "2026-01-01 10:00:00", "US-East"),
        ("ORD3", "C3", "Widget", "-1", "9.99", "2026-01-01 10:00:00", "US-East"),
        ("ORD4", "C4", "Widget", "1", None, "2026-01-01 10:00:00", "US-East"),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    cleaned = clean_orders(df)

    assert cleaned.count() == 1
    assert cleaned.collect()[0].order_id == "ORD1"


def test_clean_orders_deduplicates_by_order_id(spark):
    rows = [
        ("ORD1", "C1", "Widget", "2", "9.99", "2026-01-01 10:00:00", "US-East"),
        ("ORD1", "C1", "Widget", "2", "9.99", "2026-01-01 10:00:00", "US-East"),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    cleaned = clean_orders(df)

    assert cleaned.count() == 1


def test_clean_orders_computes_order_total(spark):
    rows = [("ORD1", "C1", "Widget", "3", "10.00", "2026-01-01 10:00:00", "US-East")]
    df = spark.createDataFrame(rows, COLUMNS)

    cleaned = clean_orders(df)

    assert cleaned.collect()[0].order_total == 30.0

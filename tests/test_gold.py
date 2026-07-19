from pipelines.gold import revenue_by_region, revenue_by_product

COLUMNS = ["order_id", "customer_id", "product", "quantity", "unit_price", "region", "order_total"]


def _silver_df(spark):
    rows = [
        ("ORD1", "C1", "Widget", 2, 10.0, "US-East", 20.0),
        ("ORD2", "C2", "Gadget", 1, 50.0, "US-East", 50.0),
        ("ORD3", "C3", "Widget", 1, 10.0, "EU-Central", 10.0),
    ]
    return spark.createDataFrame(rows, COLUMNS)


def test_revenue_by_region_sums_correctly(spark):
    df = _silver_df(spark)

    result = {row.region: row.total_revenue for row in revenue_by_region(df).collect()}

    assert result["US-East"] == 70.0
    assert result["EU-Central"] == 10.0


def test_revenue_by_product_sums_units_and_revenue(spark):
    df = _silver_df(spark)

    result = {row.product: (row.total_revenue, row.units_sold) for row in revenue_by_product(df).collect()}

    assert result["Widget"] == (30.0, 3)
    assert result["Gadget"] == (50.0, 1)

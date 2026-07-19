from pipelines.bronze import read_raw_orders, RAW_SCHEMA


def test_read_raw_orders_uses_fixed_schema(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    csv_path.write_text(
        "order_id,customer_id,product,quantity,unit_price,order_ts,region\n"
        "ORD1,CUST1,Widget,2,9.99,2026-01-01 10:00:00,US-East\n"
    )

    df = read_raw_orders(spark, str(csv_path))

    assert df.schema == RAW_SCHEMA
    assert df.count() == 1


def test_read_raw_orders_keeps_malformed_rows(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    csv_path.write_text(
        "order_id,customer_id,product,quantity,unit_price,order_ts,region\n"
        "ORD1,CUST1,Widget,not_a_number,9.99,2026-01-01 10:00:00,US-East\n"
    )

    df = read_raw_orders(spark, str(csv_path))

    # bronze is schema-on-read only - malformed numeric strings are kept
    # as-is (still strings here) rather than dropped, so nothing is lost
    row = df.collect()[0]
    assert row.quantity == "not_a_number"

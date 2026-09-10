from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.helpers import bronze_table


@dp.temporary_view(name="orders_change_feed", comment="Filtered order CDC records for Auto CDC")
def orders_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_orders_valid"))
        .filter(F.col("after").isNotNull())
        .select("after.*", "op", "ts_ms")
        .withColumn("order_date", F.col("order_date").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )


ORDERS_RULES = {
    "valid_order_status": "order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'returned', 'canceled')",
    "discount_consistency": "(discount_code IS NULL AND discount_amount IS NULL) OR (discount_code IS NOT NULL AND discount_amount IS NOT NULL AND discount_amount > 0)",
}


dp.create_streaming_table(
    name="silver_orders",
    comment="Silver SCD Type 2 table for orders",
    expect_all=ORDERS_RULES,
)


dp.create_auto_cdc_flow(
    target="silver_orders",
    source="orders_change_feed",
    keys=["order_id"],
    sequence_by="ts_ms",
    apply_as_deletes="op = 'd'",
    except_column_list=["op"],
    stored_as_scd_type=2,
)


@dp.temporary_view(name="order_items_change_feed", comment="Filtered order item CDC records for Auto CDC")
def order_items_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_order_items_valid"))
        .filter(F.col("after").isNotNull())
        .select("after.*", "op", "ts_ms")
    )


ORDER_ITEMS_RULES = {
    "valid_order_item_id": "order_item_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "positive_unit_price": "unit_price > 0",
    "positive_line_total": "line_total > 0",
    "return_consistency": "(return_requested = false AND return_reason IS NULL) OR (return_requested = true AND return_reason IS NOT NULL)",
    "line_total_matches":"ABS(line_total - (quantity * unit_price)) <= 0.01"
}

ORDER_ITEMS_QUARANTINE_RULE = "NOT (line_total_matches)"

dp.create_streaming_table(
    name="silver_order_items_all",
    comment="Silver current-state CDC table for order items",
    expect_all=ORDER_ITEMS_RULES,
)


dp.create_auto_cdc_flow(
    target="silver_order_items_all",
    source="order_items_change_feed",
    keys=["order_item_id"],
    sequence_by="ts_ms",
    apply_as_deletes="op = 'd'",
    stored_as_scd_type=1,
)


@dp.materialized_view(private=True)
def silver_order_items_quality_check():
    return (
        spark.read.table("silver_order_items_all")
        .withColumn("line_total_matches", F.expr(ORDER_ITEMS_RULES["line_total_matches"]))
        .withColumn("is_quarantined", F.expr(ORDER_ITEMS_QUARANTINE_RULE))
    )



@dp.materialized_view(
    name="silver_order_items",
    comment="Order items, current state, line_total verified. This is the table Gold reads — "
    "plain name, same convention as every other Silver table.",
    schema="""
        order_item_id STRING NOT NULL,
        order_id STRING,
        product_id STRING,
        sku STRING,
        quantity LONG,
        unit_price DOUBLE,
        line_total DOUBLE,
        return_requested BOOLEAN,
        return_reason STRING,
        op STRING,
        ts_ms LONG,
        CONSTRAINT pk_silver_order_items PRIMARY KEY (order_item_id)
    """,
)

def silver_order_items():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined=false")
        .drop("line_total_matches", "is_quarantined")
    )

@dp.materialized_view(
    name="silver_order_items_quarantined",
    comment="Order items where line_total didn't match quantity * unit_price. "
    "Investigation surface — monitored in L16, never hand-edited (see remediation pattern).",
)
def silver_order_items_quarantined():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined = true")
        .drop("line_total_matches", "is_quarantined")
    )

# 
@dp.temporary_view(name="customers_change_feed", comment="Filtered customer CDC records for Auto CDC")
def customers_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_customers_valid"))
        .filter(F.col("after").isNotNull())
        .select("after.*", "op", "ts_ms")
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("registration_date", F.col("registration_date").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )


CUSTOMERS_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email": "email IS NOT NULL AND email LIKE '%@%'",
    "customer_min_age":"months_between(current_date(),date_of_birth) >=18 * 12",
    "valid_loyalty_tier":"loyalty_tier IN ('bronze', 'silver', 'gold','platinum')"
}


dp.create_streaming_table(
    name="silver_customers",
    comment="Silver SCD Type 2 table for customers",
    expect_all=CUSTOMERS_RULES,
)


dp.create_auto_cdc_flow(
    target="silver_customers",
    source="customers_change_feed",
    keys=["customer_id"],
    sequence_by="ts_ms",
    apply_as_deletes="op = 'd'",
    except_column_list=["op"],
    stored_as_scd_type=2,
)
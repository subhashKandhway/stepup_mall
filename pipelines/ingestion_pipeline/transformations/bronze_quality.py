from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDERS_RULES = {
    "valid_order_id": "after.order_id IS NOT NULL",
    "valid_customer_ref": "after.customer_id IS NOT NULL",
    "valid_total_amount": "after.total_amount IS NOT NULL AND after.total_amount >= 0",
}

ORDER_ITEMS_RULES = {
    "valid_order_item_id": "after.order_item_id IS NOT NULL",
    "valid_order_ref": "after.order_id IS NOT NULL",
    "valid_product_ref": "after.product_id IS NOT NULL",
    "valid_quantity": "after.quantity IS NOT NULL AND after.quantity >= 0",
    "valid_unit_price": "after.unit_price IS NOT NULL AND after.unit_price >= 0",
    "valid_line_total": "after.line_total IS NOT NULL AND after.line_total >= 0",
}

CUSTOMERS_RULES = {
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_email": "after.email IS NOT NULL",
}

PRODUCTS_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_category_ref": "category_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "valid_product_name": "product_name IS NOT NULL",
    "valid_cost_price": "cost_price IS NOT NULL AND cost_price >= 0",
    "valid_retail_price": "retail_price IS NOT NULL AND retail_price >= 0",
}

CATEGORIES_RULES = {
    "valid_category_id": "category_id IS NOT NULL",
    "valid_category_name": "category_name IS NOT NULL",
}

INVENTORY_RULES = {
    "valid_snapshot_id": "snapshot_id IS NOT NULL",
    "valid_snapshot_date": "snapshot_date IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_warehouse_ref": "warehouse_id IS NOT NULL",
    "valid_quantity_on_hand": "quantity_on_hand IS NOT NULL AND quantity_on_hand >= 0",
    "valid_quantity_reserved": "quantity_reserved IS NOT NULL AND quantity_reserved >= 0",
    "valid_quantity_available": "quantity_available IS NOT NULL AND quantity_available >= 0",
}

CLICKSTREAM_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_session_id": "session_id IS NOT NULL",
}


def _quarantine_rule(rules: dict) -> str:
    if not rules:
        return "false"

    valid_rules = " AND ".join(f"({rule})" for rule in rules.values())
    return f"NOT ({valid_rules})"


def _with_quarantine_flag(source_table: str, rules: dict):
    return spark.readStream.table(source_table).withColumn(
        "is_quarantined",
        F.expr(_quarantine_rule(rules)),
    )


@dp.table(private=True,
          name="bronze_orders_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(ORDERS_RULES)
def bronze_orders_quality_check():
    return _with_quarantine_flag("bronze_orders", ORDERS_RULES)


@dp.table(name="bronze_orders_valid",
          comment="Valid orders from bronze_orders table",
)
def bronze_orders_valid():
    return spark.readStream.table("bronze_orders_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_orders_quarantined",
          comment="Orders that failed at least one structural quality check. Published",
)
def bronze_orders_quarantined():
    return spark.readStream.table("bronze_orders_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_order_items_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(ORDER_ITEMS_RULES)
def bronze_order_items_quality_check():
    return _with_quarantine_flag("bronze_order_items", ORDER_ITEMS_RULES)


@dp.table(name="bronze_order_items_valid",
          comment="Valid order items from bronze_order_items table",
)
def bronze_order_items_valid():
    return spark.readStream.table("bronze_order_items_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_order_items_quarantined",
          comment="Order items that failed at least one structural quality check. Published",
)
def bronze_order_items_quarantined():
    return spark.readStream.table("bronze_order_items_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_customers_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(CUSTOMERS_RULES)
def bronze_customers_quality_check():
    return _with_quarantine_flag("bronze_customers", CUSTOMERS_RULES)


@dp.table(name="bronze_customers_valid",
          comment="Valid customers from bronze_customers table",
)
def bronze_customers_valid():
    return spark.readStream.table("bronze_customers_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_customers_quarantined",
          comment="Customers that failed at least one structural quality check. Published",
)
def bronze_customers_quarantined():
    return spark.readStream.table("bronze_customers_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_products_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(PRODUCTS_RULES)
def bronze_products_quality_check():
    return _with_quarantine_flag("bronze_products", PRODUCTS_RULES)


@dp.table(name="bronze_products_valid",
          comment="Valid products from bronze_products table",
)
def bronze_products_valid():
    return spark.readStream.table("bronze_products_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_products_quarantined",
          comment="Products that failed at least one structural quality check. Published",
)
def bronze_products_quarantined():
    return spark.readStream.table("bronze_products_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_categories_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(CATEGORIES_RULES)
def bronze_categories_quality_check():
    return _with_quarantine_flag("bronze_categories", CATEGORIES_RULES)


@dp.table(name="bronze_categories_valid",
          comment="Valid categories from bronze_categories table",
)
def bronze_categories_valid():
    return spark.readStream.table("bronze_categories_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_categories_quarantined",
          comment="Categories that failed at least one structural quality check. Published",
)
def bronze_categories_quarantined():
    return spark.readStream.table("bronze_categories_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_inventory_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(INVENTORY_RULES)
def bronze_inventory_quality_check():
    return _with_quarantine_flag("bronze_inventory", INVENTORY_RULES)


@dp.table(name="bronze_inventory_valid",
          comment="Valid inventory snapshots from bronze_inventory table",
)
def bronze_inventory_valid():
    return spark.readStream.table("bronze_inventory_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_inventory_quarantined",
          comment="Inventory rows that failed at least one structural quality check. Published",
)
def bronze_inventory_quarantined():
    return spark.readStream.table("bronze_inventory_quality_check").where("is_quarantined = true")


@dp.table(private=True,
          name="bronze_clickstream_quality_check",
          partition_cols=["is_quarantined"])
@dp.expect_all(CLICKSTREAM_RULES)
def bronze_clickstream_quality_check():
    return _with_quarantine_flag("bronze_clickstream", CLICKSTREAM_RULES)


@dp.table(name="bronze_clickstream_valid",
          comment="Valid clickstream events from bronze_clickstream table",
)
def bronze_clickstream_valid():
    return spark.readStream.table("bronze_clickstream_quality_check").where("is_quarantined = false")


@dp.table(name="bronze_clickstream_quarantined",
          comment="Clickstream events that failed at least one structural quality check. Published",
)
def bronze_clickstream_quarantined():
    return spark.readStream.table("bronze_clickstream_quality_check").where("is_quarantined = true")
from pyspark.sql import DataFrame, functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    BooleanType,
    DateType,
)

from utilities.helpers import landing_path

# DBTITLE 1,Create schemas for the CDC payloads
ORDERS_ROW_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("discount_code", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address_id", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_country", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("updated_at", StringType(), True),
])

ORDER_ITEMS_ROW_SCHEMA = StructType([
    StructField("line_total", DoubleType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("return_reason", StringType(), True),
    StructField("return_requested", BooleanType(), True),
    StructField("sku", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

CUSTOMERS_ROW_SCHEMA = StructType([
    StructField("address_line1", StringType(), True),
    StructField("address_line2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("last_name", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("state", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("zip_code", StringType(), True),
])

PRODUCTS_ROW_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("gender_target", StringType(), True),
    StructField("size_uk", DoubleType(), True),
    StructField("colour", StringType(), True),
    StructField("material", StringType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("retail_price", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("launch_date", DateType(), True),
    StructField("_rescued_data", StringType(), True),
])

CATEGORIES_ROW_SCHEMA = StructType([
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("parent_category_id", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("_rescued_data", StringType(), True),
])

INVENTORY_ROW_SCHEMA = StructType([
    StructField("snapshot_id", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("product_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("days_of_supply", IntegerType(), True),
    StructField("_rescued_data", StringType(), True),
])

CLICKSTREAM_ROW_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("search_term", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])


def _envelope_schema(raw_schema: StructType) -> StructType:
    return StructType([
        StructField("after", raw_schema, True),
        StructField("before", raw_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("_rescued_data", StringType(), True),
    ])


def _with_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )


def _read_cdc_bronze(subfolder: str, row_schema: StructType) -> DataFrame:
    return _with_ingestion_metadata(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_envelope_schema(row_schema))
        .load(landing_path(subfolder))
    )


def _read_json_bronze(subfolder: str, row_schema: StructType) -> DataFrame:
    return _with_ingestion_metadata(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(row_schema)
        .load(landing_path(subfolder))
    )


def _read_csv_bronze(subfolder: str, row_schema: StructType) -> DataFrame:
    return _with_ingestion_metadata(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(row_schema)
        .load(landing_path(subfolder))
    )


@dp.table(name="bronze_orders", comment="Raw orders data from the CDC stream")
def orders() -> DataFrame:
    return _read_cdc_bronze("orders_cdc", ORDERS_ROW_SCHEMA)


@dp.table(name="bronze_order_items", comment="Raw order items data from the CDC stream")
def order_items() -> DataFrame:
    return _read_cdc_bronze("order_items_cdc", ORDER_ITEMS_ROW_SCHEMA)


@dp.table(name="bronze_customers", comment="Raw customers data from the CDC stream")
def customers() -> DataFrame:
    return _read_cdc_bronze("customers_cdc", CUSTOMERS_ROW_SCHEMA)


@dp.table(name="bronze_products", comment="Raw products data from landing files")
def products() -> DataFrame:
    return _read_csv_bronze("products", PRODUCTS_ROW_SCHEMA)


@dp.table(name="bronze_categories", comment="Raw categories data from landing files")
def categories() -> DataFrame:
    return _read_csv_bronze("categories", CATEGORIES_ROW_SCHEMA)


@dp.table(name="bronze_inventory", comment="Raw inventory snapshots from landing files")
def inventory() -> DataFrame:
    return _read_csv_bronze("inventory", INVENTORY_ROW_SCHEMA)


@dp.table(name="bronze_clickstream", comment="Raw clickstream events from landing files")
def clickstream() -> DataFrame:
    return _read_json_bronze("clickstream", CLICKSTREAM_ROW_SCHEMA)























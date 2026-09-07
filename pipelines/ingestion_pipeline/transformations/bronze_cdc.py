from pyspark.sql import DataFrame, functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
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


def _envelope_schema(raw_schema: StructType) -> StructType:
    return StructType([
        StructField("after", raw_schema, True),
        StructField("before", raw_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("_rescued_data", StringType(), True),
    ])


def _read_cdc_bronze(subfolder: str, row_schema: StructType) -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(_envelope_schema(row_schema))
        .load(landing_path(subfolder))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
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





















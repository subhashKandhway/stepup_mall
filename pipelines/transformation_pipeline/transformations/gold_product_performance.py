from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.helpers import current_inventory_by_product, read_table


@dp.materialized_view(
    name="gold_product_performance",
    comment="Product sales performance with latest current stock and stockout risk for merchandising.",
    cluster_by_auto=True,
)
def gold_product_performance():
    products = read_table("silver_products")
    order_items = read_table("silver_order_items")
    inventory = read_table("silver_inventory")

    product_sales = order_items.groupBy("product_id").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("revenue"),
    )
    current_inventory = current_inventory_by_product(inventory)

    return (
        products.join(product_sales, on="product_id", how="left")
        .join(current_inventory, on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "brand",
            F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
            F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue"),
            F.coalesce(F.col("current_stock"), F.lit(0)).alias("current_stock"),
            (
                F.coalesce(F.col("current_stock"), F.lit(0))
                <= F.coalesce(F.col("reorder_point"), F.lit(0))
            ).alias("stockout_risk"),
        )
    )

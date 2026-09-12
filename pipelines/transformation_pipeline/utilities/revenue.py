from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def allocate_order_discounts(order_items: DataFrame, orders_current: DataFrame) -> DataFrame:
    order_subtotals = order_items.groupBy("order_id").agg(
        F.sum("line_total").alias("order_subtotal")
    )

    return (
        order_items.join(order_subtotals, on="order_id")
        .join(orders_current, on="order_id")
        .withColumn("order_discount", F.coalesce(F.col("discount_amount"), F.lit(0.0)))
        .withColumn(
            "allocated_discount",
            F.when(
                F.col("order_subtotal") > 0,
                F.col("order_discount") * F.col("line_total") / F.col("order_subtotal"),
            ).otherwise(F.lit(0.0)),
        )
    )


def build_daily_revenue(
    items_with_discount: DataFrame,
    products: DataFrame,
    categories: DataFrame,
) -> DataFrame:
    return (
        items_with_discount.join(products, on="product_id")
        .join(categories, on="category_id")
        .groupBy(
            F.to_date("order_date").alias("revenue_date"),
            "category_name",
            F.col("shipping_state").alias("region"),
        )
        .agg(
            F.sum("line_total").alias("gross_revenue"),
            F.sum("allocated_discount").alias("total_discount"),
            (F.sum("line_total") - F.sum("allocated_discount")).alias("net_revenue"),
        )
    )

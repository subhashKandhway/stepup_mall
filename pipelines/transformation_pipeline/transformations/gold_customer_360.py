# Marketing - who's valuable and about to churn. Lifetime value, order frequency, and a plain recency signal.
# Customers + orders -> per-customer summary -> join back to the current customer profile.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.helpers import read_current_scd2


@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency and a descriptive churn signal - Marketing's question from L1",
)
def gold_customer_360():
    orders_current = read_current_scd2("silver_orders")
    customers_current = read_current_scd2("silver_customers")

    order_summary = orders_current.groupBy("customer_id").agg(
        F.sum("total_amount").alias("total_value"),
        F.count("order_id").alias("order_count"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
        F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order"),
        F.datediff(F.current_date(), F.min("order_date")).alias("days_since_first_order"),
    )

    return (
        customers_current.join(order_summary, on="customer_id", how="left")
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date")),
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "loyalty_tier",
            "total_value",
            "order_count",
            "last_order_date",
            "days_since_last_order",
        )
    )

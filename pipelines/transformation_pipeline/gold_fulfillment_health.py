from pyspark import pipelines as dp
from pyspark.sql import functions as F

SLA_DAYS = 5

@dp.materialized_view(
    name="gold_fulfillment_health",
    comment="Order fulfillment SLA compliance by day and region - Operations' question from L1. "
            "Order-timing metric only; not joined to inventory (orders has no warehouse_id)"
)
def gold_fulfillment_health():
    orders_current = spark.read.table("silver_orders").filter(F.col("__END_AT").isNull())
    delivered = (
        orders_current
        .filter(F.col("order_status") == "delivered")
        .withColumn("DELIVERY_DATE", F.col("updated_at"))
        .withColumn("DELIVERY_LAG", F.datediff(F.col("DELIVERY_DATE"), F.col("order_date")))
        .withColumn("DELIVERY_LAG_SLA", F.when(F.col("DELIVERY_LAG") <= SLA_DAYS, 1).otherwise(0))
    )

    return (
        delivered
        .groupBy(
            F.to_date(F.col("order_date")).alias("order_date"),
            F.col("shipping_state").alias("region"),
        )
        .agg(
            F.count("order_id").alias("delivered_orders"),
            F.sum(F.col("DELIVERY_LAG_SLA")).alias("on_time_deliveries"),
        )
        .withColumn("sla_compliance", F.col("on_time_deliveries") / F.col("delivered_orders"))
    )
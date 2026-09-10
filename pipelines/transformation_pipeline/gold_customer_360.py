#  Marketing - who's valuable 's about to churn. Lifetime value, order frequency , a plain recency signal
# Customers+orders ---> per customer order ---> sum,count(),max_date then join back to the profile


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import pandas_udf
from pyspark import pipelines as dp





@dp.materialized_view(name="gold_customer_360",
                     comment="Lifetime value, order frequency and a descriptive churn signal - Marketing's question from L1",)
def gold_customer_360():
    orders_current = spark.read.table("silver_orders").filter(col("__END_AT").isNull())
    customer_current = spark.read.table("silver_customers").filter(col("__END_AT").isNull())

    order_summary=(
        orders_current
        .groupBy("customer_id")
        .agg(
            sum("total_amount").alias("total_value"),
            count("order_id").alias("order_count"),
            max("order_date").alias("last_order_date"),
            min("order_date").alias("first_order_date"),
            datediff(current_date(),max("order_date")).alias("days_since_last_order"),
            datediff(current_date(),min("order_date")).alias("days_since_first_order"),
    )
    )

    return (
        customer_current.join(order_summary, on="customer_id", how="left")
        .withColumn("days_since_last_order",datediff(current_date(),col("last_order_date")))
        .select("customer_id","first_name","last_name","loyalty_tier","total_value","order_count","last_order_date","days_since_last_order")
    )


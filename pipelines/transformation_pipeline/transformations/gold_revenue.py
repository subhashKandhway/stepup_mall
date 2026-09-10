from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Daily revenue by day, category and region - gross , discount and net reported"
)

def gold_daily_revenue():
   order_items=spark.read.table("silver_order_items")
   orders_current=spark.read.table("silver_orders").filter(col("__END_AT").isNull())
   products=spark.read.table("silver_products")
   categories=spark.read.table("silver_categories")
   order_subtotals=(
       order_items.groupBy("order_id")
       .agg(sum("line_total").alias("order_subtotal"))
   )
   
   items_with_discount=(
       order_items
       .join(order_subtotals,on="order_id")
       .join(orders_current,on="order_id")
       .withColumn("order_discount",coalesce(col("discount_amount"),lit(0.0)))
       .withColumn("allocated_discount",when(col("order_subtotal")>0,col("order_discount")*col("line_total")/col("order_subtotal")).otherwise(lit(0.0)))
   )

   return (
       items_with_discount
       .join(products,on="product_id")
       .join(categories,on="category_id")
       .groupBy(to_date("order_date").alias("revenue_date"),"category_name",col("shipping_state").alias("region"))
       .agg(
           sum("line_total").alias("gross_revenue"),
           sum("allocated_discount").alias("total_discount"),
           (sum("line_total")-sum("allocated_discount")).alias("net_revenue")
       )
   )


from pyspark import pipelines as dp

from utilities.helpers import read_current_scd2, read_table
from utilities.revenue import allocate_order_discounts, build_daily_revenue


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Daily revenue by day, category and region - gross, discount and net reported",
)
def gold_daily_revenue():
    order_items = read_table("silver_order_items")
    orders_current = read_current_scd2("silver_orders")
    products = read_table("silver_products")
    categories = read_table("silver_categories")

    items_with_discount = allocate_order_discounts(order_items, orders_current)
    return build_daily_revenue(items_with_discount, products, categories)

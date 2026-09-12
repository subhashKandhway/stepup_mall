from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utilities.revenue import allocate_order_discounts, build_daily_revenue


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# allocate_order_discounts
# ---------------------------------------------------------------------------

def test_discount_distributed_proportionally_to_line_total(spark):
    """Each item's discount share = order_discount * (line_total / order_subtotal)."""
    order_items = spark.createDataFrame(
        [("o1", "p1", 100.0), ("o1", "p2", 200.0)],
        ["order_id", "product_id", "line_total"],
    )
    orders = spark.createDataFrame(
        [("o1", 30.0)],
        ["order_id", "discount_amount"],
    )

    actual = (
        allocate_order_discounts(order_items, orders)
        .select("product_id", "order_discount", "allocated_discount")
        .orderBy("product_id")
    )
    expected = spark.createDataFrame(
        [("p1", 30.0, 10.0), ("p2", 30.0, 20.0)],
        ["product_id", "order_discount", "allocated_discount"],
    ).orderBy("product_id")

    assertDataFrameEqual(actual, expected)


def test_null_discount_treated_as_zero(spark):
    """When discount_amount is NULL, allocated_discount must be 0 for every item."""
    order_items = spark.createDataFrame(
        [("o2", "p3", 80.0)],
        ["order_id", "product_id", "line_total"],
    )
    orders = spark.createDataFrame(
        [("o2", None)],
        ["order_id", "discount_amount"],
    ).selectExpr("order_id", "CAST(discount_amount AS DOUBLE) AS discount_amount")

    actual = (
        allocate_order_discounts(order_items, orders)
        .select("product_id", "order_discount", "allocated_discount")
    )
    expected = spark.createDataFrame(
        [("p3", 0.0, 0.0)],
        ["product_id", "order_discount", "allocated_discount"],
    )

    assertDataFrameEqual(actual, expected)


def test_multiple_orders_allocated_independently(spark):
    """Items from different orders must not share each other's discounts."""
    order_items = spark.createDataFrame(
        [("o1", "p1", 100.0), ("o2", "p2", 50.0)],
        ["order_id", "product_id", "line_total"],
    )
    orders = spark.createDataFrame(
        [("o1", 10.0), ("o2", 5.0)],
        ["order_id", "discount_amount"],
    )

    actual = (
        allocate_order_discounts(order_items, orders)
        .select("product_id", "allocated_discount")
        .orderBy("product_id")
    )
    expected = spark.createDataFrame(
        [("p1", 10.0), ("p2", 5.0)],
        ["product_id", "allocated_discount"],
    ).orderBy("product_id")

    assertDataFrameEqual(actual, expected)


# ---------------------------------------------------------------------------
# build_daily_revenue
# ---------------------------------------------------------------------------

def test_revenue_grouped_by_date_category_and_region(spark):
    """Two items in the same category/region/day are summed into one row."""
    items = spark.createDataFrame(
        [
            ("p1", "2026-09-01", "CA", 100.0, 10.0),
            ("p1", "2026-09-01", "CA",  50.0,  5.0),
            ("p2", "2026-09-01", "NY", 200.0, 20.0),
        ],
        ["product_id", "order_date", "shipping_state", "line_total", "allocated_discount"],
    )
    products   = spark.createDataFrame([("p1", "c1"), ("p2", "c2")], ["product_id", "category_id"])
    categories = spark.createDataFrame([("c1", "Shoes"), ("c2", "Accessories")], ["category_id", "category_name"])

    actual = (
        build_daily_revenue(items, products, categories)
        .orderBy("revenue_date", "category_name", "region")
    )
    expected = (
        spark.createDataFrame(
            [
                ("2026-09-01", "Accessories", "NY", 200.0, 20.0, 180.0),
                ("2026-09-01", "Shoes",       "CA", 150.0, 15.0, 135.0),
            ],
            ["revenue_date", "category_name", "region", "gross_revenue", "total_discount", "net_revenue"],
        )
        .selectExpr(
            "to_date(revenue_date) AS revenue_date",
            "category_name", "region", "gross_revenue", "total_discount", "net_revenue",
        )
        .orderBy("revenue_date", "category_name", "region")
    )

    assertDataFrameEqual(actual, expected)


def test_net_revenue_equals_gross_minus_discount(spark):
    """net_revenue column must equal gross_revenue - total_discount for every row."""
    items = spark.createDataFrame(
        [("p1", "2026-09-01", "TX", 300.0, 30.0)],
        ["product_id", "order_date", "shipping_state", "line_total", "allocated_discount"],
    )
    products   = spark.createDataFrame([("p1", "c1")], ["product_id", "category_id"])
    categories = spark.createDataFrame([("c1", "Bags")], ["category_id", "category_name"])

    row = build_daily_revenue(items, products, categories).collect()[0]
    assert row["net_revenue"] == row["gross_revenue"] - row["total_discount"]

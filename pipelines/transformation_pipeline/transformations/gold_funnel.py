from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.helpers import read_table


@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="This is a materialized view of the gold_funnel_analysis",
)
def gold_funnel_analysis():
    events = read_table("silver_clickstream")
    session_outcomes = events.groupBy("session_id", "referrer").agg(
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("converted")
    )

    return (
        session_outcomes.groupBy("referrer")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum("converted").alias("total_conversions"),
        )
        .withColumn(
            "conversion_rate",
            F.col("total_conversions") / F.col("total_sessions"),
        )
    )

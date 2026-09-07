# Shared utilities for the StepRight ingestion pipeline

from pyspark.sql import SparkSession


def get_catalog() -> str:
    spark = SparkSession.getActiveSession()
    return spark.conf.get("stepright.catalog", "dev")


def landing_path(subfolder: str) -> str:
    return f"/Volumes/{get_catalog()}/stepright/landing/{subfolder}"

    



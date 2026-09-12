from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_catalog() -> str:
    spark = get_spark()
    return spark.conf.get("stepright.catalog", "dev")


def bronze_table(name: str) -> str:
    return f"{get_catalog()}.stepright.{name}"


def read_table(name: str) -> DataFrame:
    return get_spark().read.table(name)


def read_current_scd2(name: str) -> DataFrame:
    return read_table(name).filter(F.col("__END_AT").isNull())
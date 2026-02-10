"""
Common transformation utilities.

Used across all dimension and fact transformations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    lower,
    trim,
    regexp_replace,
    to_timestamp,
    current_timestamp
)


# ----------------------------------
# Mongo RAW reader
# ----------------------------------

def read_raw_collection(
    spark: SparkSession,
    collection: str
) -> DataFrame:
    """
    Read a raw MongoDB collection into Spark.

    Mongo connection URI is injected at runtime.
    """

    return (
        spark.read
        .format("mongodb")
        .option("uri", "MONGO_URI_HASHED_PLACEHOLDER")
        .option("database", "raw_genesys")
        .option("collection", collection)
        .load()
    )


# ----------------------------------
# ID cleaning helpers
# ----------------------------------

def clean_id(col_expr):
    """
    Clean ID fields that may arrive as:
    - single-element arrays
    - stringified arrays
    """

    return (
        when(col_expr.isNull(), None)
        .when(col_expr.cast("string").startswith("["),
              regexp_replace(col_expr.cast("string"), r'[\[\]\"]', ""))
        .otherwise(col_expr)
    )


# ----------------------------------
# String normalization
# ----------------------------------

def normalize_string(col_expr, to_lower: bool = True, default_value: str = "UNKNOWN"):
    cleaned = trim(col_expr)

    if to_lower:
        cleaned = lower(cleaned)

    return when(cleaned.isNull(), default_value).otherwise(cleaned)


# ----------------------------------
# Timestamp normalization
# ----------------------------------

def normalize_utc_timestamp(col_name: str):
    """
    Force timestamps to UTC format:
    YYYY-MM-DD HH:MM:SSZ
    """

    return to_timestamp(col(col_name))


# ----------------------------------
# Data quality flags
# ----------------------------------

def flag_incomplete_record(*cols):
    condition = None
    for c in cols:
        expr = col(c).isNull()
        condition = expr if condition is None else condition | expr

    return when(condition, 1).otherwise(0)


def flag_valid_numeric(col_name: str):
    return when(col(col_name) < 0, 0).otherwise(1)


# ----------------------------------
# Audit columns
# ----------------------------------

def add_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return (
        df
        .withColumn("etl_source", lit(source))
        .withColumn("etl_loaded_at", current_timestamp())
    )

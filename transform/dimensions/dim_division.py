"""
Division dimension built from raw MongoDB divisions collection.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transform.utils.common import (
    clean_id,
    normalize_string,
    add_audit_columns
)


def build_dim_division(raw_divisions_df: DataFrame) -> DataFrame:
    dim = (
        raw_divisions_df
        .withColumn("Division_Id", clean_id(col("id")))
        .withColumn("Division_Name", normalize_string(col("name"), to_lower=False))
        .dropDuplicates(["Division_Id"])
    )

    dim = dim.select(
        "Division_Id",
        "Division_Name"
    )

    dim = add_audit_columns(dim, source="genesys_divisions_raw")

    return dim

"""
Qualification (wrap-up codes) dimension built from raw MongoDB data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transform.utils.common import (
    clean_id,
    normalize_string,
    add_audit_columns
)


def build_dim_qualification(raw_wrapups_df: DataFrame) -> DataFrame:
    dim = (
        raw_wrapups_df
        .withColumn("WrapUpCode_Id", clean_id(col("id")))
        .withColumn("WrapUp_Label", normalize_string(col("name"), to_lower=False))
        .dropDuplicates(["WrapUpCode_Id"])
    )

    dim = dim.select(
        "WrapUpCode_Id",
        "WrapUp_Label"
    )

    dim = add_audit_columns(dim, source="genesys_wrapups_raw")

    return dim

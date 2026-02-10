"""
Queue dimension built from raw MongoDB queues collection.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transform.utils.common import (
    clean_id,
    normalize_string,
    add_audit_columns
)


def build_dim_queue(raw_queues_df: DataFrame) -> DataFrame:
    """
    Build queue dimension from raw queues data.
    """

    dim = (
        raw_queues_df
        .withColumn("Queue_Id", clean_id(col("id")))
        .withColumn("Queue_Name", normalize_string(col("name"), to_lower=False))
        .withColumn("Division_Id", clean_id(col("division")["id"]))
        .dropDuplicates(["Queue_Id"])
    )

    dim = dim.select(
        "Queue_Id",
        "Queue_Name",
        "Division_Id"
    )

    dim = add_audit_columns(dim, source="genesys_queues_raw")

    return dim

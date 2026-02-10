"""
User dimension built from raw MongoDB users collection.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transform.utils.common import (
    clean_id,
    normalize_string,
    add_audit_columns
)


def build_dim_users(raw_users_df: DataFrame) -> DataFrame:
    """
    Build user dimension from raw users data.
    """

    dim = (
        raw_users_df
        .withColumn("User_Id", clean_id(col("id")))
        .withColumn("User_Name", normalize_string(col("name"), to_lower=False))
        .withColumn("User_Email", normalize_string(col("email")))
        .withColumn("Division_Id", clean_id(col("division")["id"]))
        .dropDuplicates(["User_Id"])
    )

    dim = dim.select(
        "User_Id",
        "User_Name",
        "User_Email",
        "Division_Id"
    )

    dim = add_audit_columns(dim, source="genesys_users_raw")

    return dim

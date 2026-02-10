"""
Skills dimension built from raw MongoDB skills collection.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transform.utils.common import (
    clean_id,
    normalize_string,
    add_audit_columns
)


def build_dim_skills(raw_skills_df: DataFrame) -> DataFrame:
    dim = (
        raw_skills_df
        .withColumn("Skill_Id", clean_id(col("id")))
        .withColumn("Skill_Name", normalize_string(col("name"), to_lower=False))
        .dropDuplicates(["Skill_Id"])
    )

    dim = dim.select(
        "Skill_Id",
        "Skill_Name"
    )

    dim = add_audit_columns(dim, source="genesys_skills_raw")

    return dim

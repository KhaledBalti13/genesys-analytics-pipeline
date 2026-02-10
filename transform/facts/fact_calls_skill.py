from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode

from transform.utils.common import (
    clean_id,
    add_audit_columns,
    flag_incomplete_record
)

def build_fact_call_skills(raw_df: DataFrame) -> DataFrame:
    """
    Build the call-to-skill bridge fact table.

    Args:
        raw_df (DataFrame): Raw Genesys API DataFrame.

    Returns:
        DataFrame: Bridge table linking calls and skills.
    """

    fact = (
        raw_df
        .withColumn("Interaction_Id", col("conversationId"))
        .withColumn("skill", explode(col("participants")[0]["skills"]))
        .withColumn("Skill_Id", clean_id(col("skill")["id"]))
    )

    # Data quality flag
    fact = fact.withColumn(
        "Is_Incomplete_Record",
        flag_incomplete_record("Interaction_Id", "Skill_Id")
    )

    # Select final columns
    fact = fact.select(
        "Interaction_Id",
        "Skill_Id",
        "Is_Incomplete_Record"
    )

    # Deduplicate bridge rows
    fact = fact.dropDuplicates(["Interaction_Id", "Skill_Id"])

    # Add audit & lineage
    fact = add_audit_columns(fact, source="genesys_api")

    return fact

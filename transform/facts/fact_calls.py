from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    unix_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from transform.utils.common import (
    clean_id,
    normalize_utc_timestamp,
    add_audit_columns,
    flag_incomplete_record,
    flag_valid_numeric
)

def build_fact_calls(raw_df: DataFrame) -> DataFrame:
    """
    Build the main calls fact table from raw Genesys API data.

    Args:
        raw_df (DataFrame): Raw API DataFrame.

    Returns:
        DataFrame: Analytics-ready calls fact table.
    """

    fact = (
        raw_df
        .withColumn("Interaction_Id", col("conversationId"))
        .withColumn("User_Id", clean_id(col("participants")[0]["userId"]))
        .withColumn("Queue_Id", clean_id(col("participants")[0]["queueId"]))
        .withColumn("Division_Id", clean_id(col("participants")[0]["divisionId"]))
        .withColumn("WrapUpCode_Id", clean_id(col("participants")[0]["wrapupCodes"][0]["code"]))
        .withColumn("Type_Appel", col("participants")[0]["purpose"])
        .withColumn("Start_Time", normalize_utc_timestamp("startTime"))
        .withColumn("End_Time", normalize_utc_timestamp("endTime"))
    )

    # -------------------------
    # Business metrics
    # -------------------------

    fact = fact.withColumn(
        "Call_Duration_Seconds",
        unix_timestamp(col("End_Time")) - unix_timestamp(col("Start_Time"))
    )

    fact = fact.withColumn(
        "Abandoned",
        when(col("participants")[0]["state"] != "connected", 1).otherwise(0)
    )

    fact = fact.withColumn(
        "Is_Answered",
        when(col("Abandoned") == 0, 1).otherwise(0)
    )

    fact = fact.withColumn(
        "Is_Long_Call",
        when(col("Call_Duration_Seconds") > 300, 1).otherwise(0)
    )

    # -------------------------
    # Data quality flags
    # -------------------------

    fact = fact.withColumn(
        "Is_Incomplete_Record",
        flag_incomplete_record(
            "Interaction_Id",
            "User_Id",
            "Queue_Id",
            "Start_Time"
        )
    )

    fact = fact.withColumn(
        "Is_Valid_Record",
        flag_valid_numeric("Call_Duration_Seconds")
    )

    # -------------------------
    # Soft deduplication
    # -------------------------

    window = Window.partitionBy("Interaction_Id").orderBy(col("Start_Time").desc())

    fact = (
        fact
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # -------------------------
    # Final selection
    # -------------------------

    fact = fact.select(
        "Interaction_Id",
        "User_Id",
        "Queue_Id",
        "Division_Id",
        "WrapUpCode_Id",
        "Type_Appel",
        "Start_Time",
        "End_Time",
        "Call_Duration_Seconds",
        "Is_Answered",
        "Is_Long_Call",
        "Abandoned",
        "Is_Valid_Record",
        "Is_Incomplete_Record"
    )

    # Add audit & lineage
    fact = add_audit_columns(fact, source="genesys_api")

    return fact

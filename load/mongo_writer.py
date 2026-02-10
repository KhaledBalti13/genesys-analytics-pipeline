"""
MongoDB load utilities for the Genesys Analytics ETL.

- Uses UPSERT logic to avoid duplicates
- Supports idempotent ETL runs
- Credentials are injected via environment variables
"""

import os
from pymongo import MongoClient, UpdateOne
from typing import List


def write_to_mongo_upsert(
    df,
    collection_name: str,
    natural_key: str
) -> None:
    """
    Writes a Spark DataFrame to MongoDB using UPSERT logic.

    Args:
        df (DataFrame): Spark DataFrame to load
        collection_name (str): Target MongoDB collection
        natural_key (str): Business key used for upsert matching
    """

    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is not set")

    client = MongoClient(mongo_uri)
    db = client["genesys_dwh"]
    collection = db[collection_name]

    # Convert Spark DataFrame to list of dicts
    records = df.toPandas().to_dict("records")

    operations: List[UpdateOne] = []

    for record in records:
        if natural_key not in record or record[natural_key] is None:
            continue

        operations.append(
            UpdateOne(
                {natural_key: record[natural_key]},
                {"$set": record},
                upsert=True
            )
        )

    if operations:
        collection.bulk_write(operations, ordered=False)

    client.close()

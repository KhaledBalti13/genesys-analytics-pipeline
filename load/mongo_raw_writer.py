"""
Raw MongoDB writer.

- Stores raw API payloads as-is
- Uses UPSERT for idempotency
- One collection per entity type
"""

import os
from pymongo import MongoClient, UpdateOne
from typing import List, Dict


def write_raw_entities(
    records: List[Dict],
    collection_name: str,
    natural_key: str
) -> None:
    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is not set")

    client = MongoClient(mongo_uri)
    db = client["raw_genesys"]
    collection = db[collection_name]

    operations = []

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

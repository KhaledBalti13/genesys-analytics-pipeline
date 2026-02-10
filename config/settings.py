"""
Global configuration for the Genesys Analytics ETL project.

- No credentials are hardcoded
- All sensitive values are injected via environment variables
- Spark behavior is fully controlled from this file
"""

import os


# =====================================================
# MongoDB Configuration (SECURE)
# =====================================================

# MongoDB connection string injected via environment variables
# Example (DO NOT COMMIT REAL VALUES):
# mongodb://import_user:$2b$12$8FJ2kLQzX9Yv...@cluster0.mongodb.net/genesys_dwh
MONGO_URI = os.getenv("MONGO_URI")

# Target MongoDB database
MONGO_DB = "genesys_dwh"


# =====================================================
# ETL Metadata
# =====================================================

ETL_SOURCE = "genesys_api"


# =====================================================
# Spark Application Configuration
# =====================================================

SPARK_APP_NAME = "genesys-analytics-etl"


# =====================================================
# Spark Runtime Controls (PRODUCTION-READY DEFAULTS)
# =====================================================

SPARK_CONFIG = {

    # ---- Time & correctness ----
    # Force all Spark sessions to UTC
    "spark.sql.session.timeZone": "UTC",

    # Handle legacy / inconsistent timestamp formats from APIs
    "spark.sql.legacy.timeParserPolicy": "LEGACY",

    # ---- Performance (safe defaults) ----
    # Number of shuffle partitions (default 200 is often too high)
    "spark.sql.shuffle.partitions": "8",

    # Enable adaptive query execution (Spark optimizes at runtime)
    "spark.sql.adaptive.enabled": "true",

    # ---- Stability & safety ----
    # Prevent unexpected large broadcast joins
    "spark.sql.autoBroadcastJoinThreshold": "-1",

    # Use Kryo serialization for better performance
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

    # ---- Data handling ----
    # Ignore corrupt records instead of failing the job
    "spark.sql.files.ignoreCorruptFiles": "true"
}

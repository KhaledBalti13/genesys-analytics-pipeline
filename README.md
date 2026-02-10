Genesys Analytics ETL (Near Real-Time)

This project is a near real-time ETL pipeline built on Genesys Cloud data.
The focus is on data ingestion, transformation, and modeling rather than dashboard visuals.

The objective is to demonstrate how production-style data pipelines are designed, with attention to data quality, replayability, and data security.

Architecture overview

Genesys Cloud APIs provide operational data such as conversations, users, queues, skills, and wrap-up codes.

Data flow:

Genesys API
MongoDB raw layer
Spark transformations
MongoDB curated (DWH) layer
BI dashboard

Raw and curated layers

Two MongoDB databases are used.

Raw layer:

Stores API payloads with minimal changes

Preserves original structures for replay and debugging

Avoids repeated API calls when transformation logic changes

Curated (DWH) layer:

Contains cleaned and modeled data

Organized into fact and dimension collections

Optimized for analytics and reporting

Ingestion strategy

Pipeline runs every 2 minutes

Each run pulls data from the last 3 minutes

Overlapping window handles late-arriving data

Data is loaded using upserts to avoid duplicates

Credentials and connection strings are never hardcoded.
Placeholders are used to reflect secure credential handling.

Transformations

All transformations are done using PySpark.

Main processing steps:

Timestamp normalization to UTC format

Cleaning ID fields arriving as arrays or stringified arrays

String normalization (trim, casing, default values)

Deduplication based on business keys

Audit columns added for traceability

Data model

Dimensions:

Users

Queues

Divisions

Skills

Wrap-up codes

Facts:

Calls

Call-skill relationships

The model follows a star-schema logic to simplify analytical queries.

Dashboard and data privacy

Only one dashboard screenshot is included intentionally.

The dashboard contains operational and customer-related data.
To respect GDPR and data privacy principles, full dashboards and raw metrics are not publicly exposed.

The primary value of this project lies in the ETL pipeline design and data modeling decisions.

What this project demonstrates

Realistic ETL pipeline design

Near real-time ingestion patterns

Raw versus curated data layering

Analytics-oriented modeling

Data security and privacy awareness
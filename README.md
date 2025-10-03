### User Denormalization Pipeline with Spark, Delta Lake & Kafka ###

This project implements a real-time user profile enrichment pipeline designed for scalable data engineering workloads. It leverages Delta Lake Change Data Capture (CDC) to track incremental updates, applies denormalization logic across multiple related datasets, and streams enriched user profiles to Apache Kafka for downstream services.

### Features ###

* CDC with Delta Lake

Efficiently captures only the incremental changes from source tables using Delta Lake’s table_changes.

Checkpoint tables ensure reliable recovery and exactly-once semantics.

User Denormalization

Consolidates scattered user information (accounts, education, work experience, tags, etc.) into a single enriched view.

Nested JSON-like structures provide a clean and consumer-friendly schema.


* Kafka Integration

Converts Spark DataFrames into Kafka key/value messages.

Streams denormalized user profiles to downstream consumers in near real-time.

Performance Awareness

Frequently reused DataFrames are cached to reduce redundant queries and accelerate joins.

Deduplication logic ensures minimal processing of repeated IDs.


* Modular Codebase

Separation of concerns:

index.py – pipeline orchestration.

core/delta_lake_to_kafka.py – CDC extraction & Kafka writers.

core/denormalization_sql.py – SQL logic for user profile enrichment.

core/logger.py – structured logging.


### Repository Structure ###
.
├── index.py                        # Main entrypoint, orchestrates pipeline
└── core/
    ├── delta_lake_to_kafka.py      # CDC extraction, Kafka formatting/writing
    ├── denormalization_sql.py      # SQL-based denormalization logic
    └── logger.py                   # Logging setup

### Data Flow ###

Delta Lake CDC – Polls Delta tables on S3 for incremental updates.

User ID Extraction – Collects and deduplicates changed user IDs.

Denormalization – Joins across multiple related datasets to enrich user profiles.

Caching – Optimizes repeated queries for high-traffic datasets.

Kafka Streaming – Publishes enriched profiles to Kafka for downstream services.

### Tech Stack ### 

Apache Spark – distributed processing & SQL.

Delta Lake – reliable CDC and ACID guarantees.

Apache Kafka – streaming integration for real-time pipelines.

Python – pipeline orchestration and modular design.


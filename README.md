### User Denormalization Pipeline with Spark, Delta Lake & Kafka ###

This project implements a near-real-time user profile enrichment pipeline designed for scalable data engineering workloads. It leverages Delta Lake Change Data Capture (CDC) to track incremental updates, applies denormalization logic across multiple related datasets, and streams enriched user profiles to Apache Kafka for downstream services.

### Key Features ###

* CDC with Delta Lake

Efficiently captures only the incremental changes from source tables using Delta Lake’s table_changes.<br>
Checkpoint tables ensure reliable recovery and exactly-once semantics.<br>

* User Denormalization

Consolidates scattered user information (accounts, education, work experience, tags, etc.) into a single enriched view.<br>
Nested JSON-like structures provide a clean and consumer-friendly schema.<br>


* Kafka Integration

Converts Spark DataFrames into Kafka key/value messages.<br>
Streams denormalized user profiles to downstream consumers in near real-time.<br>

* Performance Awareness

Frequently reused DataFrames are cached to reduce redundant queries and accelerate joins.<br>
Deduplication logic ensures minimal processing of repeated IDs.<br>


* Modular Codebase

Separation of concerns:
index.py – pipeline orchestration.<br>
core/delta_lake_to_kafka.py – CDC extraction & Kafka writers.<br>
core/denormalization_sql.py – SQL logic for user profile enrichment.<br>
core/logger.py – structured logging.<br>


### Data Flow ###

Delta Lake CDC – Polls Delta tables on S3 for incremental updates.<br>
User ID Extraction – Collects and deduplicates changed user IDs.<br>
Denormalization – Joins across multiple related datasets to enrich user profiles.<br>
Caching – Optimizes repeated queries for high-traffic datasets.<br>
Kafka Streaming – Publishes enriched profiles to Kafka for downstream services.<br>

### Tech Stack ### 

Python ,Pyspark<br>
Apache Spark <br>
Delta Lake<br>
Apache Kafka.<br>
Aiven Kafka Connector (for Opensearch indexing)<br>
S3/Cloud Object Storage





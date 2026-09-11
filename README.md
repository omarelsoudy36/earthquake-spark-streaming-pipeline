# 🌋 End-to-End Earthquake Streaming Pipeline (Medallion Architecture)

An end-to-end real-time data streaming pipeline built on **Databricks** and **PySpark**, designed to ingest, process, clean, and enrich global earthquake data using the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 📐 Architecture & Workflow

The pipeline utilizes **Databricks Workflows** to orchestrate a 3-task dependency chain ensuring data governance, schema evolution, and zero-loss streaming using checkpointing.

<!-- ضع صورة الـ Pipeline Job هنا -->
![Databricks Workflow Pipeline]<img width="1611" height="469" alt="Captur" src="https://github.com/user-attachments/assets/9e405176-ce54-4de5-842c-12299c277167" />


---

## 🛠️ Tech Stack & Key Concepts

* **Platform:** Databricks Lakehouse Platform
* **Orchestration:** Databricks Workflows (Multi-task Job Scheduling)
* **Processing Engine:** Apache Spark / PySpark Streaming
* **Storage & Governance:** Delta Lake, Unity Catalog Volumes
* **Ingestion:** Databricks Auto Loader (`cloudFiles`)
* **Core Principles:** ACID Transactions, Schema Evolution, Exactly-Once Processing (Checkpointing)

---

## 🏗️ Pipeline Layers Breakdown

### 1. 🥉 Bronze Layer (Raw Ingestion)
* **Objective:** Ingest raw JSON payloads fetched from the USGS Earthquake API.
* **Key Operations:**
  * Uses **Databricks Auto Loader (`cloudFiles`)** for automatic and scalable incremental file ingestion.
  * Stores data in raw form inside Unity Catalog Volumes (`landing_zone`).
  * Tracks schema changes seamlessly without pipeline breakdowns.

### 2. 🥈 Silver Layer (Cleaning & Parsing)
* **Objective:** Parse, clean, and flatten raw JSON data into structured Delta tables.
* **Key Operations:**
  * **Data Flattening:** Extracts nested JSON attributes into standard tabular columns.
  * **Data Quality Enforcements:** Filters out null `earthquake_id` values and duplicate entries.
  * **Checkpointing:** Utilizes dedicated `_checkpoints` and `_schemas` directories for fault tolerance and stream recovery.

### 3. 🥇 Gold Layer (Enrichment & Business Logic)
* **Objective:** Prepare aggregated, business-ready analytics data for consumption and BI tools.
* **Key Operations:**
  * **Categorization:** Classifies earthquake magnitude into categories (`Minor`, `Moderate`, `Major`) placed directly adjacent to the `magnitude` metric.
  * **Data Transformation:** Standardizes timestamp formats (`event_time_earthquake`), casts coordinates/depth to precise decimal values, and handles null titles.
  * **Auditability:** Appends an `ingested_at` timestamp column to maintain data lineage.
  * **Output:** Persists data as a high-performance Delta Table ready for SQL querying and reporting.

---

## 📊 Sample SQL Query (Gold Layer Analysis)

```sql
SELECT 
    earthquake_id,
    magnitude,
    magnitude_class,
    event_time_earthquake,
    is_tsunami,
    title,
    longitude,
    latitude,
    depth,
    ingested_at
FROM delta.`/Volumes/workspace/earthquake_db/gold_zone/earthquakes_gold`
ORDER BY event_time_earthquake DESC;

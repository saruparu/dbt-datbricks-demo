-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 📊 Databricks Data Flow — Delta Lake Deep Dive
-- MAGIC
-- MAGIC This notebook demonstrates how **data flows** through the Databricks Lakehouse using Delta Lake.
-- MAGIC We explore the Gold layer tables created by our dbt pipeline to showcase key Delta Lake features.
-- MAGIC
-- MAGIC **Prerequisites**: Run the dbt pipeline first (`dbt seed && dbt run`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1️⃣ Delta Lake Table Inspection
-- MAGIC
-- MAGIC Every table in Databricks is a **Delta table** — a Parquet-based format with a transaction log.
-- MAGIC Let's inspect the physical structure of our Gold layer fact table.

-- COMMAND ----------

-- Inspect the Delta table metadata
-- Shows: format, location, file count, size, partition columns, created timestamp
DESCRIBE DETAIL workspace.iot_dev_gold.fct_device_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What This Shows
-- MAGIC | Field | Meaning |
-- MAGIC |-------|---------|
-- MAGIC | `format` | Always "delta" — confirms Delta Lake format |
-- MAGIC | `location` | Cloud storage path (S3/ADLS/GCS) |
-- MAGIC | `numFiles` | Number of Parquet data files |
-- MAGIC | `sizeInBytes` | Total storage size |
-- MAGIC | `properties` | Table properties (clustering, optimization) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2️⃣ Transaction Log — DESCRIBE HISTORY
-- MAGIC
-- MAGIC Delta Lake maintains a **transaction log** (the `_delta_log/` directory) that records every change.
-- MAGIC This enables **audit trails**, **rollback**, and **time travel**.

-- COMMAND ----------

-- View the full history of operations on the fact table
-- Each row = one transaction (CREATE, WRITE, MERGE, OPTIMIZE, etc.)
DESCRIBE HISTORY workspace.iot_dev_gold.fct_device_summary;

-- COMMAND ----------

-- View history for the Silver incremental table
-- Shows MERGE operations from incremental dbt runs
DESCRIBE HISTORY workspace.iot_dev_silver.int_sensor_readings_cleaned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Key History Fields
-- MAGIC | Field | Meaning |
-- MAGIC |-------|---------|
-- MAGIC | `version` | Auto-incrementing version number (0, 1, 2, ...) |
-- MAGIC | `timestamp` | When the transaction occurred |
-- MAGIC | `operation` | CREATE TABLE, WRITE, MERGE, OPTIMIZE, etc. |
-- MAGIC | `operationMetrics` | Rows inserted/updated/deleted |
-- MAGIC | `userName` | Who made the change |
-- MAGIC
-- MAGIC 💡 **Interview Insight**: The transaction log is what makes Delta Lake **ACID-compliant** — every write is atomic, consistent, isolated, and durable.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3️⃣ Time Travel — Query Historical Data
-- MAGIC
-- MAGIC Delta Lake stores previous versions of data. You can query **any past version** using:
-- MAGIC - `VERSION AS OF <number>` — by version number
-- MAGIC - `TIMESTAMP AS OF <timestamp>` — by timestamp

-- COMMAND ----------

-- Query the CURRENT version (latest)
SELECT 
    'current' as version_label,
    count(*) as total_rows,
    count(distinct device_id) as unique_devices,
    round(avg(health_score), 1) as avg_health_score
FROM workspace.iot_dev_gold.fct_device_summary;

-- COMMAND ----------

-- Query VERSION 0 (the initial create)
-- This is the state of the table when it was first created
SELECT 
    'version_0' as version_label,
    count(*) as total_rows,
    count(distinct device_id) as unique_devices,
    round(avg(health_score), 1) as avg_health_score
FROM workspace.iot_dev_gold.fct_device_summary VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Time Travel Use Cases
-- MAGIC | Use Case | SQL Syntax |
-- MAGIC |----------|------------|
-- MAGIC | Debugging bad data | `SELECT * FROM table VERSION AS OF 3` |
-- MAGIC | Audit compliance | `DESCRIBE HISTORY table` |
-- MAGIC | Rollback a mistake | `RESTORE TABLE table TO VERSION AS OF 2` |
-- MAGIC | Compare versions | `EXCEPT` or `MINUS` between two versions |
-- MAGIC
-- MAGIC 💡 **How long is history kept?** Default is **30 days**. Controlled by `delta.logRetentionDuration` and `VACUUM` operations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4️⃣ Schema Evolution
-- MAGIC
-- MAGIC Delta Lake supports **schema evolution** — adding new columns without breaking existing queries.
-- MAGIC Our Silver layer models are configured with `on_schema_change='append_new_columns'`.

-- COMMAND ----------

-- View the current schema of the Silver incremental table
DESCRIBE TABLE workspace.iot_dev_silver.int_sensor_readings_cleaned;

-- COMMAND ----------

-- View the schema of the Gold dimension table
-- Notice the mix of source columns (from Bronze) and computed columns (from Silver/Gold)
DESCRIBE TABLE workspace.iot_dev_gold.dim_devices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### How Schema Evolution Works in dbt + Delta
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- In dbt model config:
-- MAGIC {{ config(
-- MAGIC     materialized='incremental',
-- MAGIC     on_schema_change='append_new_columns'  -- ← This enables evolution
-- MAGIC ) }}
-- MAGIC ```
-- MAGIC
-- MAGIC | `on_schema_change` | Behavior |
-- MAGIC |--------------------|----------|
-- MAGIC | `'ignore'` | New columns are silently dropped |
-- MAGIC | `'append_new_columns'` | New columns are added to the target table |
-- MAGIC | `'sync_all_columns'` | Columns are added AND removed to match source |
-- MAGIC | `'fail'` | Throw an error if schema changes |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5️⃣ Incremental Data Flow
-- MAGIC
-- MAGIC Our Silver layer uses **incremental materialization** — only new/changed rows are processed.
-- MAGIC This is critical for IoT workloads where millions of readings arrive continuously.

-- COMMAND ----------

-- Check row counts across all layers to see the data flow
SELECT 'Bronze: stg_sensor_readings' as layer, count(*) as row_count FROM workspace.iot_dev_bronze.stg_sensor_readings
UNION ALL
SELECT 'Silver: int_sensor_readings_cleaned', count(*) FROM workspace.iot_dev_silver.int_sensor_readings_cleaned
UNION ALL
SELECT 'Silver: int_device_health', count(*) FROM workspace.iot_dev_silver.int_device_health
UNION ALL
SELECT 'Gold: fct_hourly_metrics', count(*) FROM workspace.iot_dev_gold.fct_hourly_metrics
UNION ALL
SELECT 'Gold: fct_device_summary', count(*) FROM workspace.iot_dev_gold.fct_device_summary
UNION ALL
SELECT 'Gold: fct_anomaly_events', count(*) FROM workspace.iot_dev_gold.fct_anomaly_events
UNION ALL
SELECT 'Gold: dim_devices', count(*) FROM workspace.iot_dev_gold.dim_devices
ORDER BY layer;

-- COMMAND ----------

-- Show deduplication in action:
-- Bronze has 209 raw rows, Silver has fewer after removing duplicates
SELECT 
    (SELECT count(*) FROM workspace.iot_dev_bronze.stg_sensor_readings) as bronze_rows,
    (SELECT count(*) FROM workspace.iot_dev_silver.int_sensor_readings_cleaned) as silver_rows,
    (SELECT count(*) FROM workspace.iot_dev_bronze.stg_sensor_readings) - 
    (SELECT count(*) FROM workspace.iot_dev_silver.int_sensor_readings_cleaned) as duplicates_removed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incremental Model Logic
-- MAGIC ```sql
-- MAGIC -- dbt generates this for incremental models:
-- MAGIC MERGE INTO silver_table AS target
-- MAGIC USING (
-- MAGIC     SELECT * FROM bronze_view
-- MAGIC     WHERE ingested_at > (SELECT MAX(ingested_at) FROM silver_table)
-- MAGIC ) AS source
-- MAGIC ON target.reading_id = source.reading_id
-- MAGIC WHEN MATCHED THEN UPDATE SET ...
-- MAGIC WHEN NOT MATCHED THEN INSERT ...
-- MAGIC ```
-- MAGIC
-- MAGIC 💡 **Why Incremental?** Processing 209 rows is instant. Processing 209 million rows per day? Incremental cuts that to only the **new** rows since the last run.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6️⃣ Unity Catalog — Data Lineage
-- MAGIC
-- MAGIC Unity Catalog tracks **which tables read from which tables** automatically.

-- COMMAND ----------

-- View table lineage (requires Unity Catalog with lineage enabled)
-- This shows upstream/downstream dependencies — similar to the dbt DAG
SELECT
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'workspace.iot_dev_%'
ORDER BY target_table_full_name;

-- COMMAND ----------

-- View column-level lineage (which source columns feed which target columns)
SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name LIKE 'workspace.iot_dev_gold%'
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Note**: If the `system.access.table_lineage` query returns an error, lineage tracking may not be enabled on your workspace tier. This is available on **Premium** and **Enterprise** plans.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7️⃣ Delta Lake Optimization
-- MAGIC
-- MAGIC For production workloads, Delta Lake provides commands to optimize storage and query performance.

-- COMMAND ----------

-- OPTIMIZE compacts small files into larger ones (improves read performance)
-- This is critical for streaming workloads that create many small files
OPTIMIZE workspace.iot_dev_gold.fct_hourly_metrics;

-- COMMAND ----------

-- OPTIMIZE with ZORDER for predefined query patterns
-- ZORDER co-locates related data for faster filtering
OPTIMIZE workspace.iot_dev_gold.fct_device_summary ZORDER BY (device_id);

-- COMMAND ----------

-- VACUUM removes old files no longer referenced by the transaction log
-- Default retention: 7 days (protects time travel queries)
-- DRY RUN first to see what would be deleted
VACUUM workspace.iot_dev_gold.fct_device_summary DRY RUN;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimization Summary
-- MAGIC | Command | What It Does | When to Use |
-- MAGIC |---------|--------------|-------------|
-- MAGIC | `OPTIMIZE` | Compacts small files → fewer, larger files | After many small writes (streaming, incremental) |
-- MAGIC | `ZORDER BY` | Co-locates data by column(s) | Frequently filtered columns (device_id, date) |
-- MAGIC | `VACUUM` | Deletes old unreferenced files | Storage cleanup (respects retention period) |
-- MAGIC | `ANALYZE TABLE` | Updates column statistics | Improve query optimizer decisions |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8️⃣ Production Data Flow — Auto Loader & DLT
-- MAGIC
-- MAGIC In production, **seeds** are replaced by streaming ingestion:
-- MAGIC
-- MAGIC ### Auto Loader (Streaming File Ingestion)
-- MAGIC ```python
-- MAGIC # Auto Loader incrementally processes new files as they land in cloud storage
-- MAGIC df = (spark.readStream
-- MAGIC     .format("cloudFiles")
-- MAGIC     .option("cloudFiles.format", "json")
-- MAGIC     .option("cloudFiles.schemaLocation", "/checkpoints/sensors")
-- MAGIC     .load("/data/iot/raw/sensors/")
-- MAGIC )
-- MAGIC
-- MAGIC # Write to Bronze Delta table
-- MAGIC (df.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "/checkpoints/bronze_sensors")
-- MAGIC     .outputMode("append")
-- MAGIC     .toTable("workspace.iot_prod_bronze.raw_sensor_readings")
-- MAGIC )
-- MAGIC ```
-- MAGIC
-- MAGIC ### Delta Live Tables (Declarative Pipelines)
-- MAGIC ```python
-- MAGIC import dlt
-- MAGIC
-- MAGIC @dlt.table(comment="Bronze: Raw sensor readings")
-- MAGIC def bronze_sensor_readings():
-- MAGIC     return spark.readStream.format("cloudFiles") \
-- MAGIC         .option("cloudFiles.format", "json") \
-- MAGIC         .load("/data/iot/raw/")
-- MAGIC
-- MAGIC @dlt.table(comment="Silver: Cleaned readings")
-- MAGIC @dlt.expect_or_drop("valid_temperature", "temperature BETWEEN 10 AND 100")
-- MAGIC def silver_sensor_readings():
-- MAGIC     return dlt.read_stream("bronze_sensor_readings") \
-- MAGIC         .withColumn("is_anomaly", col("temperature") > 85)
-- MAGIC ```
-- MAGIC
-- MAGIC 💡 **Key Difference**: dbt operates in **batch** (run → done). Auto Loader and DLT operate in **streaming** (continuous). In production, you'd use **both**: Auto Loader/DLT for ingestion, dbt for complex transformations and testing.

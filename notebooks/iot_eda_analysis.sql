-- Databricks notebook source
-- =============================================================================
-- IoT Smart Factory — Exploratory Data Analysis (EDA)
-- =============================================================================
-- This notebook explores the Gold layer tables produced by the dbt pipeline.
-- Run this in your Databricks workspace after `dbt seed && dbt run`.
-- =============================================================================

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 🏭 IoT Smart Factory — Exploratory Data Analysis
-- MAGIC
-- MAGIC This notebook explores the **Gold layer** tables built by our dbt + Databricks pipeline.
-- MAGIC
-- MAGIC **Architecture**: Bronze (raw) → Silver (cleaned) → Gold (analytics-ready)
-- MAGIC
-- MAGIC **Tables explored**:
-- MAGIC - `fct_hourly_metrics` — Hourly sensor statistics per device
-- MAGIC - `fct_device_summary` — Daily device health scorecards
-- MAGIC - `fct_anomaly_events` — Anomaly events for alerting
-- MAGIC - `dim_devices` — Device dimension with lifetime stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Fleet Overview — Device Health Scores

-- COMMAND ----------

-- Which devices are healthy vs at risk?
SELECT
    device_name,
    device_type,
    plant_location,
    health_score,
    health_category,
    total_readings,
    anomaly_readings,
    critical_alerts,
    data_completeness_pct
FROM iot_dev_gold.fct_device_summary
ORDER BY health_score ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Anomaly Trends — Temperature by Device

-- COMMAND ----------

-- Temperature anomalies: which devices are running hot?
SELECT
    device_name,
    plant_location,
    anomaly_hour,
    actual_value,
    upper_threshold,
    breach_direction
FROM iot_dev_gold.fct_anomaly_events
WHERE metric_name = 'temperature'
ORDER BY actual_value DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Hourly Metrics — Time Series View

-- COMMAND ----------

-- Hourly average temperature across all devices
SELECT
    reading_hour,
    device_name,
    avg_value AS avg_temperature,
    anomaly_count,
    health_status
FROM iot_dev_gold.fct_hourly_metrics
WHERE metric_name = 'temperature'
ORDER BY reading_hour, device_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Device Dimension — Lifetime Statistics

-- COMMAND ----------

-- Device fleet overview with lifetime anomaly rates
SELECT
    device_name,
    device_type,
    plant_location,
    manufacturer,
    days_since_install,
    lifetime_readings,
    lifetime_anomalies,
    lifetime_anomaly_rate_pct,
    is_active
FROM iot_dev_gold.dim_devices
ORDER BY lifetime_anomaly_rate_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Alert Analysis — Resolution Times

-- COMMAND ----------

-- How quickly are alerts being resolved?
SELECT
    device_name,
    plant_location,
    COUNT(*) AS total_alerts,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN is_resolved THEN 1 ELSE 0 END) AS resolved_count,
    ROUND(AVG(minutes_to_resolve), 1) AS avg_resolution_min
FROM iot_dev_silver.int_alerts_enriched
GROUP BY device_name, plant_location
ORDER BY critical_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Data Quality — Missing Readings

-- COMMAND ----------

-- Check for missing sensor data
SELECT
    device_name,
    metric_name,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN is_missing_value THEN 1 ELSE 0 END) AS missing_count,
    ROUND(
        SUM(CASE WHEN is_missing_value THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) AS missing_pct
FROM iot_dev_silver.int_sensor_readings_cleaned
GROUP BY device_name, metric_name
HAVING missing_count > 0
ORDER BY missing_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Vibration Analysis — Predictive Maintenance Signal

-- COMMAND ----------

-- Devices with high vibration variance may indicate mechanical issues
SELECT
    device_name,
    device_type,
    plant_location,
    avg_value AS avg_vibration,
    max_value AS peak_vibration,
    stddev_value AS vibration_stddev,
    anomaly_rate_pct
FROM iot_dev_gold.fct_hourly_metrics
WHERE metric_name = 'vibration'
ORDER BY vibration_stddev DESC NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ### 💡 Key Findings
-- MAGIC
-- MAGIC - **DEV004 (Furnace-A4, Chicago)** consistently shows critical temperature readings (88-98°F) and high vibration
-- MAGIC - **DEV009 (Boiler-C9, Austin)** is a secondary concern with borderline temperature anomalies
-- MAGIC - Data completeness is high (>99%) across the fleet except for targeted null readings
-- MAGIC - Average alert resolution time provides a baseline for SLA monitoring

# 🏭 IoT Smart Factory — dbt + Databricks

A production-style **dbt** project on **Databricks** implementing the **Medallion Architecture** (Bronze → Silver → Gold) for IoT sensor data from a smart factory environment.

## Architecture

```
                    ┌──────────────────────────────────────────────────────┐
                    │              Databricks Lakehouse                     │
                    │                                                      │
  IoT Sensors ───►  │  ┌─────────┐    ┌──────────┐    ┌───────────┐      │  ───► Dashboards
  (10 devices)      │  │ BRONZE  │───►│  SILVER  │───►│   GOLD    │      │       & Analytics
                    │  │  Views  │    │Incremental│    │  Tables   │      │
                    │  │         │    │          │    │           │      │
                    │  │ stg_*   │    │ int_*    │    │ fct_*/dim_│      │
                    │  └─────────┘    └──────────┘    └───────────┘      │
                    │                                                      │
                    │  Delta Lake  •  Unity Catalog  •  SQL Warehouse     │
                    └──────────────────────────────────────────────────────┘
```

## Medallion Layers

| Layer | Schema | Models | Materialization | Purpose |
|-------|--------|--------|-----------------|---------|
| **Bronze** | `*_bronze` | `stg_sensor_readings`, `stg_devices`, `stg_alerts` | View | Raw data with standardized types |
| **Silver** | `*_silver` | `int_sensor_readings_cleaned`, `int_device_health`, `int_alerts_enriched` | Incremental | Cleaned, deduplicated, anomaly-flagged |
| **Gold** | `*_gold` | `fct_hourly_metrics`, `fct_device_summary`, `fct_anomaly_events`, `dim_devices` | Table | Business-ready analytics |

## IoT Scenario

**10 devices** across **3 factory plants** (Detroit, Chicago, Austin) report:
- 🌡️ **Temperature** — Overheating detection
- 📳 **Vibration** — Mechanical failure prediction
- 💧 **Humidity** — Environmental monitoring
- 🔽 **Pressure** — System integrity checks

The pipeline detects anomalies using configurable thresholds, computes device **health scores (0-100)**, and generates alert-ready tables.

## Key Features Demonstrated

### dbt Features
- ✅ **Incremental models** with `unique_key` and `on_schema_change`
- ✅ **Source freshness** monitoring (`warn_after: 24h`, `error_after: 48h`)
- ✅ **Custom generic tests** (`value_in_range`)
- ✅ **Singular tests** (orphan alert detection)
- ✅ **dbt vars** for configurable anomaly thresholds
- ✅ **Surrogate keys** via `dbt_utils.generate_surrogate_key`
- ✅ **Custom schema routing** (medallion-prefixed schemas)
- ✅ **Auto-generated documentation** with DAG visualization

### Databricks Features
- ✅ **Delta Lake** — ACID transactions, schema evolution, time travel
- ✅ **Unity Catalog** — Three-level namespace (`catalog.schema.table`)
- ✅ **SQL Warehouse** — Serverless compute via HTTP path
- ✅ **Photon Engine** — Optimized query execution
- ✅ **Liquid Clustering** — Next-gen partitioning for Delta tables
- ✅ **Databricks Notebooks** — EDA notebook included (`notebooks/iot_eda_analysis.sql`)
- ✅ **Databricks Workflows** — Orchestration for scheduled dbt runs

## Quick Start

### Prerequisites
- Databricks workspace (free Community Edition works)
- Python 3.9+
- `dbt-databricks` adapter

### Setup

```bash
# 1. Install dbt with Databricks adapter
pip install dbt-databricks

# 2. Configure your connection
cp profiles.yml.template ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Databricks host, token, and HTTP path

# 3. Install dbt packages
dbt deps

# 4. Verify connection
dbt debug

# 5. Load seed data (simulated IoT readings)
dbt seed

# 6. Build all models (Bronze → Silver → Gold)
dbt run

# 7. Run all tests
dbt test

# 8. Generate and view documentation
dbt docs generate
dbt docs serve
```

### Optional: Run the EDA Notebook
1. Import `notebooks/iot_eda_analysis.sql` into your Databricks workspace
2. Attach to a SQL Warehouse or cluster
3. Run all cells to explore the Gold layer tables

## Project Structure

```
├── README.md
├── dbt_project.yml                 # Project configuration + anomaly thresholds
├── packages.yml                    # dbt_utils dependency
├── profiles.yml.template           # Databricks connection template (Git-safe)
├── .gitignore
├── .github/workflows/dbt_ci.yml    # CI pipeline: deps → seed → run → test
│
├── seeds/                          # Simulated IoT data
│   ├── raw_sensor_readings.csv     # 209 sensor readings (includes dirty data)
│   ├── raw_devices.csv             # 10 factory devices
│   ├── raw_alerts.csv              # 24 alert events
│   └── schema.yml                  # Seed column types
│
├── models/
│   ├── overview.md                 # dbt docs homepage
│   ├── staging/      ← BRONZE
│   │   ├── sources.yml             # Source definitions + freshness
│   │   ├── stg_sensor_readings.sql
│   │   ├── stg_devices.sql
│   │   ├── stg_alerts.sql
│   │   └── schema.yml
│   ├── intermediate/ ← SILVER
│   │   ├── int_sensor_readings_cleaned.sql  # Incremental + anomaly detection
│   │   ├── int_device_health.sql            # Hourly aggregations
│   │   ├── int_alerts_enriched.sql          # Alert enrichment
│   │   └── schema.yml
│   └── marts/        ← GOLD
│       ├── fct_hourly_metrics.sql           # Time-series fact
│       ├── fct_device_summary.sql           # Daily health scorecard
│       ├── fct_anomaly_events.sql           # Anomaly fact
│       ├── dim_devices.sql                  # Device dimension (SCD)
│       └── schema.yml
│
├── macros/
│   ├── generate_schema_name.sql    # Medallion schema routing
│   └── test_value_in_range.sql     # Custom generic test
│
├── tests/
│   └── assert_no_orphan_alerts.sql # Singular test
│
└── notebooks/
    └── iot_eda_analysis.sql        # Databricks SQL notebook for EDA
```

## Data Quality

The seed data intentionally includes data quality issues to demonstrate handling:
- **Null values**: `R169` (missing temperature), `R183` (missing humidity)
- **Duplicate records**: `R189` is a duplicate of `R013`
- **Anomalous readings**: DEV004 (Furnace) consistently exceeds temperature thresholds

The Silver layer handles these via:
- Row deduplication using `row_number()` window function
- `is_missing_value` flag for null tracking
- `is_anomaly` flag using configurable threshold vars


## License

This is a sample project for demonstration purposes.

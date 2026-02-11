{% docs __overview__ %}

# 🏭 IoT Smart Factory — dbt + Databricks

## Medallion Architecture

This project implements a **medallion architecture** (Bronze → Silver → Gold) to process IoT sensor data from a smart factory environment using **dbt** on **Databricks**.

### Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   BRONZE    │     │    SILVER    │     │    GOLD     │
│  (Staging)  │ ──► │(Intermediate)│ ──► │   (Marts)   │
│             │     │              │     │             │
│ Raw sensor  │     │ Cleaned &    │     │ Business-   │
│ readings,   │     │ deduplicated,│     │ ready facts │
│ devices,    │     │ anomaly      │     │ & dims for  │
│ alerts      │     │ detection,   │     │ dashboards  │
│             │     │ enrichment   │     │             │
└─────────────┘     └──────────────┘     └─────────────┘
     Views           Incremental           Tables
```

### Key Features Demonstrated

| Feature | Where |
|---------|-------|
| **Incremental Models** | Silver layer — processes only new IoT data |
| **Source Freshness** | Bronze sources — monitors data arrival |
| **Custom Tests** | `value_in_range` — validates sensor bounds |
| **Surrogate Keys** | Gold layer — `dbt_utils.generate_surrogate_key` |
| **Schema Routing** | Custom macro routes to `bronze/silver/gold` schemas |
| **dbt Vars** | Configurable anomaly thresholds |

### IoT Scenario

10 devices across 3 factory plants (Detroit, Chicago, Austin) report temperature, vibration, humidity, and pressure readings. The pipeline detects anomalies, computes device health scores, and produces dashboard-ready tables.

{% enddocs %}

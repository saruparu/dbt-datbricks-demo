# Databricks notebook source

# MAGIC %md
# MAGIC # 🎛️ Databricks Control Flow — Workflow Orchestration
# MAGIC
# MAGIC This notebook demonstrates how **control flow** works in Databricks using **Workflows** (Jobs API).
# MAGIC We define multi-task pipelines with dependencies, conditional execution, loops, and error handling.
# MAGIC
# MAGIC **This notebook is a reference guide** — it defines workflow JSON payloads that can be submitted
# MAGIC to the Databricks Jobs API. You can also create these visually via the **Workflows UI**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Workflow Overview
# MAGIC
# MAGIC A **Databricks Workflow** (Job) consists of:
# MAGIC
# MAGIC | Component | Description |
# MAGIC |-----------|-------------|
# MAGIC | **Job** | A named collection of tasks with a schedule |
# MAGIC | **Task** | A single unit of work (notebook, SQL, dbt, JAR, Python) |
# MAGIC | **Dependencies** | `depends_on` links between tasks (DAG) |
# MAGIC | **Trigger** | Schedule (cron), file arrival, or manual |
# MAGIC | **Parameters** | Key-value pairs passed between tasks |
# MAGIC | **Clusters** | Compute resources for each task |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Multi-Task Workflow — dbt Pipeline
# MAGIC
# MAGIC This defines a complete dbt pipeline as a Databricks Workflow with 4 chained tasks:
# MAGIC
# MAGIC ```
# MAGIC dbt_seed → dbt_run → dbt_test → generate_docs
# MAGIC ```

# COMMAND ----------

import json

# Define a multi-task dbt pipeline workflow
dbt_pipeline_workflow = {
    "name": "iot_dbt_pipeline",
    "description": "IoT Smart Factory — dbt Bronze → Silver → Gold pipeline",

    # --- Schedule: Run every day at 6 AM UTC ---
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "UTC",
        "pause_status": "PAUSED"
    },

    # --- Email notifications on failure ---
    "email_notifications": {
        "on_failure": ["team@company.com"]
    },

    # --- Task definitions ---
    "tasks": [
        {
            "task_key": "dbt_seed",
            "description": "Load seed data (CSV → Delta tables)",
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": ["dbt seed --full-refresh"],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "dbt_run_bronze_silver",
            "description": "Build Bronze and Silver layer models",
            "depends_on": [{"task_key": "dbt_seed"}],
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": [
                    "dbt run --select tag:bronze tag:silver"
                ],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            },
            "timeout_seconds": 1200
        },
        {
            "task_key": "dbt_test_silver",
            "description": "Run data quality tests on Silver layer",
            "depends_on": [{"task_key": "dbt_run_bronze_silver"}],
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": ["dbt test --select tag:silver"],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "dbt_run_gold",
            "description": "Build Gold layer models (only if Silver tests pass)",
            "depends_on": [{"task_key": "dbt_test_silver"}],
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": ["dbt run --select tag:gold"],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            },
            "timeout_seconds": 1200
        }
    ],

    # --- Max concurrent runs ---
    "max_concurrent_runs": 1
}

print("✅ Multi-Task dbt Pipeline Workflow Definition:")
print(json.dumps(dbt_pipeline_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task Dependency Graph
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐    ┌────────────────────┐    ┌────────────────┐    ┌──────────────┐
# MAGIC │ dbt_seed │───►│ dbt_run_bronze_    │───►│ dbt_test_      │───►│ dbt_run_gold │
# MAGIC │          │    │ silver             │    │ silver         │    │              │
# MAGIC └──────────┘    └────────────────────┘    └────────────────┘    └──────────────┘
# MAGIC                                                 │
# MAGIC                                                 │ (if tests fail)
# MAGIC                                                 ▼
# MAGIC                                          ❌ Pipeline stops
# MAGIC                                          Gold is NOT built
# MAGIC ```
# MAGIC
# MAGIC 💡 **This IS control flow** — the Gold layer only runs if Silver tests pass. If `dbt_test_silver` fails, `dbt_run_gold` is automatically skipped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Conditional Execution (IF/ELSE Tasks)
# MAGIC
# MAGIC Databricks Workflows support **condition tasks** that branch based on expressions.
# MAGIC This lets you create IF/ELSE logic in your pipeline.

# COMMAND ----------

# Workflow with conditional branching
conditional_workflow = {
    "name": "iot_conditional_pipeline",
    "description": "Pipeline with conditional Gold refresh based on anomaly count",

    "tasks": [
        # Task 1: Check anomaly count
        {
            "task_key": "check_anomaly_count",
            "description": "Count anomalies in Silver layer",
            "sql_task": {
                "query": {
                    "query_text": """
                        SELECT CASE 
                            WHEN count(*) > 50 THEN 'HIGH_ANOMALIES'
                            ELSE 'NORMAL'
                        END as anomaly_status
                        FROM workspace.iot_dev_silver.int_sensor_readings_cleaned
                        WHERE is_anomaly = true
                    """
                },
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        },

        # Task 2: CONDITION — Branch based on anomaly count
        {
            "task_key": "evaluate_anomalies",
            "description": "IF anomaly count > 50 → run full refresh, ELSE → normal incremental",
            "depends_on": [{"task_key": "check_anomaly_count"}],
            "condition_task": {
                "op": "EQUAL_TO",
                "left": "{{tasks.check_anomaly_count.values.anomaly_status}}",
                "right": "HIGH_ANOMALIES"
            }
        },

        # Task 3a: IF TRUE → Full refresh Gold (recompute everything)
        {
            "task_key": "full_refresh_gold",
            "description": "Full refresh Gold tables due to high anomaly count",
            "depends_on": [
                {"task_key": "evaluate_anomalies", "outcome": "true"}
            ],
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": ["dbt run --select tag:gold --full-refresh"],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        },

        # Task 3b: IF FALSE → Normal incremental run
        {
            "task_key": "incremental_gold",
            "description": "Normal incremental Gold update",
            "depends_on": [
                {"task_key": "evaluate_anomalies", "outcome": "false"}
            ],
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": ["dbt run --select tag:gold"],
                "schema": "iot_dev",
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        }
    ]
}

print("✅ Conditional Workflow (IF/ELSE):")
print(json.dumps(conditional_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Flow
# MAGIC ```
# MAGIC ┌──────────────────┐     ┌──────────────────────┐
# MAGIC │ check_anomaly_   │────►│ evaluate_anomalies   │
# MAGIC │ count            │     │ (CONDITION TASK)     │
# MAGIC └──────────────────┘     └──────────┬───────────┘
# MAGIC                                     │
# MAGIC                          ┌──────────┴──────────┐
# MAGIC                          │                     │
# MAGIC                     outcome=true          outcome=false
# MAGIC                          │                     │
# MAGIC                          ▼                     ▼
# MAGIC                 ┌────────────────┐    ┌────────────────┐
# MAGIC                 │ full_refresh_  │    │ incremental_   │
# MAGIC                 │ gold           │    │ gold           │
# MAGIC                 │ (--full-refresh│    │ (normal run)   │
# MAGIC                 └────────────────┘    └────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ ForEach Task — Loop Over Plant Locations
# MAGIC
# MAGIC The **ForEach** task iterates over a list of values and runs a nested task for each.
# MAGIC Perfect for running the same logic per plant, per device type, or per date partition.

# COMMAND ----------

# Workflow with ForEach loop over plant locations
foreach_workflow = {
    "name": "iot_per_plant_analysis",
    "description": "Run device health analysis for each plant location",

    "tasks": [
        # Task 1: Get list of plant locations
        {
            "task_key": "get_plant_locations",
            "description": "Retrieve distinct plant locations",
            "sql_task": {
                "query": {
                    "query_text": """
                        SELECT DISTINCT plant_location 
                        FROM workspace.iot_dev_gold.dim_devices
                    """
                },
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        },

        # Task 2: ForEach plant → run analysis
        {
            "task_key": "analyze_per_plant",
            "description": "Run health analysis for each plant",
            "depends_on": [{"task_key": "get_plant_locations"}],
            "for_each_task": {
                "inputs": "{{tasks.get_plant_locations.values}}",
                "task": {
                    "task_key": "plant_health_check",
                    "sql_task": {
                        "query": {
                            "query_text": """
                                SELECT 
                                    '{{input}}' as plant,
                                    count(*) as total_readings,
                                    round(avg(health_score), 1) as avg_health,
                                    sum(CASE WHEN health_category = 'critical' THEN 1 ELSE 0 END) as critical_count
                                FROM workspace.iot_dev_gold.fct_device_summary s
                                JOIN workspace.iot_dev_gold.dim_devices d 
                                    ON s.device_id = d.device_id
                                WHERE d.plant_location = '{{input}}'
                            """
                        },
                        "warehouse_id": "0b4eee1bcc7b2623"
                    }
                },
                "concurrency": 3  # Run 3 plants in parallel
            }
        },

        # Task 3: Aggregate results after all plants complete
        {
            "task_key": "aggregate_results",
            "description": "Combine per-plant results into summary",
            "depends_on": [{"task_key": "analyze_per_plant"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/iot-dbt-databricks/notebooks/aggregate_health",
                "base_parameters": {
                    "run_date": "{{job.start_time.iso_date}}"
                }
            }
        }
    ]
}

print("✅ ForEach Workflow (Loop Over Plants):")
print(json.dumps(foreach_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ForEach Flow
# MAGIC ```
# MAGIC ┌──────────────────┐     ┌─────────────────────────────────────────────┐
# MAGIC │ get_plant_       │────►│ analyze_per_plant (ForEach)                │
# MAGIC │ locations        │     │                                             │
# MAGIC │                  │     │  ┌──────────┐ ┌──────────┐ ┌──────────┐   │
# MAGIC │ Returns:         │     │  │ Detroit  │ │ Chicago  │ │ Austin   │   │
# MAGIC │ [Detroit,        │     │  │ health   │ │ health   │ │ health   │   │
# MAGIC │  Chicago,        │     │  │ check    │ │ check    │ │ check    │   │
# MAGIC │  Austin]         │     │  └──────────┘ └──────────┘ └──────────┘   │
# MAGIC └──────────────────┘     │       (runs in parallel, concurrency=3)    │
# MAGIC                          └─────────────────────┬───────────────────────┘
# MAGIC                                                │
# MAGIC                                                ▼
# MAGIC                                    ┌──────────────────────┐
# MAGIC                                    │ aggregate_results    │
# MAGIC                                    └──────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Error Handling & Retry Policies
# MAGIC
# MAGIC Databricks Workflows support **automatic retries**, **timeouts**, and **failure notifications**.

# COMMAND ----------

# Task-level error handling configuration
error_handling_config = {
    "task_key": "dbt_run_with_retries",
    "description": "dbt run with robust error handling",

    "dbt_task": {
        "project_directory": "/Workspace/Repos/iot-dbt-databricks",
        "commands": ["dbt run --select tag:gold"],
        "schema": "iot_dev",
        "warehouse_id": "0b4eee1bcc7b2623"
    },

    # --- Retry Policy ---
    "retry_on_timeout": True,
    "max_retries": 3,             # Retry up to 3 times on failure
    "min_retry_interval_millis": 30000,   # Wait 30 seconds between retries
    "timeout_seconds": 1800,       # Timeout after 30 minutes

    # --- Notification on events ---
    "email_notifications": {
        "on_start": ["data-team@company.com"],
        "on_success": ["data-team@company.com"],
        "on_failure": ["oncall@company.com", "data-team@company.com"]
    },

    # --- Health rules (SLA monitoring) ---
    "health": {
        "rules": [
            {
                "metric": "RUN_DURATION_SECONDS",
                "op": "GREATER_THAN",
                "value": 900  # Alert if run takes > 15 minutes
            }
        ]
    }
}

print("✅ Error Handling Configuration:")
print(json.dumps(error_handling_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error Handling Summary
# MAGIC | Feature | Configuration | Purpose |
# MAGIC |---------|---------------|---------|
# MAGIC | **Retries** | `max_retries: 3` | Automatically retry failed tasks |
# MAGIC | **Retry delay** | `min_retry_interval_millis: 30000` | Wait 30s between retries |
# MAGIC | **Timeout** | `timeout_seconds: 1800` | Kill task if running > 30 mins |
# MAGIC | **Notifications** | `on_failure: [email]` | Alert on failure via email/Slack |
# MAGIC | **Health rules** | `RUN_DURATION > 900s` | SLA monitoring — flag slow runs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Parameters — Passing Values Between Tasks
# MAGIC
# MAGIC Databricks Workflows support **job-level** and **task-level** parameters.
# MAGIC Tasks can reference outputs from upstream tasks.

# COMMAND ----------

# Parameterized workflow
parameterized_workflow = {
    "name": "iot_parameterized_pipeline",
    "description": "Pipeline with environment and threshold parameters",

    # Job-level parameters (can be overridden per run)
    "parameters": [
        {"name": "environment", "default": "dev"},
        {"name": "temperature_threshold", "default": "85.0"},
        {"name": "run_full_refresh", "default": "false"}
    ],

    "tasks": [
        {
            "task_key": "dbt_run_parameterized",
            "description": "Run dbt with dynamic parameters",
            "dbt_task": {
                "project_directory": "/Workspace/Repos/iot-dbt-databricks",
                "commands": [
                    # Use job parameters in dbt commands
                    "dbt run "
                    "--target {{job.parameters.environment}} "
                    "--vars '{temperature_upper: {{job.parameters.temperature_threshold}}}' "
                    + "{% if job.parameters.run_full_refresh == 'true' %}--full-refresh{% endif %}"
                ],
                "schema": "iot_{{job.parameters.environment}}",
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        },
        {
            "task_key": "log_run_metadata",
            "description": "Log run metadata for audit trail",
            "depends_on": [{"task_key": "dbt_run_parameterized"}],
            "sql_task": {
                "query": {
                    "query_text": """
                        INSERT INTO workspace.iot_audit.run_log
                        VALUES (
                            current_timestamp(),
                            '{{job.parameters.environment}}',
                            '{{job.parameters.temperature_threshold}}',
                            '{{tasks.dbt_run_parameterized.result_state}}'
                        )
                    """
                },
                "warehouse_id": "0b4eee1bcc7b2623"
            }
        }
    ]
}

print("✅ Parameterized Workflow:")
print(json.dumps(parameterized_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameter Reference Syntax
# MAGIC | Syntax | Resolves To |
# MAGIC |--------|-------------|
# MAGIC | `{{job.parameters.environment}}` | Job-level parameter value |
# MAGIC | `{{tasks.task_key.values.column}}` | Output value from a SQL task |
# MAGIC | `{{tasks.task_key.result_state}}` | Task result: SUCCESS, FAILED, etc. |
# MAGIC | `{{job.start_time.iso_date}}` | Job start date (2025-01-15) |
# MAGIC | `{{job.run_id}}` | Unique run ID |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Creating a Workflow via the Jobs API
# MAGIC
# MAGIC You can create workflows programmatically using the Databricks REST API.
# MAGIC Uncomment and run the cell below to create the multi-task dbt pipeline workflow.

# COMMAND ----------

# # Uncomment to create the workflow via API
# # Requires: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
#
# import requests
# import os
#
# host = os.environ.get("DATABRICKS_HOST", "https://dbc-d45fd83c-044d.cloud.databricks.com")
# token = os.environ.get("DATABRICKS_TOKEN", dbutils.secrets.get("dbt", "token"))
#
# response = requests.post(
#     f"{host}/api/2.1/jobs/create",
#     headers={"Authorization": f"Bearer {token}"},
#     json=dbt_pipeline_workflow
# )
#
# if response.status_code == 200:
#     job_id = response.json()["job_id"]
#     print(f"✅ Workflow created! Job ID: {job_id}")
#     print(f"   View at: {host}/jobs/{job_id}")
# else:
#     print(f"❌ Error: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Comparison — Orchestration Options
# MAGIC
# MAGIC | Feature | Databricks Workflows | Apache Airflow | dbt Cloud |
# MAGIC |---------|---------------------|----------------|-----------|
# MAGIC | **Native dbt support** | ✅ dbt task type | ⚠️ via BashOperator | ✅ Built-in |
# MAGIC | **Conditional logic** | ✅ IF/ELSE tasks | ✅ BranchOperator | ❌ |
# MAGIC | **ForEach loops** | ✅ ForEach task | ⚠️ Dynamic DAGs | ❌ |
# MAGIC | **Retry policies** | ✅ Per-task config | ✅ Per-task config | ✅ |
# MAGIC | **Managed service** | ✅ Fully managed | ❌ Self-hosted (or MWAA) | ✅ Fully managed |
# MAGIC | **Cost** | Included with Databricks | Separate infra | $100+/month |
# MAGIC | **Delta Lake integration** | ✅ Native | ⚠️ Via provider | ⚠️ Via adapter |
# MAGIC | **Git integration** | ✅ Repos | ✅ DAGs in Git | ✅ Native |
# MAGIC
# MAGIC 💡 **Bottom line**: If you're already on Databricks, use **Databricks Workflows** to orchestrate dbt.
# MAGIC It eliminates the need for a separate orchestrator (Airflow, Prefect) and provides native dbt task support.

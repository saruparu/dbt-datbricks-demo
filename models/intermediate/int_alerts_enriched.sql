-- =============================================================================
-- SILVER LAYER: Enriched alerts with device context and reading data
-- Joins alerts with device metadata and the triggering sensor reading.
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='alert_id',
        on_schema_change='append_new_columns'
    )
}}

with alerts as (

    select * from {{ ref('stg_alerts') }}

    {% if is_incremental() %}
        where alert_ts > (select max(alert_ts) from {{ this }})
    {% endif %}

),

devices as (

    select * from {{ ref('stg_devices') }}

),

enriched_alerts as (

    select
        a.alert_id,
        a.device_id,
        a.alert_type,
        a.severity,
        a.metric_name,
        a.threshold_value,
        a.actual_value,
        a.alert_ts,
        a.resolved_at,
        a.resolution_notes,
        a.is_resolved,

        -- Device context
        d.device_name,
        d.device_type,
        d.plant_location,
        d.factory_zone,
        d.manufacturer,
        d.days_since_install,

        -- Derived: time to resolution
        case
            when a.is_resolved then
                round(
                    (unix_timestamp(a.resolved_at) - unix_timestamp(a.alert_ts)) / 60.0,
                    1
                )
            else null
        end as minutes_to_resolve,

        -- Severity scoring for prioritization
        case a.severity
            when 'critical' then 3
            when 'warning'  then 2
            when 'info'     then 1
            else 0
        end as severity_score,

        current_timestamp() as _dbt_processed_at

    from alerts a
    left join devices d
        on a.device_id = d.device_id

)

select * from enriched_alerts

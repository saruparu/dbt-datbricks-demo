-- =============================================================================
-- GOLD LAYER: Hourly metrics fact table
-- Pre-aggregated hourly statistics per device per metric.
-- Optimized for BI dashboards and time-series analysis.
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

with device_health as (

    select * from {{ ref('int_device_health') }}

),

hourly_summary as (

    select
        device_health_key,
        device_id,
        device_name,
        device_type,
        plant_location,
        factory_zone,
        metric_name,
        reading_hour,

        reading_count,
        avg_value,
        min_value,
        max_value,
        stddev_value,
        anomaly_count,
        anomaly_rate_pct,
        latest_reading_ts,

        -- Time dimensions for BI slicing
        date(reading_hour)                               as reading_date,
        hour(reading_hour)                               as hour_of_day,
        dayofweek(reading_hour)                          as day_of_week,

        -- Health classification
        case
            when anomaly_rate_pct >= 50.0 then 'critical'
            when anomaly_rate_pct >= 25.0 then 'degraded'
            when anomaly_rate_pct > 0     then 'warning'
            else 'healthy'
        end as health_status,

        current_timestamp() as _dbt_created_at

    from device_health

)

select * from hourly_summary

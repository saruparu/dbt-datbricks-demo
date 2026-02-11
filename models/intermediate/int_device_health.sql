-- =============================================================================
-- SILVER LAYER: Device health metrics
-- Aggregates sensor readings per device to compute health indicators.
-- Rolling statistics help identify devices trending toward failure.
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='device_health_key',
        on_schema_change='append_new_columns'
    )
}}

with cleaned_readings as (

    select * from {{ ref('int_sensor_readings_cleaned') }}
    where is_missing_value = false

    {% if is_incremental() %}
        and _dbt_processed_at > (select max(_dbt_processed_at) from {{ this }})
    {% endif %}

),

device_metrics as (

    select
        {{ dbt_utils.generate_surrogate_key(['device_id', 'metric_name', "date_trunc('hour', reading_ts)"]) }}
            as device_health_key,

        device_id,
        device_name,
        device_type,
        plant_location,
        factory_zone,
        metric_name,
        date_trunc('hour', reading_ts)                   as reading_hour,

        -- Statistical aggregations
        count(*)                                          as reading_count,
        round(avg(metric_value), 2)                       as avg_value,
        round(min(metric_value), 2)                       as min_value,
        round(max(metric_value), 2)                       as max_value,
        round(stddev(metric_value), 2)                    as stddev_value,

        -- Anomaly metrics
        sum(case when is_anomaly then 1 else 0 end)       as anomaly_count,
        round(
            sum(case when is_anomaly then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 1
        )                                                 as anomaly_rate_pct,

        -- Latest reading
        max(reading_ts)                                   as latest_reading_ts,

        current_timestamp()                                as _dbt_processed_at

    from cleaned_readings
    group by
        device_id, device_name, device_type,
        plant_location, factory_zone, metric_name,
        date_trunc('hour', reading_ts)

)

select * from device_metrics

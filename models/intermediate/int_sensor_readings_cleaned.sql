-- =============================================================================
-- SILVER LAYER: Cleaned and deduplicated sensor readings
-- Key features showcased:
--   * Incremental materialization (efficient for streaming IoT data)
--   * Schema evolution support (on_schema_change)
--   * Data quality filtering (remove nulls and duplicates)
--   * Anomaly detection using configurable thresholds (dbt vars)
--   * Device enrichment via join
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='reading_id',
        on_schema_change='append_new_columns',
        tags=['silver', 'incremental']
    )
}}

with sensor_readings as (

    select * from {{ ref('stg_sensor_readings') }}

    {% if is_incremental() %}
        -- Only process new readings since the last run
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

devices as (

    select * from {{ ref('stg_devices') }}

),

-- Remove duplicates: keep the latest ingested record per reading_id
deduplicated as (

    select
        *,
        row_number() over (
            partition by reading_id
            order by ingested_at desc
        ) as _row_num
    from sensor_readings

),

cleaned as (

    select
        r.reading_id,
        r.device_id,
        r.metric_name,
        r.metric_value,
        r.reading_ts,
        r.ingested_at,

        -- Enrich with device metadata
        d.device_name,
        d.device_type,
        d.plant_location,
        d.factory_zone,
        d.manufacturer,

        -- Data quality flags
        case
            when r.metric_value is null then true
            else false
        end as is_missing_value,

        -- Anomaly detection using configurable thresholds
        case
            when r.metric_name = 'temperature'
                 and r.metric_value is not null
                 and (r.metric_value > {{ var('temperature_upper') }}
                      or r.metric_value < {{ var('temperature_lower') }})
                then true
            when r.metric_name = 'vibration'
                 and r.metric_value is not null
                 and r.metric_value > {{ var('vibration_upper') }}
                then true
            when r.metric_name = 'humidity'
                 and r.metric_value is not null
                 and (r.metric_value > {{ var('humidity_upper') }}
                      or r.metric_value < {{ var('humidity_lower') }})
                then true
            when r.metric_name = 'pressure'
                 and r.metric_value is not null
                 and (r.metric_value > {{ var('pressure_upper') }}
                      or r.metric_value < {{ var('pressure_lower') }})
                then true
            else false
        end as is_anomaly,

        current_timestamp() as _dbt_processed_at

    from deduplicated r
    left join devices d
        on r.device_id = d.device_id
    where r._row_num = 1  -- Keep only the latest version of each reading

)

select * from cleaned

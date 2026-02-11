-- =============================================================================
-- BRONZE LAYER: Staging model for raw sensor readings
-- Mirrors raw data with standardized column names and types.
-- Materialized as VIEW for cost efficiency (no data duplication).
-- =============================================================================

with source as (

    select * from {{ source('iot_raw', 'raw_sensor_readings') }}

),

renamed as (

    select
        reading_id,
        device_id,
        lower(trim(metric_name))                       as metric_name,
        cast(metric_value as double)                    as metric_value,
        cast(reading_ts as timestamp)                   as reading_ts,
        cast(ingested_at as timestamp)                  as ingested_at,

        -- Metadata columns for lineage tracking
        current_timestamp()                             as _dbt_loaded_at,
        '{{ invocation_id }}'                           as _dbt_invocation_id

    from source

)

select * from renamed

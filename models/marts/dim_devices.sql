-- =============================================================================
-- GOLD LAYER: Device dimension table
-- SCD-style device dimension with surrogate keys.
-- Demonstrates dbt_utils.generate_surrogate_key pattern.
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'dimension']
    )
}}

with devices as (

    select * from {{ ref('stg_devices') }}

),

device_reading_stats as (

    select
        device_id,
        count(*)                                         as total_readings,
        sum(case when is_anomaly then 1 else 0 end)     as total_anomalies,
        sum(case when is_missing_value then 1 else 0 end) as total_missing,
        min(reading_ts)                                  as first_reading_ts,
        max(reading_ts)                                  as last_reading_ts
    from {{ ref('int_sensor_readings_cleaned') }}
    group by device_id

),

dim_devices as (

    select
        {{ dbt_utils.generate_surrogate_key(['d.device_id']) }}
            as device_key,

        d.device_id,
        d.device_name,
        d.device_type,
        d.plant_location,
        d.factory_zone,
        d.install_date,
        d.manufacturer,
        d.firmware_version,
        d.is_active,
        d.days_since_install,

        -- Lifetime stats
        coalesce(s.total_readings, 0)                    as lifetime_readings,
        coalesce(s.total_anomalies, 0)                   as lifetime_anomalies,
        coalesce(s.total_missing, 0)                     as lifetime_missing_readings,
        s.first_reading_ts,
        s.last_reading_ts,

        -- Derived metrics
        case
            when s.total_readings > 0
                then round(
                    s.total_anomalies * 100.0 / s.total_readings, 2
                )
            else 0
        end                                              as lifetime_anomaly_rate_pct,

        -- SCD metadata (Type 1 for simplicity, can extend to Type 2)
        current_timestamp()                              as _valid_from,
        cast(null as timestamp)                          as _valid_to,
        true                                             as _is_current,
        current_timestamp()                              as _dbt_created_at

    from devices d
    left join device_reading_stats s
        on d.device_id = s.device_id

)

select * from dim_devices

-- =============================================================================
-- GOLD LAYER: Anomaly events fact table
-- Flattened anomaly events with full device context.
-- Designed for alert dashboards and root cause analysis.
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

with anomalous_readings as (

    select * from {{ ref('int_sensor_readings_cleaned') }}
    where is_anomaly = true

),

alerts as (

    select * from {{ ref('int_alerts_enriched') }}
    where alert_type = 'threshold_breach'

),

anomaly_events as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.reading_id']) }}
            as anomaly_event_key,

        r.reading_id,
        r.device_id,
        r.device_name,
        r.device_type,
        r.plant_location,
        r.factory_zone,
        r.manufacturer,

        r.metric_name,
        r.metric_value                                    as actual_value,

        -- Thresholds for context
        case r.metric_name
            when 'temperature' then {{ var('temperature_upper') }}
            when 'vibration'   then {{ var('vibration_upper') }}
            when 'humidity'    then {{ var('humidity_upper') }}
            when 'pressure'    then {{ var('pressure_upper') }}
        end                                               as upper_threshold,

        case r.metric_name
            when 'temperature' then {{ var('temperature_lower') }}
            when 'humidity'    then {{ var('humidity_lower') }}
            when 'pressure'    then {{ var('pressure_lower') }}
            else null
        end                                               as lower_threshold,

        r.reading_ts                                      as anomaly_ts,

        -- Time dimensions
        date(r.reading_ts)                                as anomaly_date,
        hour(r.reading_ts)                                as anomaly_hour,

        -- Breach direction
        case
            when r.metric_name in ('temperature', 'humidity', 'pressure')
                and r.metric_value > (
                    case r.metric_name
                        when 'temperature' then {{ var('temperature_upper') }}
                        when 'humidity'    then {{ var('humidity_upper') }}
                        when 'pressure'    then {{ var('pressure_upper') }}
                    end
                )
                then 'above_upper'
            else 'below_lower'
        end                                               as breach_direction,

        current_timestamp()                               as _dbt_created_at

    from anomalous_readings r

)

select * from anomaly_events

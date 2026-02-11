-- =============================================================================
-- BRONZE LAYER: Staging model for alert events
-- Standardizes and enriches alert data with resolution status.
-- =============================================================================

with source as (

    select * from {{ source('iot_raw', 'raw_alerts') }}

),

renamed as (

    select
        alert_id,
        device_id,
        lower(trim(alert_type))                          as alert_type,
        lower(trim(severity))                            as severity,
        lower(trim(metric_name))                         as metric_name,
        cast(threshold_value as double)                  as threshold_value,
        cast(actual_value as double)                     as actual_value,
        cast(alert_ts as timestamp)                      as alert_ts,
        cast(resolved_at as timestamp)                   as resolved_at,
        resolution_notes,

        -- Derived columns
        case
            when resolved_at is not null then true
            else false
        end                                              as is_resolved,

        -- Metadata
        current_timestamp()                              as _dbt_loaded_at

    from source

)

select * from renamed

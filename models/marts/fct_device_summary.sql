-- =============================================================================
-- GOLD LAYER: Device summary fact table
-- Daily device health scorecard for operational dashboards.
-- Provides at-a-glance view of fleet health.
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

with cleaned_readings as (

    select * from {{ ref('int_sensor_readings_cleaned') }}

),

alerts as (

    select * from {{ ref('int_alerts_enriched') }}

),

-- Daily reading statistics per device
daily_readings as (

    select
        device_id,
        device_name,
        device_type,
        plant_location,
        factory_zone,
        date(reading_ts) as reading_date,

        count(*)                                              as total_readings,
        sum(case when is_missing_value then 1 else 0 end)     as missing_readings,
        sum(case when is_anomaly then 1 else 0 end)           as anomaly_readings,

        round(
            (count(*) - sum(case when is_missing_value then 1 else 0 end)) * 100.0
            / nullif(count(*), 0), 1
        )                                                      as data_completeness_pct,

        round(
            sum(case when is_anomaly then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 1
        )                                                      as anomaly_rate_pct,

        min(reading_ts)                                        as first_reading_ts,
        max(reading_ts)                                        as last_reading_ts

    from cleaned_readings
    group by
        device_id, device_name, device_type,
        plant_location, factory_zone, date(reading_ts)

),

-- Daily alert counts per device
daily_alerts as (

    select
        device_id,
        date(alert_ts) as alert_date,
        count(*)                                               as total_alerts,
        sum(case when severity = 'critical' then 1 else 0 end) as critical_alerts,
        sum(case when severity = 'warning'  then 1 else 0 end) as warning_alerts,
        sum(case when is_resolved then 1 else 0 end)           as resolved_alerts,

        round(avg(
            case when minutes_to_resolve is not null
                 then minutes_to_resolve
            end
        ), 1)                                                   as avg_resolution_minutes

    from alerts
    group by device_id, date(alert_ts)

),

device_summary as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.device_id', 'r.reading_date']) }}
            as device_summary_key,

        r.device_id,
        r.device_name,
        r.device_type,
        r.plant_location,
        r.factory_zone,
        r.reading_date,

        -- Reading stats
        r.total_readings,
        r.missing_readings,
        r.anomaly_readings,
        r.data_completeness_pct,
        r.anomaly_rate_pct,
        r.first_reading_ts,
        r.last_reading_ts,

        -- Alert stats
        coalesce(a.total_alerts, 0)                            as total_alerts,
        coalesce(a.critical_alerts, 0)                         as critical_alerts,
        coalesce(a.warning_alerts, 0)                          as warning_alerts,
        coalesce(a.resolved_alerts, 0)                         as resolved_alerts,
        a.avg_resolution_minutes,

        -- Overall health score (0-100)
        round(
            greatest(0,
                100
                - (coalesce(a.critical_alerts, 0) * 20)
                - (coalesce(a.warning_alerts, 0) * 5)
                - (r.anomaly_rate_pct * 0.5)
                - ((100 - r.data_completeness_pct) * 0.3)
            ), 0
        )                                                      as health_score,

        -- Health category
        case
            when coalesce(a.critical_alerts, 0) > 3 then 'critical'
            when coalesce(a.critical_alerts, 0) > 0 then 'at_risk'
            when r.anomaly_rate_pct > 10 then 'degraded'
            else 'healthy'
        end                                                    as health_category,

        current_timestamp()                                    as _dbt_created_at

    from daily_readings r
    left join daily_alerts a
        on r.device_id = a.device_id
        and r.reading_date = a.alert_date

)

select * from device_summary

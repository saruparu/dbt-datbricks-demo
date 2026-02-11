-- =============================================================================
-- Singular test: Assert no orphan alerts exist
-- Every alert must reference a valid device_id that exists in stg_devices.
-- This catches data integrity issues early in the pipeline.
-- =============================================================================

select
    a.alert_id,
    a.device_id
from {{ ref('stg_alerts') }} a
left join {{ ref('stg_devices') }} d
    on a.device_id = d.device_id
where d.device_id is null

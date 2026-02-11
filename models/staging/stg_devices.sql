-- =============================================================================
-- BRONZE LAYER: Staging model for device master data
-- Standardizes column names and adds dbt metadata.
-- =============================================================================

with source as (

    select * from {{ source('iot_raw', 'raw_devices') }}

),

renamed as (

    select
        device_id,
        device_name,
        lower(trim(device_type))                        as device_type,
        location                                         as plant_location,
        zone                                             as factory_zone,
        cast(install_date as date)                       as install_date,
        manufacturer,
        firmware_version,
        cast(is_active as boolean)                       as is_active,

        -- Derived columns
        datediff(current_date(), cast(install_date as date))  as days_since_install,

        -- Metadata
        current_timestamp()                              as _dbt_loaded_at

    from source

)

select * from renamed

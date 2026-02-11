-- =============================================================================
-- Custom schema name generation for medallion architecture
-- Routes models to bronze_, silver_, gold_ prefixed schemas.
--
-- Example: if target schema = "iot_dev" and model schema = "bronze",
--          the actual schema becomes "iot_dev_bronze"
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

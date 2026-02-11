-- =============================================================================
-- Generic test: assert a column's values fall within an expected range
-- Usage in schema.yml:
--   tests:
--     - value_in_range:
--         min_value: 0
--         max_value: 100
-- =============================================================================

{% test value_in_range(model, column_name, min_value, max_value) %}

    select
        {{ column_name }} as invalid_value,
        count(*) as occurrences
    from {{ model }}
    where {{ column_name }} is not null
      and (
          {{ column_name }} < {{ min_value }}
          or {{ column_name }} > {{ max_value }}
      )
    group by {{ column_name }}

{% endtest %}

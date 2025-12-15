{% macro safe_json_array(col) %}
    case
        when {{ col }} is null then null

        -- looks like an array
        when {{ col }} ~ '^\s*\[.*\]\s*$' then
            (
              replace({{ col }}, '''', '"')::jsonb
            )

        else null
    end
{% endmacro %}

{% macro safe_json(col) %}
    case
        when {{ col }} is null then null
        when {{ col }} ~ '^\s*\{.*\}\s*$' then
            replace({{ col }}, '''', '"')::jsonb
        else null
    end
{% endmacro %}
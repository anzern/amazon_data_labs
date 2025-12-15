{% macro select_with_cast(columns) %}
{% for col, dtype in columns -%}
    cast({{ col }} as {{ dtype }}) as {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}

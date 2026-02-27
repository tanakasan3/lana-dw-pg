{#
  Cross-platform math functions.
#}

{#
  Safe division that returns NULL instead of error on divide by zero.
  
  BigQuery: safe_divide(numerator, denominator)
  Postgres: numerator / NULLIF(denominator, 0)
#}

{% macro safe_divide(numerator, denominator) %}
  {% if target.type == 'bigquery' %}
    safe_divide({{ numerator }}, {{ denominator }})
  {%- elif target.type == 'postgres' %}
    ({{ numerator }})::numeric / nullif(({{ denominator }})::numeric, 0)
  {%- else %}
    ({{ numerator }})::numeric / nullif(({{ denominator }})::numeric, 0)
  {%- endif %}
{% endmacro %}

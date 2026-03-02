{#
  Cross-platform math functions.
#}

{#
  Safe division that returns NULL instead of error on divide by zero.
  
  BigQuery: safe_divide(numerator, denominator)
  Postgres: numerator / NULLIF(denominator, 0)
#}
{%- macro safe_divide(numerator, denominator) -%}
  {%- if target.type == 'bigquery' -%}
    safe_divide({{ numerator }}, {{ denominator }})
  {%- elif target.type == 'postgres' -%}
    ({{ numerator }})::numeric / nullif(({{ denominator }})::numeric, 0)
  {%- else -%}
    ({{ numerator }})::numeric / nullif(({{ denominator }})::numeric, 0)
  {%- endif -%}
{%- endmacro -%}


{#
  Safe multiplication that returns NULL if either operand is NULL.
  
  BigQuery: safe_multiply(a, b)
  Postgres: (a * b) - native behavior is fine, but wrap for consistency
#}
{%- macro safe_multiply(a, b) -%}
  {%- if target.type == 'bigquery' -%}
    safe_multiply({{ a }}, {{ b }})
  {%- elif target.type == 'postgres' -%}
    (({{ a }})::numeric * ({{ b }})::numeric)
  {%- else -%}
    (({{ a }})::numeric * ({{ b }})::numeric)
  {%- endif -%}
{%- endmacro -%}

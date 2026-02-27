{#
  PostgreSQL workaround for BigQuery's SELECT * EXCEPT syntax.
  
  This macro generates a select list excluding specified columns.
  It requires explicit column specification since PostgreSQL doesn't 
  support EXCEPT for column selection.
  
  For simple deduplication patterns, consider using DISTINCT ON instead.
#}

{# 
  select_star_except: Generates column list excluding specified columns.
  
  Usage: {{ select_star_except('table_alias', ['col_to_exclude1', 'col_to_exclude2']) }}
  
  NOTE: This requires the caller to know the table structure.
  For staging models using row_number() for deduplication, 
  consider rewriting with DISTINCT ON.
#}
{% macro select_star_except(columns, exclude) %}
  {% set result_cols = [] %}
  {% for col in columns %}
    {% if col not in exclude %}
      {% do result_cols.append(col) %}
    {% endif %}
  {% endfor %}
  {{ result_cols | join(', ') }}
{% endmacro %}

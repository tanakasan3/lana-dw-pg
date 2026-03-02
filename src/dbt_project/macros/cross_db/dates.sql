{#
  Cross-platform date/time functions.
#}

{#
  timestamp_micros: Convert microseconds since epoch to timestamp.
  
  BigQuery: timestamp_micros(int64)
  Postgres: to_timestamp(double precision)
#}
{%- macro timestamp_micros(micros_expr) -%}
  {%- if target.type == 'bigquery' -%}
    timestamp_micros({{ micros_expr }})
  {%- elif target.type == 'postgres' -%}
    to_timestamp({{ micros_expr }}::double precision / 1000000.0)
  {%- else -%}
    to_timestamp({{ micros_expr }}::double precision / 1000000.0)
  {%- endif -%}
{%- endmacro -%}


{#
  Cross-platform date formatting functions.
  
  BigQuery: to_char(format, date)
  Postgres: to_char(date, format)
  
  Format string translation:
  - %Y → YYYY (4-digit year)
  - %m → MM (2-digit month)
  - %d → DD (2-digit day)
#}
{%- macro to_char(format_str, date_expr) -%}
  {%- if target.type == 'bigquery' -%}
    to_char('{{ format_str }}', {{ date_expr }})
  {%- elif target.type == 'postgres' -%}
    {%- set pg_format = format_str | replace('%Y', 'YYYY') | replace('%m', 'MM') | replace('%d', 'DD') -%}
    to_char({{ date_expr }}, '{{ pg_format }}')
  {%- else -%}
    to_char({{ date_expr }}, '{{ format_str | replace('%Y', 'YYYY') | replace('%m', 'MM') | replace('%d', 'DD') }}')
  {%- endif -%}
{%- endmacro -%}


{#
  Cross-platform date_trunc.
  
  BigQuery: date_trunc(date_expr, part) - date first, part second
  Postgres: date_trunc(part, date_expr) - part first, date second
#}
{%- macro xdb_date_trunc(part, date_expr) -%}
  {%- if target.type == 'bigquery' -%}
    date_trunc({{ date_expr }}, {{ part }})
  {%- elif target.type == 'postgres' -%}
    date_trunc('{{ part }}', {{ date_expr }})
  {%- else -%}
    date_trunc('{{ part }}', {{ date_expr }})
  {%- endif -%}
{%- endmacro -%}


{#
  last_day: Get the last day of the period containing a date.
  
  BigQuery: last_day(date, MONTH)
  Postgres: (date_trunc('month', date) + interval '1 month' - interval '1 day')::date
#}
{%- macro last_day(date_expr, period='month') -%}
  {%- if target.type == 'bigquery' -%}
    last_day({{ date_expr }}, {{ period }})
  {%- elif target.type == 'postgres' -%}
    (date_trunc('{{ period }}', {{ date_expr }}::date) + interval '1 {{ period }}' - interval '1 day')::date
  {%- else -%}
    (date_trunc('{{ period }}', {{ date_expr }}::date) + interval '1 {{ period }}' - interval '1 day')::date
  {%- endif -%}
{%- endmacro -%}

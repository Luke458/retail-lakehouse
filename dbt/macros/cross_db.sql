{#- SQL that differs between DuckDB and Snowflake. One macro per difference, so the
    models stay one copy (adapter.dispatch picks snowflake__ on Snowflake). -#}

{#- True when any row is true; false (not null) for an empty or all-null group. -#}
{% macro any_true(expr) -%}
  {{ return(adapter.dispatch("any_true", "retail_lakehouse")(expr)) }}
{%- endmacro %}
{% macro default__any_true(expr) -%} coalesce(bool_or({{ expr }}), false) {%- endmacro %}
{% macro snowflake__any_true(expr) -%} coalesce(boolor_agg({{ expr }}), false) {%- endmacro %}

{#- Values joined with sep in order_by order; null when there are none (Snowflake's listagg returns ''). -#}
{% macro ordered_string_agg(expr, sep, order_by) -%}
  {{ return(adapter.dispatch("ordered_string_agg", "retail_lakehouse")(expr, sep, order_by)) }}
{%- endmacro %}
{% macro default__ordered_string_agg(expr, sep, order_by) -%}
  string_agg({{ expr }}, '{{ sep }}' order by {{ order_by }})
{%- endmacro %}
{% macro snowflake__ordered_string_agg(expr, sep, order_by) -%}
  nullif(listagg({{ expr }}, '{{ sep }}') within group (order by {{ order_by }}), '')
{%- endmacro %}

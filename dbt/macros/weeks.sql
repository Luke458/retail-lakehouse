{#- Which weeks a gold model may read: the run's week, and only if the gate approved it. -#}
{% macro week_filter(column="week_ending") -%}
  {%- if var("week_ending") is not none %}
    {{ column }} = date '{{ var("week_ending") }}'
  {%- else %}
    true
  {%- endif %}
  {%- if var("require_gate") %}
    and {{ column }} in ({{ approved_weeks() }})
  {%- endif %}
{%- endmacro %}

{#- A week is approved when its latest gate decision approves it (verity, or a person's release). -#}
{% macro approved_weeks() -%}
  select week_ending
  from (
    select cast(week_ending as date) as week_ending, approved,
           row_number() over (partition by week_ending order by decided_at desc) as latest
    from {{ source("gate", "decisions") }}
  )
  where latest = 1 and approved
{%- endmacro %}

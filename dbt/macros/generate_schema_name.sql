{#
  dbt 默认把 schema 拼成「默认 schema_自定义 schema」（如 main_silver）。
  这里改成：配了自定义 schema 就直接用它（silver / gold），没配就用默认的。
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

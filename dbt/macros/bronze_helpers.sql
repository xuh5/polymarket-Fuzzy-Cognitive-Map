{# 读 bronze 的公共片段，每个 staging 模型都用 #}

{# 归档是 at-least-once，崩溃重启可能写出重复行：同一个 partition + offset 只留一行 #}
{% macro dedup_kafka() -%}
    qualify row_number() over (partition by kafka_partition, kafka_offset order by kafka_timestamp) = 1
{%- endmacro %}

{# Polymarket 的毫秒时间戳字符串 → UTC 时间 #}
{% macro ms_to_ts(expr) -%}
    epoch_ms(cast({{ expr }} as bigint)) at time zone 'UTC'
{%- endmacro %}

{# 通用测试：列的值必须在 [min_value, max_value] 之间（null 不算违规，非空另用 not_null 测）。查出的行 = 违规的行 #}
{% test between(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

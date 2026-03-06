{% test regex_match(model, column_name, regex) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_like({{ column_name }}, regex)
{% endtest %}

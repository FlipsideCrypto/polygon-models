{% macro create_tasks() %}
    {% if target.database == 'POLYGON' %}
        {{ task_get_abis('resume') }};
    {% else %}
        {{ task_get_abis('suspend') }};
    {% endif %}
{% endmacro %}
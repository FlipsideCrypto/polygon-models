{% macro create_tasks() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% if target.database == 'POLYGON' %}
        {# {{ task_get_abis() }}; #}
    {% endif %}
{% endif %}
{% endmacro %}
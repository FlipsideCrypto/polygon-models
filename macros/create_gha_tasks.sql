{% macro create_gha_tasks() %}
  {% if var("UPDATE_GHA_TASKS") %}
    {% if target.database == 'POLYGON' %}
         {{ generate_snowflake_tasks() }}; 
    {% endif %}
{% endif %}
{% endmacro %}
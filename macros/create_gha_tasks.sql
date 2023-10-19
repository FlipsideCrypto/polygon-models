{% macro create_gha_tasks() %}
  {% if var("UPDATE_GHA_TASKS") %}
    {% if target.database == 'POLYGON' %}
         {{ trigger_dummy_gha() }}; 
    {% endif %}
{% endif %}
{% endmacro %}
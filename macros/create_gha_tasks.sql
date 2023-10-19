{% macro create_gha_tasks() %}
  {% if var("CREATE_GHA_TASKS") %}
    {% if target.database == 'POLYGON' %}
         {{ trigger_dummy_gha() }}; 
    {% endif %}
{% endif %}
{% endmacro %}
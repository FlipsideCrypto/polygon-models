{% macro trigger_dummy_gha() %}
  {% set sql %}
    EXECUTE IMMEDIATE 
    'CREATE OR REPLACE TASK silver.trigger_dummy_workflow 
    WAREHOUSE = DBT_CLOUD
    SCHEDULE = \'USING CRON 0,20,40 * * * * UTC\' 
    COMMENT = \'Trigger Dummy Workflow\' AS
DECLARE
  rs resultset;
  output string;
BEGIN
  rs := (SELECT github_actions.workflow_dispatches(\'FlipsideCrypto\', \'polygon-models\', \'dbt_run_dummy.yml\', NULL)::status_code::int AS status_code);
  SELECT LISTAGG($1, ';') INTO :output FROM TABLE(result_scan(LAST_QUERY_ID())) LIMIT 1;
  CALL SYSTEM$SET_RETURN_VALUE(:output);
END;'
{% endset %}
  {% do run_query(sql) %}

  {% if target.database.upper() == 'POLYGON' %}
    {% set sql %}
      ALTER TASK silver.trigger_dummy_workflow RESUME;
    {% endset %}
    {% do run_query(sql) %}
  {% endif %}

{% endmacro %}

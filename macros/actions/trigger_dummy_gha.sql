{% macro trigger_dummy_gha() %}
  {% set sql %}
    EXECUTE IMMEDIATE 
    'CREATE OR REPLACE task silver.trigger_dummy_workflow 
    warehouse = DBT_CLOUD
    schedule = \'USING CRON 0,20,40 * * * * UTC\' 
    COMMENT = \'trigger dummy workflow\' AS
select
    github_actions.workflow_dispatches(
        \'FlipsideCrypto\',
        \'polygon-models\',
        \'dbt_run_dummy.yml\',
        NULL
    );'
{% endset %}
    {% do run_query(sql) %}

{% if target.database.upper() == 'POLYGON' %}
    {% set sql %}
        alter task silver.trigger_dummy_workflow resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}

{% endmacro %}
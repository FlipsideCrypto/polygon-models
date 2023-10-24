{{ config(
    materialized = 'view'
) }}

WITH workflow_list AS (

    SELECT
        'dbt_run_dummy' AS workflow_name,
        '0,20,40 * * * *' AS workflow_schedule
    UNION
    SELECT
        'dbt_run_dummy2' AS workflow_name,
        '40 * * * *' AS workflow_schedule
    UNION
    SELECT
        'dbt_run_dummy3' AS workflow_name,
        '10 * * * *' AS workflow_schedule
)
SELECT
    workflow_name,
    concat_ws('_', 'TRIGGER', UPPER(SUBSTR(workflow_name, 9)), 'WORKFLOW') AS task_name,
    workflow_schedule
FROM
    workflow_list

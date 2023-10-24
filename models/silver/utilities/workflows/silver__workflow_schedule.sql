{{ config(
    materialized = 'view'
) }}

SELECT
    'dbt_run_dummy' AS workflow_name,
    '0,20,40 * * * *' AS workflow_schedule
UNION
SELECT
    'dbt_run_dummy2' AS workflow_name,
    '40 * * * *' AS workflow_schedule

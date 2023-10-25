{{ config(
    materialized = 'view'
) }}

SELECT
    workflow_name,
    concat_ws(
        '_',
        'TRIGGER',
        UPPER(workflow_name)
    ) AS task_name,
    workflow_schedule
FROM
    {{ source(
        'github_actions',
        'workflows'
    ) }}

{{ config(
    materialized = 'view'
) }}

SELECT
    s.task_name,
    s.workflow_name,
    s.scheduled_time,
    h.return_value
FROM
    {{ ref('silver__gha_task_schedule') }}
    s
    LEFT JOIN {{ ref('silver__gha_task_history') }}
    h
    ON s.task_name = h.task_name
    AND TO_TIMESTAMP_NTZ(DATE_TRUNC('second', s.scheduled_time)) = TO_TIMESTAMP_NTZ(DATE_TRUNC('second', h.scheduled_time))
    AND h.return_value = 204
    AND h.state = 'SUCCEEDED'
ORDER BY
    task_name,
    scheduled_time

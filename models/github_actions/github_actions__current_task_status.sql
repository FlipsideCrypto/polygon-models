{{ config(
    materialized = 'view'
) }}
{{ gha_task_current_status() }}

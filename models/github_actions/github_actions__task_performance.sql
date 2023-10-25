{{ config(
    materialized = 'view'
) }}
{{ gha_task_results() }}

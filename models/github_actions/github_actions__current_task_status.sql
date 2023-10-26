{{ config(
    materialized = 'view'
) }}

{{ fsc_utils.gha_task_current_status_view() }}
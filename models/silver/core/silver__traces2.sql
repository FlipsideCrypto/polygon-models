-- depends_on: {{ ref('bronze__traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = false,
    tags = ['non_realtime']
) }}
{{ silver_traces_v1(
    full_reload_start_block = 5000000,
    full_reload_blocks = 1000000,
    partition_key = true
) }}

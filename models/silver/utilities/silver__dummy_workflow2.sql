{{ config(
    materialized = 'incremental',
    unique_key = 'run_timestamp'
) }}

SELECT
    SYSDATE() AS run_timestamp,
    1 AS dummy

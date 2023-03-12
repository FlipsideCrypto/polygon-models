{{ config(
    materialized = 'incremental',
    unique_key = "pool",
) }}

SELECT 
  decoded_flat:pair ::STRING AS pool,
  decoded_flat:token0 ::STRING AS token0,
  decoded_flat:token1 ::STRING AS token1
FROM {{ ref('silver__decoded_logs') }} 
WHERE event_name = 'PairCreated' 
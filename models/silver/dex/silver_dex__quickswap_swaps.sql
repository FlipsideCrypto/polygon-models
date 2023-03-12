{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH txs AS (

SELECT 
  block_timestamp,
  block_number,
  tx_hash,
  origin_function_signature,
  from_address AS origin_from_address,
  to_address AS origin_to_address,
  from_address AS tx_to
FROM {{ ref('silver__transactions') }}

WHERE to_address = '0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff' -- Quickswap Router Address
  AND STATUS = 'SUCCESS'
  
)

SELECT 
  l.block_number,
  t.block_timestamp,
  l.tx_hash,
  t.origin_function_signature,
  t.origin_from_address,
  t.origin_to_address,
  l.contract_address, 
  po.name AS pool_name,
  event_name,
  CASE 
   WHEN decoded_flat:amount0In ::NUMERIC = 0 THEN decoded_flat:amount1In ::NUMERIC / POW(10,18)
   ELSE decoded_flat:amount0In ::NUMERIC / POW(10,18)
  END AS amount_in,
  NULL AS amount_in_usd,
  CASE 
   WHEN decoded_flat:amount0Out ::NUMERIC = 0 THEN decoded_flat:amount1Out ::NUMERIC / POW(10,18)
   ELSE decoded_flat:amount0Out ::NUMERIC / POW(10,18)
  END AS amount_out,
  NULL AS amount_out_usd,
  decoded_flat:sender ::STRING AS sender,
  t.tx_to,
  event_index,
  'quickswap' AS platform,
  ai.token0 AS token_in,
  ai.token1 AS token_out,
  cp.name AS symbol_in,
  co.name AS symbol_out,
  _log_id
FROM {{ ref('silver__decoded_logs') }} l

JOIN txs t
  ON l.tx_hash = t.tx_hash
  AND l.block_number = t.block_number

LEFT OUTER JOIN {{ ref('silver_dex__pools') }} ai
  ON l.contract_address = ai.pool

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} cp
  ON ai.token0 = cp.address

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} co
  ON ai.token1 = co.address  

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} po
  ON l.contract_address = po.address


WHERE l.event_name = 'Swap'
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
  
),

prices AS (

SELECT
  hour,
  token_address,
  price
FROM {{ ref('core__fact_hourly_token_prices') }}
WHERE
    token_address IN (SELECT DISTINCT token0 FROM {{ ref('silver_dex__pools') }}) 
      OR token_address IN (SELECT DISTINCT token1 FROM {{ ref('silver_dex__pools') }}) 

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
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
  p0.price AS token0_price,
  p1.price AS token1_price,
  CASE 
   WHEN decoded_flat:amount0In ::NUMERIC = 0 THEN decoded_flat:amount1In ::NUMERIC / POW(10,18)
   ELSE decoded_flat:amount0In ::NUMERIC / POW(10,18)
  END AS amount_in,
  token0_price * amount_in AS amountin_usd,
  CASE 
   WHEN decoded_flat:amount0Out ::NUMERIC = 0 THEN decoded_flat:amount1Out ::NUMERIC / POW(10,18)
   ELSE decoded_flat:amount0Out ::NUMERIC / POW(10,18)
  END AS amount_out,  
  token1_price * amount_out AS amountout_usd,
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

LEFT JOIN prices p0
  ON ai.token0 = p0.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p0.hour

LEFT JOIN prices p1
  ON ai.token1 = p1.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p1.hour    

WHERE l.event_name = 'Swap'
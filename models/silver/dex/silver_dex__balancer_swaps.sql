{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_name AS (

    SELECT
      pool_name,
      poolId,
      pool_address
    FROM {{ ref('silver_dex__balancer_pools') }}
),
swaps_base AS (
SELECT
  tx_hash,
  block_number,
  contract_address,
  _inserted_timestamp,
  event_name,
  event_index,
  decoded_flat :amountIn :: INTEGER AS amountIn,
  decoded_flat :amountOut :: INTEGER AS amountOut,
  decoded_flat :poolId :: STRING AS poolId,
  decoded_flat :tokenIn :: STRING AS token_in,
  decoded_flat :tokenOut :: STRING AS token_out,
  SUBSTR(decoded_flat :poolId :: STRING, 0, 42) AS pool_address,
  _log_id,
  'balancer' AS platform
FROM {{ ref('silver__decoded_logs') }}
WHERE contract_address = LOWER('0xBA12222222228d8Ba445958a75a0704d566BF2C8')  --Balancer v2 Vault Contract
  AND event_name = 'Swap'

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

txs AS (

SELECT 
  block_timestamp,
  block_number,
  tx_hash,
  origin_function_signature,
  from_address AS origin_from_address,
  to_address AS origin_to_address,
  from_address AS tx_to,
  from_address AS sender
FROM {{ ref('silver__transactions') }}

WHERE tx_hash IN(SELECT tx_hash FROM swaps_base)    

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

contracts AS (
SELECT
  *
FROM {{ ref('core__dim_contracts') }}
WHERE decimals IS NOT NULL
  AND (address IN ( SELECT DISTINCT token_in FROM swaps_base)
       OR address IN (SELECT DISTINCT token_out FROM swaps_base))
),
prices AS (

SELECT
  hour,
  token_address,
  price
FROM {{ ref('core__fact_hourly_token_prices') }}
WHERE
    token_address IN (SELECT DISTINCT token_in FROM swaps_base) 
      OR token_address IN (SELECT DISTINCT token_out FROM swaps_base) 

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
)

SELECT
  s.tx_hash,
  s.block_number,
  t.block_timestamp,
  t.origin_function_signature,
  t.origin_from_address,
  t.origin_to_address,
  contract_address,
  _inserted_timestamp,
  s.event_name,
  event_index,
  p0.price AS token0_price,
  p1.price AS token1_price,
  amountIn AS amountIn_unadj,
  c1.decimals AS decimals_in,
  c1.symbol AS symbol_in,
  CASE
    WHEN decimals_in IS NULL THEN amountIn_unadj
    ELSE (amountIn_unadj / pow(10, decimals_in))
  END AS amount_in,
  token0_price * amount_in AS amountin_usd,
  amountOut AS amountOut_unadj,
  c2.decimals AS decimals_out,
  c2.symbol AS symbol_out,
  CASE
    WHEN decimals_out IS NULL THEN amountOut_unadj
    ELSE (amountOut_unadj / pow(10, decimals_out))
  END AS amount_out,
  token1_price * amount_out AS amountout_usd,
  pn.poolId,
  token_in,
  token_out,
  s.pool_address,
  s._log_id,
  s.platform,
  t.sender,
  t.tx_to,
  pool_name
FROM swaps_base s

LEFT JOIN txs t
  ON s.tx_hash = t.tx_hash
  AND s.block_number = t.block_number
    
LEFT JOIN contracts c1
  ON token_in = c1.address

LEFT JOIN contracts c2
  ON token_out = c2.address

LEFT JOIN pool_name pn
  ON pn.pool_address = s.pool_address

LEFT JOIN prices p0
  ON token_in = p0.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p0.hour

LEFT JOIN prices p1
  ON token_out = p1.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p1.hour    

WHERE pool_name IS NOT NULL

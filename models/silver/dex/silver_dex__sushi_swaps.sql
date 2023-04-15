{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH sushi_pairs AS (

SELECT
  pool_address,
  pool_name,
  token0_address,
  token0_decimals,
  token0_symbol,
  token1_address,
  token1_decimals,
  token1_symbol,
  platform
FROM {{ ref('silver_dex__sushi_pools') }}
WHERE platform = 'sushiswap'
),

swap_events AS (
SELECT
  block_number,
  tx_hash,
  contract_address,
  event_name,
  TRY_TO_NUMBER(decoded_flat :amount0In :: STRING) AS amount0In,
  TRY_TO_NUMBER(decoded_flat :amount1In :: STRING) AS amount1In,
  TRY_TO_NUMBER(decoded_flat :amount0Out :: STRING) AS amount0Out,
  TRY_TO_NUMBER(decoded_flat :amount1Out :: STRING) AS amount1Out,
  decoded_flat :sender :: STRING AS sender,
  decoded_flat :to :: STRING AS tx_to,
  event_index,
  _log_id,
  _inserted_timestamp
FROM {{ ref('silver__decoded_logs') }}
WHERE event_name = 'Swap'
  AND contract_address IN (SELECT DISTINCT pool_address FROM sushi_pairs)

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
  to_address AS origin_to_address
FROM {{ ref('silver__transactions') }}

WHERE tx_hash IN(SELECT tx_hash FROM swap_events)    

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

prices AS (

SELECT
  hour,
  token_address,
  price
FROM {{ ref('core__fact_hourly_token_prices') }}
WHERE
    token_address IN (SELECT DISTINCT token0_address FROM sushi_pairs) 
      OR token_address IN (SELECT DISTINCT token1_address FROM sushi_pairs) 

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

FINAL AS (
SELECT
  s.block_number,
  t.block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  s.tx_hash,
  contract_address,
  event_name,
  CASE
    WHEN amount0In <> 0
      AND amount1In <> 0
      AND token1_decimals IS NOT NULL THEN amount1In / power(10,token1_decimals) :: FLOAT
    WHEN amount0In <> 0
      AND token0_decimals IS NOT NULL THEN amount0In / power(10,token0_decimals) :: FLOAT
    WHEN amount1In <> 0
      AND token1_decimals IS NOT NULL THEN amount1In / power(10,token1_decimals) :: FLOAT
    WHEN amount0In <> 0
      AND token0_decimals IS NULL THEN amount0In
    WHEN amount1In <> 0
      AND token1_decimals IS NULL THEN amount1In
  END AS amount_in,
  CASE
    WHEN amount0Out <> 0
      AND token0_decimals IS NOT NULL THEN amount0Out / power(10,token0_decimals) :: FLOAT
    WHEN amount1Out <> 0
      AND token1_decimals IS NOT NULL THEN amount1Out / power(10,token1_decimals) :: FLOAT
    WHEN amount0Out <> 0
      AND token0_decimals IS NULL THEN amount0Out
    WHEN amount1Out <> 0
      AND token1_decimals IS NULL THEN amount1Out
  END AS amount_out,
  sender,
  tx_to,
  event_index,
  _log_id,
  platform,
  _inserted_timestamp,
  CASE
    WHEN amount0In <> 0
      AND amount1In <> 0 THEN token1_address
    WHEN amount0In <> 0 THEN token0_address
    WHEN amount1In <> 0 THEN token1_address
  END AS token_in,
  CASE
    WHEN amount0Out <> 0 THEN token0_address
    WHEN amount1Out <> 0 THEN token1_address
  END AS token_out,
  CASE
    WHEN amount0In <> 0
      AND amount1In <> 0 THEN token1_symbol
    WHEN amount0In <> 0 THEN token0_symbol
    WHEN amount1In <> 0 THEN token1_symbol
  END AS symbol_in,
  CASE
    WHEN amount0Out <> 0 THEN token0_symbol
    WHEN amount1Out <> 0 THEN token1_symbol
  END AS symbol_out,
  CASE
    WHEN amount0In <> 0
      AND amount1In <> 0 THEN token1_decimals
    WHEN amount0In <> 0 THEN token0_decimals
    WHEN amount1In <> 0 THEN token1_decimals
  END AS decimals_in,
  CASE
    WHEN amount0Out <> 0 THEN token0_decimals
    WHEN amount1Out <> 0 THEN token1_decimals
  END AS decimals_out,
  token0_decimals,
  token1_decimals,
  token0_symbol,
  token1_symbol,
  pool_name,
  pool_address
FROM swap_events s

LEFT JOIN sushi_pairs p
  ON s.contract_address = p.pool_address

LEFT JOIN txs t
  ON s.tx_hash = t.tx_hash
  AND s.block_number = t.block_number 

)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  p0.price AS token0_price,
  p1.price AS token1_price,
  CASE
    WHEN token0_decimals IS NOT NULL THEN ROUND(token0_price * amount_in,2)
  END AS amountin_usd,
  CASE
    WHEN token1_decimals IS NOT NULL THEN ROUND(token1_price * amount_out,2)
  END AS amountout_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM
    FINAL

LEFT JOIN prices p0
  ON token_in = p0.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p0.hour

LEFT JOIN prices p1
  ON token_out = p1.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p1.hour

WHERE token_in IS NOT NULL
  AND token_out IS NOT NULL

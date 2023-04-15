{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_swaps AS (

SELECT
  *,
  decoded_flat:sender :: STRING AS sender,
  decoded_flat:recipient :: STRING AS recipient,
  decoded_flat:amount0 :: FLOAT AS amount0_unadj,
  decoded_flat:amount1 :: FLOAT AS amount1_unadj,
  decoded_flat:sqrtPriceX96 :: FLOAT AS sqrtPriceX96,
  decoded_flat:liquidity :: FLOAT AS liquidity,
  decoded_flat:tick :: FLOAT AS tick
FROM {{ ref('silver__decoded_logs') }}
WHERE tx_hash IN(SELECT tx_hash from {{ ref('silver__logs') }} 
                  WHERE topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
                    AND tx_status = 'SUCCESS'
                    AND event_removed = 'false')

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

WHERE tx_hash IN(SELECT tx_hash FROM base_swaps)    

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}

),

pool_data AS (

SELECT
  token0_address,
  token1_address,
  fee,
  fee_percent,
  tick_spacing,
  pool_address,
  token0_symbol,
  token1_symbol,
  token0_decimals,
  token1_decimals,
  pool_name
FROM {{ ref('silver_dex__univ3_pools') }}

),

prices AS (

SELECT
  hour,
  token_address,
  price
FROM {{ ref('core__fact_hourly_token_prices') }}
WHERE
    token_address IN (SELECT DISTINCT token0_address FROM pool_data) 
      OR token_address IN (SELECT DISTINCT token1_address FROM pool_data) 

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

FINAL AS (

SELECT
  s.block_number,
  block_timestamp,
  s.tx_hash,
  contract_address AS pool_address,
  pool_name,
  recipient,
  sender,
  tick,
  liquidity,
  COALESCE(liquidity / pow(10,((token0_decimals + token1_decimals) / 2)),0) AS liquidity_adjusted,
  event_index,
  amount0_unadj / pow(10,COALESCE(token0_decimals,18)) AS amount0_adjusted,
  amount1_unadj / pow(10,COALESCE(token1_decimals,18)) AS amount1_adjusted,
  COALESCE(div0(ABS(amount1_adjusted), ABS(amount0_adjusted)), 0) AS price_1_0,
  COALESCE(div0(ABS(amount0_adjusted), ABS(amount1_adjusted)), 0) AS price_0_1,
  token0_address,
  token1_address,
  token0_symbol,
  token1_symbol,
  p0.price AS token0_price,
  p1.price AS token1_price,
  CASE
    WHEN token0_decimals IS NOT NULL THEN ROUND(token0_price * amount0_adjusted,2)
  END AS amount0_usd,
  CASE
    WHEN token1_decimals IS NOT NULL THEN ROUND(token1_price * amount1_adjusted,2)
  END AS amount1_usd,
  _log_id,
  _inserted_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  amount0_unadj,
  amount1_unadj,
  token0_decimals,
  token1_decimals
FROM base_swaps s
    
INNER JOIN pool_data p
  ON p.pool_address = s.contract_address

LEFT JOIN txs t  
  ON s.tx_hash = t.tx_hash
  AND s.block_number = t.block_number

LEFT JOIN prices p0
  ON p.token0_address = p0.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p0.hour

LEFT JOIN prices p1
  ON p.token1_address = p1.token_address
  AND DATE_TRUNC('hour', block_timestamp) = p1.hour
        
)

SELECT
    *
FROM FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY _inserted_timestamp DESC)) = 1

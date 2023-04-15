{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH created_pools AS (

SELECT
  block_number AS created_block,
  tx_hash AS created_tx_hash,
  decoded_flat:token0 ::STRING AS token0_address,
  decoded_flat:token1 ::STRING AS token1_address,
  decoded_flat:fee ::INTEGER AS fee,
  decoded_flat:tickSpacing :: INTEGER AS tick_spacing,
  decoded_flat:pool ::STRING AS pool_address,
  _inserted_timestamp
FROM {{ ref('silver__decoded_logs') }}
WHERE contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984' -- Univ3 Factory
  AND event_name = 'PoolCreated'

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}

),

initial_info AS (

SELECT
  contract_address,
  decoded_flat:sqrtPriceX96 ::FLOAT AS init_sqrtPriceX96,
  decoded_flat:tick ::FLOAT AS init_tick,
  pow(1.0001, init_tick) AS init_price_1_0_unadj
FROM
    {{ ref('silver__decoded_logs') }}
WHERE event_name = 'Initialize'

{% if is_incremental() %} 
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}

),

-- pivot to uni tokens model once ready
contracts AS (

SELECT
  LOWER(address) AS address,
  symbol,
  NAME,
  decimals
FROM {{ ref('core__dim_contracts') }}
WHERE decimals IS NOT NULL

),

FINAL AS (

SELECT
  created_block,
  created_tx_hash,
  token0_address,
  token1_address,
  fee :: INTEGER AS fee,
  (fee / 10000) :: FLOAT AS fee_percent,
  tick_spacing,
  pool_address,
  COALESCE(init_tick,0) AS init_tick,
  c0.decimals AS token0_decimals,
  c1.decimals AS token1_decimals,
  -- COALESCE(
  --     init_price_1_0_unadj / pow(
  --         10,
  --         token1_decimals - token0_decimals
  --     ),
  --     0
  -- ) AS init_price_1_0,
  c0.symbol AS token0_symbol,
  c1.symbol AS token1_symbol,
  c0.name AS token0_name,
  c1.name AS token1_name,
  -- p0.price AS token0_price,
  -- p1.price AS token1_price,
  -- div0(
  --     token1_price,
  --     token0_price
  -- ) AS usd_ratio,
  -- init_price_1_0 * token1_price AS init_price_1_0_usd,
  CONCAT(
      token0_symbol,
      '-',
      token1_symbol,
      ' ',
      fee,
      ' ',
      tick_spacing
  ) AS pool_name,
  _inserted_timestamp
FROM created_pools
    
LEFT JOIN initial_info
  ON pool_address = contract_address

LEFT JOIN contracts c0
  ON c0.address = token0_address

LEFT JOIN contracts c1
  ON c1.address = token1_address
        
)

SELECT
    *
FROM FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address ORDER BY _inserted_timestamp DESC)) = 1

{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = 'pool_address',
) }}

WITH sushi_pairs AS (

SELECT
  l.block_number AS creation_block,
  t.block_timestamp AS creation_time,
  l.tx_hash AS creation_tx,
  l.contract_address AS factory_address,
  'sushiswap' as platform,
  l.event_name,
  l.decoded_flat :pair :: STRING AS pool_address,
  NULL AS pool_name,
  l.decoded_flat :token0 :: STRING AS token0,
  l.decoded_flat :token1 :: STRING AS token1,
  NULL AS fee,
  NULL AS tickSpacing,
  ARRAY_CONSTRUCT(
      token0,
      token1
  ) AS tokens,
  l._log_id,
  t.ingested_at,
  1 AS model_weight,
  'events' AS model_name
FROM {{ ref('silver__decoded_logs') }} l

LEFT JOIN {{ ref('silver__transactions') }} t
  ON l.tx_hash = t.tx_hash
  AND l.block_number = t.block_number

WHERE contract_address =  lower('0xc35DADB65012eC5796536bD9864eD8773aBc74C4')
  AND event_name = 'PairCreated'

{% if is_incremental() %} 
    AND l._inserted_timestamp >= (SELECT MAX(l._inserted_timestamp) :: DATE - 2 FROM {{ this }}) 
{% endif %}
),

dedup_pools AS (
SELECT
  *
FROM sushi_pairs
WHERE pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY pool_address ORDER BY model_weight ASC)) = 1
),

contract_details AS (
SELECT
  LOWER(address) AS token_address,
  symbol,
  decimals
FROM {{ ref('core__dim_contracts') }}
WHERE address IN (SELECT DISTINCT token0 FROM dedup_pools)
                    OR address IN (SELECT DISTINCT token1 FROM dedup_pools)
),

FINAL AS (
SELECT
  creation_block,
  creation_time,
  creation_tx,
  factory_address,
  platform,
  event_name,
  pool_address,
  CASE
    WHEN pool_name IS NULL
      AND platform = 'sushiswap' THEN contract0.symbol || '-' || contract1.symbol || ' SLP'
    ELSE pool_name
  END AS pool_name,
  token0 AS token0_address,
  contract0.symbol AS token0_symbol,
  contract0.decimals AS token0_decimals,
  token1 AS token1_address,
  contract1.symbol AS token1_symbol,
  contract1.decimals AS token1_decimals,
  fee,
  tickSpacing,
  _log_id,
  ingested_at,
  tokens,
  model_name
FROM dedup_pools

LEFT JOIN contract_details AS contract0
  ON contract0.token_address = dedup_pools.token0

LEFT JOIN contract_details AS contract1
  ON contract1.token_address = dedup_pools.token1
)

SELECT
  creation_block,
  creation_time,
  creation_tx,
  factory_address,
  platform,
  event_name,
  pool_address,
  pool_name,
  token0_address,
  token0_symbol,
  token0_decimals,
  token1_address,
  token1_symbol,
  token1_decimals,
  fee,
  tickSpacing,
  _log_id,
  ingested_at,
  tokens,
  model_name
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address ORDER BY ingested_at DESC nulls last)) = 1

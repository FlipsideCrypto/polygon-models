-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated','heal']
) }}

WITH aave AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    depositor_address,
    aave_token AS protocol_market,
    aave_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'polygon' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_deposits') }}

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
comp as (
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  depositor_address,
  compound_market AS protocol_market,
  token_address,
  token_symbol,
  amount_unadj,
  amount,
  compound_version AS platform,
  'polygon' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_deposits') }}

{% if is_incremental() and 'comp' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
deposit_union as (
    SELECT
        *
    FROM
        aave
    UNION ALL
    SELECT
        *
    FROM
        comp
),
FINAL AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    CASE
      WHEN platform = 'Compound V3' THEN 'SupplyCollateral'
      WHEN platform = 'Aave V3' THEN 'Supply'
      ELSE 'Deposit'
    END AS event_name,
    protocol_market,
    depositor_address AS depositor,
    A.token_address,
    A.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * price,
      2
    ) AS amount_usd,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    deposit_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON A.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON A.token_address = C.contract_address
)
SELECT
      *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  meta={
      'database_tags':{
          'table': {
              'PROTOCOL': 'SUSHI',
              'PURPOSE': 'DEFI, DEX'
          }
      }
  }
) }}

WITH borrow_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_asset AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_asset AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
borrow AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Borrow' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Repay' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            add_asset
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            remove_asset
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
total AS (
  SELECT
    *
  FROM
    borrow
  UNION ALL
  SELECT
    *
  FROM
    add_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    repay
  UNION ALL
  SELECT
    *
  FROM
    add_coll_in_separate_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_in_separate_txn
),
eth_prices AS (
  SELECT
    token_address,
    HOUR,
    symbol,
    AVG(price) AS price
  FROM
    {{ source(
      'ethereum',
      'fact_hourly_token_prices'
    ) }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    total
)
{% else %}
  AND HOUR :: DATE >= '2020-05-05'
{% endif %}
GROUP BY
  token_address,
  HOUR,
  symbol
),
polygon_eth_crosstab AS (
  SELECT
    NAME,
    symbol,
    MAX (
      CASE
        WHEN platform_id = 'polygon-pos' THEN token_address
        ELSE ''
      END
    ) AS polygon_address,
    MAX (
      CASE
        WHEN platform = 'ethereum' THEN token_address
        ELSE ''
      END
    ) AS eth_address
  FROM
    {{ source(
      'symbols_cross_tab',
      'MARKET_ASSET_METADATA'
    ) }}
  GROUP BY
    1,
    2
  HAVING
    polygon_address <> ''
    AND eth_address <> ''
  ORDER BY
    1,
    2
),
polygon_prices AS (
  SELECT
    DISTINCT ep.token_address,
    ep.hour,
    ep.symbol,
    ep.price,
    CASE
      WHEN pec.polygon_address = '0x0000000000000000000000000000000000001010' THEN '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270'
      ELSE pec.polygon_address
    END AS polygon_address
  FROM
    eth_prices ep
    LEFT JOIN polygon_eth_crosstab pec
    ON ep.token_address = pec.eth_Address
),
labels AS (
  SELECT
    *
  FROM
    {{ ref('sushi__dim_kashi_pairs') }}
)
SELECT
  A.block_timestamp,
  A.block_number,
  A.tx_hash,
  A.action,
  A.origin_from_address,
  A.origin_to_address,
  A.origin_function_signature,
  A.borrower2 AS borrower,
  A.borrower_is_a_contract,
  A.lending_pool_address,
  b.pair_name AS lending_pool,
  A.asset,
  CASE
  when action in ('add collateral','Remove collateral') then b.collateral_symbol
  else b.asset_symbol 
  end AS symbol,
  CASE
  when b.collateral_decimals is null THEN a.amount
  when b.asset_decimals is null then a.amount
  WHEN b.collateral_decimals is not null and action = 'add collateral' THEN (A.amount/ pow(10, b.collateral_decimals))
  WHEN b.collateral_decimals is not null and action = 'Remove collateral' THEN (A.amount/ pow(10, b.collateral_decimals))
  WHEN b.asset_decimals is not null and action = 'Borrow' then (A.amount/ pow(10, b.asset_decimals))
  WHEN b.asset_decimals is not null and action = 'Repay' then (A.amount/ pow(10, b.asset_decimals))
  END AS amount,
  CASE
    WHEN action = 'add collateral' THEN (
      A.amount * C.price / pow(
        10,
        b.collateral_decimals
      )
    )
    WHEN action = 'Remove collateral' THEN (
      A.amount * C.price / pow(
        10,
        b.collateral_decimals
      )
    )
    ELSE (A.amount * C.price / pow(10, b.asset_decimals))
  END AS amount_USD,
  A._log_id,
  _inserted_timestamp
FROM
  total A
  LEFT JOIN polygon_prices C
  ON LOWER(
    A.asset
  ) = LOWER(
    C.polygon_address
  )
  AND DATE_TRUNC(
    'hour',
    A.block_timestamp
  ) = C.hour
  LEFT JOIN labels b
  ON A.lending_pool_address = b.pair_address

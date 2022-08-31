{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

with borrow_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

repay_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
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
borrow as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Borrow' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as borrower, 
        concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when borrower = borrower2 then 'no' 
        else 'yes' end as borrower_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from borrow_txns)
and CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))  in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
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
    select pair_address from {{ ref('sushi__dim_kashi_pairs') }} 
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


repay as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Repay' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40))  as asset, 
        concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40))  as Lending_pool_address, 
        origin_from_address as borrower, 
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40))  as borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when borrower = borrower2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from repay_txns)
and CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} ) 
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
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
select pair_address from {{ ref('sushi__dim_kashi_pairs') }}
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
        from {{ ref('silver__logs') }}
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
select pair_address from {{ ref('sushi__dim_kashi_pairs') }}
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
        from {{ ref('silver__logs') }}
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
select pair_address from {{ ref('sushi__dim_kashi_pairs') }}
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
        case when pec.polygon_address = '0x0000000000000000000000000000000000001010' then '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' 
else pec.polygon_address end AS polygon_address
    FROM
        eth_prices ep
        LEFT JOIN polygon_eth_crosstab pec
        ON ep.token_address = pec.eth_Address
),




labels as (
select *
from {{ ref('sushi__dim_kashi_pairs') }}
)

select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.Borrower2 as Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
b.pair_name as lending_pool,
a.asset,
b.asset_symbol as symbol,
a.amount,
case when action = 'add collateral' then (a.amount* c.price/pow(10,b.collateral_decimals))
when action = 'Remove collateral' then (a.amount* c.price/pow(10,b.collateral_decimals))
else (a.amount* c.price/pow(10,b.asset_decimals)) end as amount_USD,
a._log_id,
_inserted_timestamp
from total a
LEFT JOIN polygon_prices c
ON LOWER(a.asset) = LOWER(
    c.polygon_address
)
AND DATE_TRUNC(
    'hour',
    a.block_timestamp
) = c.hour
left join labels b 
on a.Lending_pool_address = b.pair_address





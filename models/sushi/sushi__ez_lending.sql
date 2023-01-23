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

with lending_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

unlending_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Lending as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Deposit' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Lender, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lender2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from lending_txns)
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

Withdraw as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Withdraw' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Lender, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lender2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from unlending_txns)
and CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} ) 
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Final as (
select * from Lending
union all
select * from Withdraw
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
        Final
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
a.asset,
a.Lender2 as depositor,
a.lender_is_a_contract,
a.lending_pool_address,
a.event_index,
case when b.asset_decimals is null then a.amount else (a.amount/pow(10,b.asset_decimals)) end as amount,
(a.amount* c.price)/pow(10,b.asset_decimals) as amount_USD,
b.pair_name as lending_pool,
b.asset_symbol as symbol,
a._log_id,
_inserted_timestamp
from FINAL a
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



{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}


WITH swap_events AS (
    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        TRY_TO_NUMBER(
            event_inputs :amount0In :: STRING
        ) AS amount0In,
        TRY_TO_NUMBER(
            event_inputs :amount1In :: STRING
        ) AS amount1In,
        TRY_TO_NUMBER(
            event_inputs :amount0Out :: STRING
        ) AS amount0Out,
        TRY_TO_NUMBER(
            event_inputs :amount1Out :: STRING
        ) AS amount1Out,
        event_inputs :sender :: STRING AS sender,
        event_inputs :to :: STRING AS tx_to,
        event_index,
        _log_id,
        _inserted_timestamp
    FROM
         {{ ref('silver__logs') }}
where event_name = 'Swap'
AND tx_status = 'SUCCESS'
AND contract_address IN (  SELECt DISTINCT pool_address
    FROM
        {{ ref('sushi__dim_dex_pools') }})

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

), 

FINAL AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_name,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NOT NULL THEN amount0In / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NULL THEN amount0In
            WHEN amount1In <> 0
            AND token1_decimals IS NULL THEN amount1In
        END AS amount_in,
        CASE
            WHEN amount0Out <> 0
            AND token0_decimals IS NOT NULL THEN amount0Out / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1Out <> 0
            AND token1_decimals IS NOT NULL THEN amount1Out / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0Out <> 0
            AND token0_decimals IS NULL THEN amount0Out
            WHEN amount1Out <> 0
            AND token1_decimals IS NULL THEN amount1Out
        END AS amount_out,
        sender,
        tx_to,
        event_index,
        _log_id,
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
        _inserted_timestamp
    FROM
        swap_events
        LEFT JOIN  {{ ref('sushi__dim_dex_pools') }} bb
        ON swap_events.contract_address = bb.pool_address)
        , 

Eth_prices AS (
    SELECT
        token_address,
        HOUR,
        symbol,
        AVG(price) AS price
    FROM
        {{ source(
            'ethereum',
            'FACT_HOURLY_TOKEN_PRICES'
        ) }} 
    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        swap_events
)
{% else %}
    AND HOUR :: DATE >= '2020-05-05'
{% endif %}

GROUP BY
    token_address,
    HOUR, symbol),
    

Polygon_Eth_crosstab as
(select 
 name, 
 symbol, 
max (case 
     when platform_id = 'polygon-pos' then token_address 
    else '' end) as polygon_address, 
max (case 
    when platform = 'ethereum' then token_address 
        else '' end) as eth_address
 
from         {{ source(
            'symbols_cross_tab',
            'MARKET_ASSET_METADATA'
        ) }} 
group by 1,2
having polygon_address <> '' and eth_address <> ''
order by 1,2 
),

Polygon_prices as 
(
    select 
distinct
ep.token_address,
ep.hour,
ep.symbol,
ep.price,
pec.polygon_address as polygon_address
from Eth_prices ep
left join Polygon_Eth_crosstab pec
on ep.token_address = pec.eth_Address )


 SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'sushiswap' as platform,
    pool_name,
    event_name,
    amount_in,
    CASE
        WHEN decimals_in IS NOT NULL and amount_in * pIn.price <= 5 * amount_out * pOut.price and amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_in * pIn.price
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN decimals_out IS NOT NULL and amount_in * pIn.price <= 5 * amount_out * pOut.price and amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_out * pOut.price
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
FROM FINAL wp
left join polygon_prices pIn
    on    lower(token_in) = lower(pIn.polygon_address)
    and   date_trunc('hour',wp.block_timestamp) = pIn.hour
left join polygon_prices pOut
    on    lower(token_out) = lower(pOut.polygon_address)
    and   date_trunc('hour',wp.block_timestamp) = pOut.hour



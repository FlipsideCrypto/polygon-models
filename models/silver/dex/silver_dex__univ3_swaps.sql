{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_swaps AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        event_inputs:sender :: STRING AS sender,
        event_inputs:recipient :: STRING AS recipient,
        event_inputs:amount0 :: FLOAT AS amount0_unadj,
        event_inputs:amount1 :: FLOAT AS amount1_unadj,
        event_inputs:sqrtPriceX96 :: FLOAT AS sqrtPriceX96,
        event_inputs:liquidity :: FLOAT AS liquidity,
        event_inputs:tick :: FLOAT AS tick
    FROM
        {{ ref('silver__logs') }}
    WHERE topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
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
    FROM
        {{ ref('silver_dex__univ3_pools') }}
),
FINAL AS (
    SELECT
        'polygon' AS blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS pool_address,
        pool_name,
        recipient,
        sender,
        tick,
        liquidity,
        COALESCE(
            liquidity / pow(
                10,
                (
                    (
                        token0_decimals + token1_decimals
                    ) / 2
                )
            ),
            0
        ) AS liquidity_adjusted,
        event_index,
        amount0_unadj / pow(
            10,
            COALESCE(
                token0_decimals,
                18
            )
        ) AS amount0_adjusted,
        amount1_unadj / pow(
            10,
            COALESCE(
                token1_decimals,
                18
            )
        ) AS amount1_adjusted,
        COALESCE(div0(ABS(amount1_adjusted), ABS(amount0_adjusted)), 0) AS price_1_0,
        COALESCE(div0(ABS(amount0_adjusted), ABS(amount1_adjusted)), 0) AS price_0_1,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        -- p0.price AS token0_price,
        -- p1.price AS token1_price,
        -- CASE
        --     WHEN token0_decimals IS NOT NULL THEN ROUND(
        --         token0_price * amount0_adjusted,
        --         2
        --     )
        -- END AS amount0_usd,
        -- CASE
        --     WHEN token1_decimals IS NOT NULL THEN ROUND(
        --         token1_price * amount1_adjusted,
        --         2
        --     )
        -- END AS amount1_usd,
        _log_id,
        _inserted_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        amount0_unadj,
        amount1_unadj,
        token0_decimals,
        token1_decimals
    FROM
        base_swaps
        
        INNER JOIN pool_data
        ON pool_data.pool_address = base_swaps.contract_address
        
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

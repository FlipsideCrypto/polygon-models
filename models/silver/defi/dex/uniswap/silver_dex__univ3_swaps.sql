{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH base_swaps AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount0_unadj,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount1_unadj,
        utils.udf_hex_to_int(
            's2c', 
            segmented_data [2] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidity,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: FLOAT AS tick,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE > '2021-12-01'
        AND topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_succeeded
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
pool_data AS (
    SELECT
        token0_address,
        token1_address,
        fee,
        fee_percent,
        tick_spacing,
        pool_address
    FROM
        {{ ref('silver_dex__univ3_pools') }}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS pool_address,
        recipient,
        sender,
        fee,
        tick,
        tick_spacing,
        liquidity,
        event_index,
        token0_address,
        token1_address,
        _log_id,
        _inserted_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        amount0_unadj,
        amount1_unadj
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

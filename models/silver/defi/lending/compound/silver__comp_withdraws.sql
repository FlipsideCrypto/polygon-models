{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}


WITH 
comp_assets as (
    SELECT
        compound_market_address,
        compound_market_name,
        compound_market_symbol,
        compound_market_decimals,
        underlying_asset_address,
        underlying_asset_name,
        underlying_asset_symbol,
        underlying_asset_decimals
    FROM
        {{ ref('silver__comp_asset_details') }}
),
withdraw AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        l.contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        'base' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON token_address = C.contract_address
    WHERE
        topics [0] = '0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16' --WithdrawCollateral
        AND l.contract_address IN (SELECT DISTINCT(compound_market_address) FROM comp_assets)

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    compound_market,
    depositor_address,
    w.token_address,
    w.token_symbol,
    withdraw_amount AS amount_unadj,
    withdraw_amount / pow(
        10,
        w.token_decimals
    ) AS amount,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraw w qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

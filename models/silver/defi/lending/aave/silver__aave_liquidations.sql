{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH 
atoken_meta AS (
    SELECT
        atoken_address,
        aave_version_pool,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_tokens') }}
),
liquidation AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateral_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS debt_asset,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS debt_to_cover_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS liquidated_amount,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS liquidator_address,
        CASE
            WHEN contract_address = '0x794a61358d6845594f94dc1db02a252b5b4814ad' THEN 'Aave V3'
            WHEN contract_address = '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf' THEN 'Aave V2'
            ELSE 'ERROR'
        END AS aave_version,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286',
            '0x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address IN (SELECT distinct(aave_version_pool) from atoken_meta)
AND tx_status = 'SUCCESS' --excludes failed txs
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
    collateral_asset,
    amc.atoken_address AS collateral_aave_token,
    liquidated_amount AS amount_unadj,
    liquidated_amount / pow(
        10,
        amc.atoken_decimals
    ) AS amount,
    debt_asset,
    amd.atoken_address AS debt_aave_token,
    liquidator_address AS liquidator,
    borrower_address AS borrower,
    aave_version AS platform,
    amc.underlying_symbol AS collateral_token_symbol,
    amd.underlying_symbol AS debt_token_symbol,
    'polygon' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    liquidation
    LEFT JOIN atoken_meta amc
    ON liquidation.collateral_asset = amc.underlying_address
    LEFT JOIN atoken_meta amd
    ON liquidation.debt_asset = amd.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{{ config(
    materialized = 'incremental',
    unique_key = "dvm"
) }}

WITH pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS dvm,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs2') }}
    WHERE
        contract_address = '0x79887f65f83bdf15bcc8736b5e5bcdb48fb8fe13' --factory
        AND topics [0] :: STRING = '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND dvm NOT IN (
    SELECT
        DISTINCT dvm
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    creator,
    dvm,
    _log_id,
    _inserted_timestamp
FROM
    pools

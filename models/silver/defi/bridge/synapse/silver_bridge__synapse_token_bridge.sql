{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'synapse' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"chainId" :: STRING
        ) AS chainId,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"token" :: STRING AS token,
        decoded_flat,
        event_removed,
        tx_status,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c',
            --redeem
            '0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b' --deposit
        )
        AND contract_address IN (
            '0x8f5bbb2bb8c2ee94639e55d5f41de9b4839c1280',
            '0x2119a5c9279a13ec0de5e30d572b316f1cfca567',
            '0x0efc29e196da2e81afe96edd041bedcdf9e74893',
            '0x5f06745ee8a2001198a379bafbd0361475f3cfc3',
            '0x7103a324f423b8a4d4cc1c4f2d5b374af4f0bab5'
        )
        AND origin_to_address IS NOT NULL
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    NAME AS platform,
    amount,
    origin_from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    chainId AS destination_chain_id,
    token AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt

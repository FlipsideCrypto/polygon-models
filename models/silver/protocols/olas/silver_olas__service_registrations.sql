{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH registry_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        CASE
            WHEN topic_0 = '0xb34c1e02384201736eb4693b9b173306cb41bff12f15894dea5773088e9a3b1c' THEN 'CreateService'
            WHEN topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'Transfer'
        END AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0xe3607b00e75f6405248323a9417ff6b39b244b50' --Service Registry (AUTONOLAS-SERVICE-V1)
        AND topic_0 IN (
            '0xb34c1e02384201736eb4693b9b173306cb41bff12f15894dea5773088e9a3b1c',
            --CreateService (for services)
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        event_name,
        DATA,
        segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_3
            )
        ) AS id,
        _log_id,
        _inserted_timestamp
    FROM
        registry_evt
    WHERE
        event_name = 'Transfer'
),
multisigs AS (
    SELECT
        DISTINCT multisig_address,
        id,
        contract_address
    FROM
        {{ ref('silver_olas__create_service_multisigs') }}
        qualify(ROW_NUMBER() over (PARTITION BY multisig_address
    ORDER BY
        block_timestamp DESC)) = 1 --get latest service multisig address
),
services AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.origin_function_signature,
        r.origin_from_address,
        r.origin_to_address,
        r.contract_address,
        r.event_index,
        r.topic_0,
        r.topic_1,
        r.topic_2,
        r.topic_3,
        r.event_name,
        r.data,
        r.segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(r.topic_1)) AS service_id,
        CONCAT(
            '0x',
            r.segmented_data [0] :: STRING
        ) AS config_hash,
        t.from_address,
        t.to_address AS owner_address,
        m.multisig_address,
        r._log_id,
        r._inserted_timestamp
    FROM
        registry_evt r
        LEFT JOIN transfers t
        ON r.tx_hash = t.tx_hash
        AND r.contract_address = t.contract_address
        AND service_id = t.id
        LEFT JOIN multisigs m
        ON r.contract_address = m.contract_address
        AND service_id = m.id
    WHERE
        r.event_name = 'CreateService'
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    owner_address,
    multisig_address,
    service_id,
    config_hash,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS service_registration_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    services qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

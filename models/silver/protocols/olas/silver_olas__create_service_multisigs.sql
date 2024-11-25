{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

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
    'CreateMultisigWithAgents' AS event_name,
    DATA,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            topic_1
        )
    ) AS id,
    CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS multisig_address,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS create_service_multisigs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__logs') }}
WHERE
    contract_address = '0xe3607b00e75f6405248323a9417ff6b39b244b50' --Service Registry (AUTONOLAS-SERVICE-V1)
    AND topic_0 = '0x2d53f895cd5faf3cddba94a25c2ced2105885b5b37450ff430ffa3cbdf332c74' --CreateMultisigWithAgents
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

{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs2') }}
    WHERE
        topics [0] = '0x91ccaa7a278130b65168c3a0c8d3bcae84cf5e43704342bd3ec0b59e59c036db'
        AND contract_address = LOWER('0x411b0fAcC3489691f28ad58c47006AF5E3Ab3A28')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
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
    token0_address,
    token1_address,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation

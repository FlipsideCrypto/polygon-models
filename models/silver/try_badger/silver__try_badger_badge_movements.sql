{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    incremental_strategy = 'delete+insert',
) }}

SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    _log_id,
    _inserted_timestamp,
    contract_address AS network_address,
    event_inputs:_id::string AS badge_id,
    event_inputs:_from::string AS previous_owner,
    event_inputs:_to::string AS latest_owner
FROM {{ ref('silver__logs') }}
WHERE contract_address IN ( SELECT network_address FROM {{ ref('silver__try_badger_networks') }} )
    AND block_timestamp > '2022-10-15'
    AND topics[0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
        FROM
            {{ this }}
    )
    {% endif %}
{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    incremental_strategy = 'delete+insert',
) }}

SELECT 
    _log_id,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address AS network_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as data_split,
    public.udf_hex_to_int(substr(data_split[0], 3, 64)) as badge_id,
    public.udf_hex_to_int(substr(data_split[1], 3, 64)) as badge_amount,
    '0x' || substr(topics[2], 3+24, 40) as previous_owner,
    '0x' || substr(topics[3], 3+24, 40) as latest_owner,
    _inserted_timestamp
FROM {{ ref('silver__logs') }}
WHERE contract_address IN ( SELECT network_address FROM {{ ref('silver__try_badger_networks') }} )
    AND block_timestamp > '2022-10-15'
    AND topics[0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
        MAX(
            _inserted_timestamp
        )
        FROM
            {{ this }}
    )
    {% endif %}

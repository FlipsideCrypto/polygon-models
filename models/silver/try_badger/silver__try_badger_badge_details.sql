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
    contract_address,
    public.udf_hex_to_int(substr(topics[1], 3, 64)) as badge_id,
    try_hex_decode_string(rtrim(substr(data,3+64+64),(substr(data,3+64+64+126,1)))) as badge_ipfs,
    _inserted_timestamp
FROM {{ ref('silver__logs') }}
WHERE contract_address IN ( SELECT network_address FROM {{ ref('silver__try_badger_networks') }} )
    AND block_timestamp > '2022-10-15'
    AND topics[0] = '0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b'
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

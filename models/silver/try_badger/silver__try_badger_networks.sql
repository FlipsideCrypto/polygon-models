{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ['block_timestamp::DATE'],
    incremental_strategy = 'delete+insert',
) }}

WITH network_names as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        _call_id,
        ingested_at,
        _inserted_timestamp,
        to_address as network_address,
        try_hex_decode_string(regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') [14]) as _raw_decoded_name,
        rtrim(_raw_decoded_name, substr(_raw_decoded_name,30,1)) AS network_name
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp::DATE > '2022-10-16'::DATE
        AND type = 'CALL'
        AND from_address = LOWER('0x218b3c623ffb9c5e4dbb9142e6ca6f6559f1c2d6') -- deployer 
        AND substr(input, 0, 10) = '0xb6dbcae5'
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
)

SELECT * from network_names

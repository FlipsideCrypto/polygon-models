{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['non_realtime','reorg'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH matic_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        identifier,
        from_address,
        to_address,
        matic_value,
        _call_id,
        _inserted_timestamp,
        matic_value_precise,
        matic_value_precise_raw,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        matic_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                matic_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.from_address AS matic_from_address,
    A.to_address AS matic_to_address,
    A.matic_value AS amount,
    A.matic_value_precise_raw AS amount_precise_raw,
    A.matic_value_precise AS amount_precise,
    ROUND(
        A.matic_value * price,
        2
    ) AS amount_usd,
    _call_id,
    A._inserted_timestamp,
    tx_position,
    trace_index
FROM
    matic_base A
    LEFT JOIN {{ ref('silver__hourly_prices_priority') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
    JOIN tx_table USING (
        tx_hash,
        block_number
    )

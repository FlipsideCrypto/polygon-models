{{ config(
    materialized = 'view'
) }}

WITH matic_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        matic_value,
        identifier,
        _call_id,
        ingested_at,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        matic_value > 0
        AND tx_status = 'SUCCESS'
        and gas_used is not null
),
matic_price AS (
    SELECT
        HOUR,
        AVG(price) AS matic_price
    FROM
        {{ source(
            'ethereum',
            'fact_hourly_token_prices'
        ) }}
    WHERE
        token_address IS NULL
        AND symbol IS NULL
    GROUP BY
        HOUR
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    tx.origin_function_signature AS origin_function_signature,
    A.from_address AS matic_from_address,
    A.to_address AS matic_to_address,
    A.matic_value AS amount,
    ROUND(
        A.matic_value * matic_price,
        2
    ) AS amount_usd
FROM
    matic_base A
    LEFT JOIN matic_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash
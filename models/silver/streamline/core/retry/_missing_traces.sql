{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT COALESCE(
        tx.block_number,
        tr.block_number
    ) AS block_number
FROM
    {{ ref("silver__transactions") }}
    tx full
    OUTER JOIN {{ ref("silver__traces") }}
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
    AND tr.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
WHERE
    tx.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
    AND (
        tr.tx_hash IS NULL
        OR tx.tx_hash IS NULL
    )
    AND (
        tx.from_address <> '0x0000000000000000000000000000000000000000'
        AND tx.to_address <> '0x0000000000000000000000000000000000000000'
    )

{{ config (
    materialized = "view"
) }}

WITH LAST_DAY AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 2
),
transactions AS (
    SELECT
        block_number,
        tx_hash,
        POSITION,
        LAG(
            POSITION,
            1
        ) over (
            PARTITION BY block_number
            ORDER BY
                POSITION ASC
        ) AS prev_POSITION
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                LAST_DAY
        )
),
missing_txs AS (
    SELECT
        DISTINCT block_number
    FROM
        transactions
    WHERE
        POSITION - prev_POSITION <> 1
),
missing_traces AS (
    SELECT
        DISTINCT tx.block_number AS block_number
    FROM
        transactions tx
        LEFT JOIN {{ ref("silver__traces") }}
        tr
        ON tx.block_number = tr.block_number
        AND tx.tx_hash = tr.tx_hash
    WHERE
        (
            tr.tx_hash IS NULL
            OR tr.block_number IS NULL
        )
        AND tr.block_number >= (
            SELECT
                block_number
            FROM
                LAST_DAY
        )
)
SELECT
    block_number
FROM
    missing_txs
UNION
SELECT
    block_number
FROM
    missing_traces

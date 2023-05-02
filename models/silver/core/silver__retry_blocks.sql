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
)
SELECT
    block_number
FROM
    missing_txs

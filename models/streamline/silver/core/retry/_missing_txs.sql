{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
tx_position AS (
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
        block_timestamp >= DATEADD('hour', -84, SYSDATE())
        AND block_number >= (
            SELECT
                block_number
            FROM
                lookback
        )
),
tx_count AS (
    SELECT
        block_number,
        MAX(POSITION) AS num_tx
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_timestamp >= DATEADD('hour', -84, SYSDATE())
        AND block_number >= (
            SELECT
                block_number
            FROM
                lookback
        )
    GROUP BY
        1
),
receipt_count AS (
    SELECT
        block_number,
        MAX(POSITION) AS num_r
    FROM
        {{ ref("silver__receipts") }}
    WHERE
        _inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        AND block_number >= (
            SELECT
                block_number
            FROM
                lookback
        )
    GROUP BY
        1
),
all_blocks AS (
    SELECT
        DISTINCT block_number AS block_number
    FROM
        tx_position
    WHERE
        POSITION - prev_POSITION <> 1
    UNION
    SELECT
        DISTINCT block_number
    FROM
        tx_count t
        JOIN receipt_count r USING(block_number)
    WHERE
        num_tx <> num_r
)
SELECT
    block_number
FROM
    all_blocks

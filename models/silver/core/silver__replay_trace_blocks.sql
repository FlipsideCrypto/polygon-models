{{ config(
    materialized = 'table'
) }}

WITH txs_base AS (

    SELECT
        block_number AS base_block_number,
        tx_hash AS base_tx_hash
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'
        AND to_address <> '0x0000000000000000000000000000000000000000'
),
model_name AS (
    SELECT
        block_number AS model_block_number,
        tx_hash AS model_tx_hash
    FROM
        {{ ref('silver__traces2') }}
),
FINAL AS (
    SELECT
        base_block_number,
        base_tx_hash,
        model_block_number,
        model_tx_hash
    FROM
        txs_base
        LEFT JOIN model_name
        ON base_block_number = model_block_number
        AND base_tx_hash = model_tx_hash
    WHERE
        (
            model_tx_hash IS NULL
            OR model_block_number IS NULL
        )
        AND base_block_number NOT IN (
            SELECT
                block_number
            FROM
                {{ ref('silver_observability__excluded_trace_blocks') }}
        )
        AND base_block_number <= (
            SELECT
                MAX(model_block_number)
            FROM
                model_name
        )
)
SELECT
    DISTINCT base_block_number AS missing_block
FROM
    FINAL

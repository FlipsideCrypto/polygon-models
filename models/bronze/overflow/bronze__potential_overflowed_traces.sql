{{ config (
    materialized = "view",
    tags = ['overflowed_traces']
) }}

WITH impacted_blocks AS (

    SELECT
        51073769 AS block_number
    UNION
    SELECT
        50888353
    UNION
    SELECT
        51103282
    UNION
    SELECT
        50888044
    UNION
    SELECT
        51105228
    UNION
    SELECT
        51073794
    UNION
    SELECT
        51102645
    UNION
    SELECT
        50020883
    UNION
    SELECT
        51073411
    UNION
    SELECT
        51073884
    UNION
    SELECT
        51104249
    UNION
    SELECT
        51073379
    UNION
    SELECT
        51105014
    UNION
    SELECT
        51073239
    UNION
    SELECT
        51096591
    UNION
    SELECT
        50890486
    UNION
    SELECT
        51055050
    UNION
    SELECT
        50020889
    UNION
    SELECT
        51103946
    UNION
    SELECT
        51068789
    UNION
    SELECT
        50891139
    UNION
    SELECT
        51073292
    UNION
    SELECT
        51073493
    UNION
    SELECT
        50891095 {#  remove after filling
    SELECT
        VALUE :: INT AS block_number
    FROM
        (
            SELECT
                blocks_impacted_array
            FROM
                {{ ref("silver_observability__traces_completeness") }}
            ORDER BY
                test_timestamp DESC
            LIMIT
                1
        ), LATERAL FLATTEN (
            input => blocks_impacted_array
        ) #}
),
all_txs AS (
    SELECT
        t.block_number,
        t.position,
        t.tx_hash
    FROM
        {{ ref("silver__transactions") }}
        t
        JOIN impacted_blocks USING (block_number)
),
missing_txs AS (
    SELECT
        DISTINCT block_number,
        POSITION,
        file_name
    FROM
        all_txs
        LEFT JOIN {{ ref("silver__traces") }}
        tr USING (
            block_number,
            tx_hash
        )
        JOIN {{ ref("streamline__complete_debug_traceBlockByNumber") }} USING (block_number)
    WHERE
        tr.tx_hash IS NULL
)
SELECT
    block_number,
    POSITION,
    file_name,
    build_scoped_file_url(
        @streamline.bronze.external_tables,
        file_name
    ) AS file_url,
    ['block_number', 'array_index'] AS index_cols,
    ROW_NUMBER() over (
        ORDER BY
            block_number ASC,
            POSITION ASC
    ) AS row_no
FROM
    missing_txs
ORDER BY
    block_number ASC,
    POSITION ASC

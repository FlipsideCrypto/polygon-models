{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_traces(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceBlockByNumber', 'sql_limit', {{var('sql_limit','480000')}}, 'producer_batch_size', {{var('producer_batch_size','120000')}}, 'worker_batch_size', {{var('worker_batch_size','300')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
blocks AS (
    SELECT
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number > 30000000
        {# (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        ) #}
    EXCEPT
    SELECT
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_debug_traceBlockByNumber") }}
    WHERE
        block_number > 30000000
        {# (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        ) #}
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "debug_traceBlockByNumber", "params":[',
            block_number :: INTEGER,
            ',{"tracer": "callTracer"}',
            '],"id":"',
            block_number :: STRING,
            '"}'
        )
    ) AS request
FROM
    blocks

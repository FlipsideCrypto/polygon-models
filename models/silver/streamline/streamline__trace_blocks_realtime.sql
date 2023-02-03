{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_polygon_generic(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'trace_blocks', 'sql_limit', '1000', 'producer_batch_size', '1000', 'worker_batch_size', '1000', 'batch_call_limit', '100'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    PARSE_JSON(
        CONCAT(
            '{"method": "trace_block", "params":["',
            block_number :: STRING,
            '"],"id":"',
            block_number :: STRING,
            '"}'
        )
    ) AS request
FROM
    {{ ref("streamline__blocks") }}
WHERE
    block_number > 35000000
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number
FROM
    {{ ref("streamline__complete_trace_blocks") }}
WHERE
    block_number > 35000000

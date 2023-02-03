{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_polygon_generic(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'trace_blocks', 'sql_limit', '30000', 'producer_batch_size', '10000', 'worker_batch_size', '10000', 'batch_call_limit', '30'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(35) %}
    (

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
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number
        FROM
            {{ ref("streamline__complete_trace_blocks") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

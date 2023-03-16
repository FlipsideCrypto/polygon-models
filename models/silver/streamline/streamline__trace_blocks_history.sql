{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'trace_blocks', 'sql_limit', {{var('sql_limit','480000')}}, 'producer_batch_size', {{var('producer_batch_size','60000')}}, 'worker_batch_size', {{var('worker_batch_size','30000')}}, 'batch_call_limit', {{var('batch_call_limit','99')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(35) %}
    (
        WITH blocks AS (

            SELECT
                block_number :: STRING AS block_number
            FROM
                {{ ref("streamline__blocks") }}
            WHERE
                block_number BETWEEN {{ item * 1000000 + 1 }}
                AND {{(
                    item + 1
                ) * 1000000 }}
            EXCEPT
            SELECT
                block_number :: STRING
            FROM
                {{ ref("streamline__complete_trace_blocks") }}
            WHERE
                block_number BETWEEN {{ item * 1000000 + 1 }}
                AND {{(
                    item + 1
                ) * 1000000 }}
        )
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
            blocks
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

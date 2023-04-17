{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_traces(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceBlockByNumber', 'sql_limit', {{var('sql_limit','150000')}}, 'producer_batch_size', {{var('producer_batch_size','150000')}}, 'worker_batch_size', {{var('worker_batch_size','500')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(40) %}
    (
        WITH blocks AS (

            SELECT
                block_number
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
                {{ ref("streamline__complete_debug_traceBlockByNumber") }}
            WHERE
                block_number BETWEEN {{ item * 1000000 + 1 }}
                AND {{(
                    item + 1
                ) * 1000000 }}
        )
        SELECT
            PARSE_JSON(
                CONCAT(
                    '{"jsonrpc": "2.0",',
                    '"method": "debug_traceBlockByNumber", "params":["',
                    REPLACE(
                        concat_ws(
                            '',
                            '0x',
                            to_char(
                                block_number :: INTEGER,
                                'XXXXXXXX'
                            )
                        ),
                        ' ',
                        ''
                    ),
                    '",{"tracer": "callTracer"}',
                    '],"id":"',
                    block_number :: STRING,
                    '"}'
                )
            ) AS request
        FROM
            blocks
        ORDER BY
            block_number ASC
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

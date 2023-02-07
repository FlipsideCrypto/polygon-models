{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'eth_getBlockReceipts', 'sql_limit', {{var('sql_limit','30000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','10000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(35) %}
    (

        SELECT
            PARSE_JSON(
                CONCAT(
                    '{"method": "eth_getBlockReceipts", "params":["',
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
            {{ ref("streamline__complete_eth_getBlockReceipts") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

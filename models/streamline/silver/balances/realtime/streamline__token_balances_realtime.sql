{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'token_balances', 'sql_limit', {{var('sql_limit','40000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_balances_realtime']
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
logs AS (
    SELECT
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 42)) AS address2,
        l.contract_address,
        l.block_number
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        (
            l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                l.topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND l.contract_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            )
            OR (
                l.topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND l.contract_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            )
        )
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_timestamp :: DATE >= DATEADD(
            'day',
            -5,
            CURRENT_TIMESTAMP
        )
),
transfers AS (
    SELECT
        DISTINCT block_number,
        contract_address,
        address1 AS address
    FROM
        logs
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        contract_address,
        address2 AS address
    FROM
        logs
    WHERE
        address2 IS NOT NULL
        AND address2 <> '0x0000000000000000000000000000000000000000'
),
FINAL AS (
    SELECT
        block_number,
        address,
        contract_address
    FROM
        transfers
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        address,
        contract_address
    FROM
        {{ ref("streamline__complete_token_balances") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
        AND _inserted_timestamp :: DATE >= DATEADD(
            'day',
            -7,
            CURRENT_TIMESTAMP
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_call", "params": [{ "to": "',
            contract_address,
            '", "data": "',
            '0x70a08231',
            -- Method ID for balanceOf(address)
            CONCAT(REPEAT('0', 24), RIGHT(address, 40)),
            '"}, "',
            REPLACE(
                CONCAT(
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '"], "id": ',
            block_number :: STRING,
            '}'
        )
    ) AS request
FROM
    FINAL
ORDER BY
    block_number ASC
LIMIT
    1000 -- TODO: remove this limit

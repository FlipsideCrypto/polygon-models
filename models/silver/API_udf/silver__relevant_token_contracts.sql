{{ config(
    materialized = 'table',
    unique_key = "contract_address"
) }}

SELECT
    contract_address,
    'polygon' AS blockchain,
    COUNT(*) AS transfers,
    MIN(block_number) + 1 AS created_block
FROM
    {{ ref('silver__logs') }}
WHERE
    topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
GROUP BY
    1,
    2
HAVING
    COUNT(*) > 25

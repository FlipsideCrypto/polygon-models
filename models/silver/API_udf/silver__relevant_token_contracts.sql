{{ config(
    materialized = 'table',
    unique_key = "contract_address"
) }}

SELECT
    contract_address,
    'polygon' AS blockchain,
    COUNT(*) AS transfers,
    MAX(block_number) AS created_block
FROM
    {{ ref('silver__logs') }}
WHERE
    tx_status = 'SUCCESS'
GROUP BY
    1,
    2
HAVING
    COUNT(*) > 25

{{ config(
    materialized = 'table',
    unique_key = "contract_address",
    tags = ['non_realtime']
) }}

SELECT
    contract_address,
    'polygon' AS blockchain,
    COUNT(*) AS transfers,
    MAX(block_number) AS latest_block
FROM
    {{ ref('silver__logs') }}
WHERE
    tx_status = 'SUCCESS'
GROUP BY
    1,
    2
HAVING
    COUNT(*) > 25

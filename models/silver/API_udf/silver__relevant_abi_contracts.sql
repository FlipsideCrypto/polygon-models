{{ config(
    materialized = 'table',
    unique_key = "contract_address"
) }}

WITH base AS (

    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_token_contracts') }}
),
proxies AS (
    SELECT
        proxy_address
    FROM
        {{ ref('silver__proxies') }}
        JOIN base USING (contract_address)
)
SELECT
    contract_address
FROM
    base
UNION
SELECT
    proxy_address AS contract_address
FROM
    proxies

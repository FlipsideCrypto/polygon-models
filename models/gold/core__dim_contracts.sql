{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address,
    token_symbol AS symbol,
    token_name AS NAME,
    token_decimals AS decimals
FROM
    {{ ref('silver__contracts') }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    decimals,
    symbol,
    price AS token_price,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    CASE
        WHEN decimals IS NOT NULL AND price IS NOT NULL THEN amount * price
        ELSE NULL
    END AS amount_usd,
    CASE
        WHEN decimals IS NULL THEN 'false'
        ELSE 'true'
    END AS has_decimal,
    CASE
        WHEN price IS NULL THEN 'false'
        ELSE 'true'
    END AS has_price,
    _log_id
FROM
    {{ ref('core__fact_token_transfers') }} t
LEFT JOIN {{ ref('core__fact_hourly_token_prices') }} p 
    ON t.contract_address = p.token_address 
        AND DATE_TRUNC('hour', t.block_timestamp) = HOUR
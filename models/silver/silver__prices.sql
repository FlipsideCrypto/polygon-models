{{ config(
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address AS eth_address,
    CASE
        WHEN token_address = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') THEN LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619')
        WHEN token_address = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
        WHEN token_address = LOWER('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0') THEN 'MATIC'
    END AS matic_address,
    price
FROM
    {{ source(
        'ethereum',
        'fact_hourly_token_prices'
    ) }}
WHERE
    token_address IN (
        LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
        --WETH
        LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        --USDC
        LOWER('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0') -- MATIC
    )

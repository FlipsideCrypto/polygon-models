{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    _inserted_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'token_prices_priority_hourly'
    ) }}
WHERE
    blockchain = 'polygon'
    AND token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'

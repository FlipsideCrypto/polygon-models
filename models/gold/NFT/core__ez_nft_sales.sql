{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__seaport_1_1') }}
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__seaport_1_4') }}
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__seaport_1_5') }}
),
all_prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        price AS hourly_price,
        decimals
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base
        )
    UNION ALL
    SELECT
        HOUR,
        'MATIC' AS token_address,
        'MATIC' AS symbol,
        price AS hourly_price,
        decimals
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base
        )
        AND token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
),
matic_price AS (
    SELECT
        HOUR,
        price AS matic_hourly_price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base
        )
        AND token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    cc1.token_name AS project_name,
    erc1155_value,
    tokenId,
    CASE
        WHEN currency_address = 'MATIC' THEN 'MATIC'
        ELSE p.symbol
    END AS currency_symbol,
    currency_address,
    CASE
        WHEN currency_address IN (
            'MATIC',
            '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        ) THEN price_raw / pow(
            10,
            18
        )
        ELSE COALESCE (price_raw / pow(10, decimals), price_raw)
    END AS price,
    IFF(
        decimals IS NULL,
        0,
        price * hourly_price
    ) AS price_usd,
    CASE
        WHEN currency_address IN (
            'MATIC',
            '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        ) THEN total_fees_raw / pow(
            10,
            18
        )
        ELSE COALESCE (total_fees_raw / pow(10, decimals), total_fees_raw)
    END AS total_fees,
    CASE
        WHEN currency_address IN (
            'MATIC',
            '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        ) THEN platform_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (platform_fee_raw / pow(10, decimals), platform_fee_raw)
    END AS platform_fee,
    CASE
        WHEN currency_address IN (
            'MATIC',
            '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        ) THEN creator_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (creator_fee_raw / pow(10, decimals), creator_fee_raw)
    END AS creator_fee,
    IFF(
        decimals IS NULL,
        0,
        total_fees * hourly_price
    ) AS total_fees_usd,
    IFF(
        decimals IS NULL,
        0,
        platform_fee * hourly_price
    ) AS platform_fee_usd,
    IFF(
        decimals IS NULL,
        0,
        creator_fee * hourly_price
    ) AS creator_fee_usd,
    tx_fee,
    tx_fee * matic_hourly_price AS tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature
FROM
    base b
    LEFT JOIN all_prices p
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = p.hour
    AND b.currency_address = p.token_address
    LEFT JOIN matic_price m
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = m.hour
    LEFT JOIN {{ ref('silver__contracts') }}
    cc1
    ON b.nft_address = cc1.contract_address

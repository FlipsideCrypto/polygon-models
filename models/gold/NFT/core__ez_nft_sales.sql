{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

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
        ELSE cc2.token_symbol
    END AS currency_symbol,
    currency_address,
    s1.price / pow(
        10,
        COALESCE(
            CASE
                WHEN currency_address = 'MATIC' THEN 18
                ELSE cc2.token_decimals
            END,
            0
        )
    ) AS price,
    CASE
        WHEN currency_address = 'MATIC'
        OR cc2.token_decimals IS NOT NULL THEN (
            s1.price / pow(
                10,
                COALESCE(
                    CASE
                        WHEN currency_address = 'MATIC' THEN 18
                        ELSE cc2.token_decimals
                    END,
                    0
                )
            )
        ) * p1.price
    END AS price_usd,
    total_fees / pow(
        10,
        COALESCE(
            CASE
                WHEN currency_address = 'MATIC' THEN 18
                ELSE cc2.token_decimals
            END,
            0
        )
    ) AS total_fees,
    platform_fee / pow(
        10,
        COALESCE(
            CASE
                WHEN currency_address = 'MATIC' THEN 18
                ELSE cc2.token_decimals
            END,
            0
        )
    ) AS platform_fee,
    creator_fee / pow(
        10,
        COALESCE(
            CASE
                WHEN currency_address = 'MATIC' THEN 18
                ELSE cc2.token_decimals
            END,
            0
        )
    ) AS creator_fee,
    CASE
        WHEN currency_address = 'MATIC'
        OR cc2.token_decimals IS NOT NULL THEN (
            s1.total_fees / pow(
                10,
                COALESCE(
                    CASE
                        WHEN currency_address = 'MATIC' THEN 18
                        ELSE cc2.token_decimals
                    END,
                    0
                )
            )
        ) * p1.price
    END AS total_fees_usd,
    CASE
        WHEN currency_address = 'MATIC'
        OR cc2.token_decimals IS NOT NULL THEN (
            s1.platform_fee / pow(
                10,
                COALESCE(
                    CASE
                        WHEN currency_address = 'MATIC' THEN 18
                        ELSE cc2.token_decimals
                    END,
                    0
                )
            )
        ) * p1.price
    END AS platform_fee_usd,
    CASE
        WHEN currency_address = 'MATIC'
        OR cc2.token_decimals IS NOT NULL THEN (
            s1.creator_fee / pow(
                10,
                COALESCE(
                    CASE
                        WHEN currency_address = 'MATIC' THEN 18
                        ELSE cc2.token_decimals
                    END,
                    0
                )
            )
        ) * p1.price
    END AS creator_fee_usd,
    tx_fee,
    tx_fee * p2.price AS tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature
FROM
    {{ ref('silver__seaport_1_1') }}
    s1
    LEFT JOIN {{ ref('silver__contracts') }}
    cc1
    ON s1.nft_address = cc1.contract_address
    LEFT JOIN {{ ref('silver__contracts') }}
    cc2
    ON s1.currency_address = cc2.contract_address
    LEFT JOIN {{ ref('silver__prices') }}
    p1
    ON s1.currency_address = p1.matic_address
    AND DATE_TRUNC(
        'hour',
        s1.block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('silver__prices') }}
    p2
    ON p2.matic_address = 'MATIC'
    AND DATE_TRUNC(
        'hour',
        s1.block_timestamp
    ) = p2.hour

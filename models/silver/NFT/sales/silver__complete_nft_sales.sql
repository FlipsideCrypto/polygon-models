{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform_name','platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg', 'heal']
) }}

WITH nft_base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value :: STRING AS erc1155_value,
        tokenId,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__seaport_1_1_sales') }}

{% if is_incremental() and 'seaport_1_1' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_4_sales') }}

{% if is_incremental() and 'seaport_1_4' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_5_sales') }}

{% if is_incremental() and 'seaport_1_5' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__tofunft_sales_test') }}

{% if is_incremental() and 'tofunft' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                nft_base_models
        )
        AND token_address IN (
            SELECT
                DISTINCT currency_address
            FROM
                nft_base_models
        )
),
all_prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        hourly_prices,
        decimals
    FROM
        prices_raw
    UNION ALL
    SELECT
        HOUR,
        'MATIC' AS token_address,
        'MATIC' AS symbol,
        hourly_prices,
        decimals
    FROM
        prices_raw
    WHERE
        token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
),
matic_price AS (
    SELECT
        HOUR,
        hourly_prices AS matic_hourly_price
    FROM
        prices_raw
    WHERE
        token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
),
final_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        CASE
            WHEN origin_to_address IN (
                '0x5e06c349a4a1b8dde8da31e0f167d1cb1d99967c'
            ) THEN 'dew'
            ELSE platform_name
        END AS platform_name,
        platform_exchange_version,
        --credits to hildobby and 0xRob for reservoir calldata logic https://github.com/duneanalytics/spellbook/blob/main/models/nft/ethereum/nft_ethereum_aggregators_markers.sql
        CASE
            WHEN RIGHT(
                input_data,
                2
            ) = '1f'
            AND LEFT(REGEXP_REPLACE(input_data, '^.*00', ''), 2) = '1f'
            AND REGEXP_REPLACE(
                input_data,
                '^.*00',
                ''
            ) != '1f'
            AND LENGTH(REGEXP_REPLACE(input_data, '^.*00', '')) % 2 = 0 THEN REGEXP_REPLACE(
                input_data,
                '^.*00',
                ''
            )
            ELSE NULL
        END AS calldata_hash,
        IFF(
            calldata_hash IS NULL,
            NULL,
            utils.udf_hex_to_string (
                SPLIT(
                    calldata_hash,
                    '1f'
                ) [1] :: STRING
            )
        ) AS marketplace_decoded,
        CASE
            WHEN RIGHT(
                input_data,
                8
            ) = '72db8c0b'
            AND block_timestamp :: DATE <= '2023-11-01' THEN 'Gem'
            WHEN RIGHT(
                input_data,
                8
            ) = '72db8c0b'
            AND block_timestamp :: DATE >= '2023-11-02' THEN 'OpenSea Pro'
            WHEN RIGHT(
                input_data,
                15
            ) = '9616c6c64617461' THEN 'Rarible'
            WHEN marketplace_decoded IS NOT NULL THEN marketplace_decoded
            ELSE NULL
        END AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        CASE
            WHEN currency_address = 'MATIC' THEN 'MATIC'
            ELSE p.symbol
        END AS currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        CASE
            WHEN currency_address IN (
                'MATIC',
                '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ) THEN total_price_raw / pow(
                10,
                18
            )
            ELSE COALESCE (total_price_raw / pow(10, decimals), total_price_raw)
        END AS price,
        IFF(
            decimals IS NULL,
            0,
            price * hourly_prices
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
            total_fees * hourly_prices
        ) AS total_fees_usd,
        IFF(
            decimals IS NULL,
            0,
            platform_fee * hourly_prices
        ) AS platform_fee_usd,
        IFF(
            decimals IS NULL,
            0,
            creator_fee * hourly_prices
        ) AS creator_fee_usd,
        tx_fee,
        tx_fee * matic_hourly_price AS tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        b._inserted_timestamp
    FROM
        nft_base_models b
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
)

{% if is_incremental() and var(
    'HEAL_MODEL'
) %},
heal_model AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        calldata_hash,
        marketplace_decoded,
        aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        C.token_name AS project_name,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        price,
        price_usd,
        total_fees,
        total_fees_usd,
        platform_fee,
        platform_fee_usd,
        creator_fee,
        creator_fee_usd,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        nft_log_id,
        input_data,
        _log_id,
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON t.nft_address = C.contract_address
    WHERE
        t.block_number IN (
            SELECT
                DISTINCT t1.block_number AS block_number
            FROM
                {{ this }}
                t1
            WHERE
                t1.project_name IS NULL
                AND _inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        ) - INTERVAL '36 hours'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('silver__contracts') }} C
                    WHERE
                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.token_name IS NOT NULL
                        AND C.contract_address = t1.nft_address)
                )
        )
    {% endif %}
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        calldata_hash,
        marketplace_decoded,
        aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        C.token_name AS project_name,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        price,
        price_usd,
        total_fees,
        total_fees_usd,
        platform_fee,
        platform_fee_usd,
        creator_fee,
        creator_fee_usd,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        nft_log_id,
        input_data,
        _log_id,
        b._inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
        ) }} AS complete_nft_sales_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        final_base b
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.nft_address = C.contract_address qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
    ORDER BY
        b._inserted_timestamp DESC)) = 1

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    calldata_hash,
    marketplace_decoded,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    currency_symbol,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    price,
    price_usd,
    total_fees,
    total_fees_usd,
    platform_fee,
    platform_fee_usd,
    creator_fee,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS complete_nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    heal_model
{% endif %}

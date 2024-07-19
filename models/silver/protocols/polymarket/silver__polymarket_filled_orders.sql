{{ config(
    materialized = 'table'
) }}

WITH polymarket_orders AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        segmented_data [1] :: STRING AS order_hash,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS marker,
        LOWER(CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))) AS taker,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS maker_asset_id,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS taker_asset_id,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS maker_amount_filled,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS taker_amount_filled,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
),
polymarket_shape AS(
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        order_hash,
        marker,
        taker,
        COALESCE(NULLIF(maker_asset_id, '0'), taker_asset_id) AS asset_id, 
        maker_asset_id,
        taker_asset_id,
        maker_amount_filled / pow(
            10,
            6
        ) AS amount_usd,
        taker_amount_filled / pow(
            10,
            6
        ) AS shares,
        maker_amount_filled/taker_amount_filled AS price_per_share,
        _inserted_timestamp,
        _log_id
    FROM
        polymarket_orders
),
yes_tokens AS(
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        order_hash,
        marker,
        taker,
        condition_id,
        question_id,
        question,
        market_slug,
        end_date_iso,
        token_1_outcome as outcome,
        asset_id, 
        maker_asset_id, 
        taker_asset_id,
        amount_usd,
        shares,
        price_per_share,
        _inserted_timestamp,
        _log_id
    FROM
        polymarket_shape p
        INNER JOIN EXTERNAL.polymarket.dim_markets m
        ON asset_id = token_1_token_id
),
no_tokens AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        order_hash,
        marker,
        taker,
        condition_id,
        question_id,
        question,
        market_slug,
        end_date_iso,
        token_2_outcome as outcome,
        asset_id, 
        maker_asset_id, 
        taker_asset_id,
        amount_usd,
        shares,
        price_per_share,
        _inserted_timestamp,
        _log_id
    FROM
        polymarket_shape p
        INNER JOIN EXTERNAL.polymarket.dim_markets m
        ON asset_id = token_2_token_id
)
SELECT
    *
FROM
    no_tokens
UNION ALL
SELECT
    *
FROM
    yes_tokens
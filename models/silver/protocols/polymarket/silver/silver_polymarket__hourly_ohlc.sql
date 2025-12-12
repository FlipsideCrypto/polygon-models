{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['ez_market_ohlc_id'],
    cluster_by = ['hour', 'question'],
    tags = ['silver','curated','polymarket']
) }}

WITH ordered_prices AS (
    SELECT
        DATEADD(
            hour,
            FLOOR(
                EXTRACT(epoch FROM block_timestamp) / 3600
            ),
            '1970-01-01'::timestamp
        ) AS hour,
        outcome,
        question,
        question_id,
        price_per_share,
        shares,
        amount_usd,
        -- Event metadata from enriched orders table
        event_title,
        market_description,
        dim_condition_id,
        event_id,
        event_slug,
        ROW_NUMBER() OVER (
            PARTITION BY DATEADD(hour, FLOOR(EXTRACT(epoch FROM block_timestamp) / 3600), '1970-01-01'::timestamp), question, outcome, question_id
            ORDER BY block_timestamp ASC
        ) AS row_num_asc,
        ROW_NUMBER() OVER (
            PARTITION BY DATEADD(hour, FLOOR(EXTRACT(epoch FROM block_timestamp) / 3600), '1970-01-01'::timestamp), question, outcome, question_id
            ORDER BY block_timestamp DESC
        ) AS row_num_desc
    FROM
        {{ ref('silver_polymarket__filled_orders') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
ohlc_aggregated AS (
    SELECT
        op.hour,
        op.outcome,
        op.question,
        op.question_id,
        ROUND(MIN(CASE WHEN row_num_asc = 1 THEN price_per_share END), 4) AS open_price,
        ROUND(MAX(price_per_share), 4) AS high_price,
        ROUND(MIN(price_per_share), 4) AS low_price,
        ROUND(MIN(CASE WHEN row_num_desc = 1 THEN price_per_share END), 4) AS close_price,
        ROUND(SUM(shares), 2) AS total_shares,
        COUNT(*) AS total_orders,
        ROUND(SUM(amount_usd), 2) AS total_amount_usd,
        ROUND(SUM(amount_usd) / COUNT(*), 2) AS avg_amount_usd,
        -- Event metadata (using MAX to get one value per group)
        MAX(op.event_title) AS event_title,
        MAX(op.market_description) AS market_description,
        MAX(op.dim_condition_id) AS dim_condition_id,
        MAX(op.event_id) AS event_id,
        MAX(op.event_slug) AS event_slug,
        {{ dbt_utils.generate_surrogate_key(['op.hour', 'op.question', 'op.outcome', 'op.question_id']) }} AS polymarket_ohlc_id
    FROM
        ordered_prices op
    GROUP BY
        op.hour,
        op.outcome,
        op.question,
        op.question_id
)
SELECT
    'polygon' AS blockchain,
    'polymarket-v1' AS platform,
    'polymarket' AS protocol,
    'v1' AS protocol_version,
    oa.hour,
    oa.outcome,
    oa.question,
    oa.question_id,
    oa.event_title,
    oa.event_id,
    oa.open_price,
    oa.high_price,
    oa.low_price,
    oa.close_price,
    oa.total_orders,
    oa.total_amount_usd,
    oa.avg_amount_usd,
    oa.total_shares,
    oa.dim_condition_id,
    oa.polymarket_ohlc_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ohlc_aggregated oa

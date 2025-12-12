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
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
        {% endif %}
),
ohlc_aggregated AS (
    SELECT
        op.hour,
        op.outcome,
        op.question,
        op.question_id,
        MIN(CASE WHEN row_num_asc = 1 THEN price_per_share END) * 100 AS open_price,
        MAX(price_per_share) * 100 AS high_price,
        MIN(price_per_share) * 100 AS low_price,
        MIN(CASE WHEN row_num_desc = 1 THEN price_per_share END) * 100 AS close_price,
        COUNT(*) AS total_orders,
        SUM(amount_usd) AS amount_usd,
        SUM(amount_usd) / COUNT(*) AS avg_order_size,
        -- Event metadata (using MAX to get one value per group)
        MAX(op.event_title) AS event_title,
        MAX(op.market_description) AS market_description,
        MAX(op.dim_condition_id) AS dim_condition_id,
        MAX(op.event_id) AS event_id,
        MAX(op.event_slug) AS event_slug,
        {{ dbt_utils.generate_surrogate_key(['op.hour', 'op.question', 'op.outcome', 'op.question_id']) }} AS ez_market_ohlc_id
    FROM
        ordered_prices op
    GROUP BY
        op.hour,
        op.outcome,
        op.question,
        op.question_id
)
SELECT
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
    oa.amount_usd,
    oa.avg_order_size,
    oa.dim_condition_id,
    oa.ez_market_ohlc_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ohlc_aggregated oa

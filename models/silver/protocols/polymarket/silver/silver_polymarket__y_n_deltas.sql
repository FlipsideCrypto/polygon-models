{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['six_hour_period', 'question'],
    cluster_by = ['six_hour_period', 'question'],
    tags = ['silver','curated','polymarket']
) }}

WITH yes_tokens AS (

    SELECT
        *
    FROM
        {{ ref('silver_polymarket__filled_orders') }}
    WHERE
        outcome = 'Yes'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
    WHERE outcome = 'Yes'
)
{% endif %}
),
no_tokens AS (
    SELECT
        *
    FROM
        {{ ref('silver_polymarket__filled_orders') }}
    WHERE
        outcome = 'No'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
    WHERE outcome = 'No'
)
{% endif %}
),
FINAL AS (
    SELECT
        DATEADD(
            HOUR,
            6 * FLOOR(
                EXTRACT(
                    epoch
                    FROM
                        block_timestamp
                ) / 21600
            ),
            '1970-01-01' :: TIMESTAMP
        ) AS six_hour_period,
        DATEADD(
            HOUR,
            12 * FLOOR(
                EXTRACT(
                    epoch
                    FROM
                        block_timestamp
                ) / 43200
            ),
            '1970-01-01' :: TIMESTAMP
        ) AS half_day,
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DAY,
        DATE_TRUNC(
            'week',
            block_timestamp
        ) AS week,
        DATE_TRUNC(
            'month',
            block_timestamp
        ) AS MONTH,
        question,
        end_date_iso AS end_date,
        outcome,
        SUM(amount_usd) AS total_amount_usd,
        AVG(amount_usd) AS avg_amount_usd,
        SUM(shares) AS total_share_amount,
        AVG(shares) AS avg_share_amount,
        SUM(price_per_share) AS total_price_per_share,
        AVG(price_per_share) AS avg_price_per_share,
        MIN(price_per_share) AS min_price_per_share,
        MAX(price_per_share) AS max_price_per_share,
        -- Event metadata from enriched orders table
        MAX(event_title) AS event_title,
        MAX(market_description) AS market_description,
        MAX(dim_condition_id) AS dim_condition_id,
        MAX(event_id) AS event_id,
        MAX(event_slug) AS event_slug
    FROM
        no_tokens
    GROUP BY
        ALL
    UNION ALL
    SELECT
        DATEADD(
            HOUR,
            6 * FLOOR(
                EXTRACT(
                    epoch
                    FROM
                        block_timestamp
                ) / 21600
            ),
            '1970-01-01' :: TIMESTAMP
        ) AS six_hour_period,
        DATEADD(
            HOUR,
            12 * FLOOR(
                EXTRACT(
                    epoch
                    FROM
                        block_timestamp
                ) / 43200
            ),
            '1970-01-01' :: TIMESTAMP
        ) AS half_day,
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DAY,
        DATE_TRUNC(
            'week',
            block_timestamp
        ) AS week,
        DATE_TRUNC(
            'month',
            block_timestamp
        ) AS MONTH,
        question,
        end_date_iso AS end_date,
        outcome,
        SUM(amount_usd) AS total_amount_usd,
        AVG(amount_usd) AS avg_amount_usd,
        SUM(shares) AS total_share_amount,
        AVG(shares) AS avg_share_amount,
        SUM(price_per_share) AS total_price_per_share,
        AVG(price_per_share) AS avg_price_per_share,
        MIN(price_per_share) AS min_price_per_share,
        MAX(price_per_share) AS max_price_per_share,
        -- Event metadata from enriched orders table
        MAX(event_title) AS event_title,
        MAX(market_description) AS market_description,
        MAX(dim_condition_id) AS dim_condition_id,
        MAX(event_id) AS event_id,
        MAX(event_slug) AS event_slug
    FROM
        yes_tokens
    GROUP BY
        ALL
),
final_2 AS (
    SELECT
        six_hour_period,
        half_day,
        DAY,
        week,
        MONTH,
        question,
        SUM(total_amount_usd) AS total_amount_usd,
        AVG(total_amount_usd) AS avg_amount_usd,
        SUM(total_share_amount) AS total_share_amount,
        AVG(avg_share_amount) AS avg_share_amount,
        SUM(total_price_per_share) AS total_price_per_share,
        AVG(avg_price_per_share) AS avg_price_per_share,
        MIN(min_price_per_share) AS min_price_per_share,
        MAX(max_price_per_share) AS max_price_per_share,
        IFF(MAX(outcome) = 'Yes', AVG(avg_price_per_share), 0) AS yes_avg_price_per_share,
        IFF(MAX(outcome) = 'Yes', AVG(avg_share_amount), 0) AS yes_avg_share_amount,
        IFF(MAX(outcome) = 'Yes', SUM(total_share_amount), 0) AS yes_share_amount,
        IFF(MAX(outcome) = 'Yes', SUM(total_amount_usd), 0) AS yes_amount_usd,
        IFF(MAX(outcome) = 'No', AVG(avg_price_per_share), 0) AS no_avg_price_per_share,
        IFF(MAX(outcome) = 'No', AVG(avg_share_amount), 0) AS no_avg_share_amount,
        IFF(MAX(outcome) = 'No', SUM(total_share_amount), 0) AS no_share_amount,
        IFF(MAX(outcome) = 'No', SUM(total_amount_usd), 0) AS no_amount_usd,
        -- Event metadata (using MAX to get one value per group)
        MAX(event_title) AS event_title,
        MAX(market_description) AS market_description,
        MAX(dim_condition_id) AS dim_condition_id,
        MAX(event_id) AS event_id,
        MAX(event_slug) AS event_slug
    FROM
        FINAL
    GROUP BY
        ALL
)
SELECT
    f.six_hour_period,
    f.half_day,
    f.day,
    f.week,
    f.month,
    f.question,
    f.event_title,
    f.total_amount_usd,
    f.avg_amount_usd,
    f.total_share_amount,
    f.avg_share_amount,
    f.total_price_per_share,
    f.avg_price_per_share,
    f.min_price_per_share,
    f.max_price_per_share,
    f.yes_avg_price_per_share,
    f.yes_avg_share_amount,
    f.yes_share_amount,
    f.yes_amount_usd,
    f.no_avg_price_per_share,
    f.no_avg_share_amount,
    f.no_share_amount,
    f.no_amount_usd,
    f.yes_avg_price_per_share - f.no_avg_price_per_share AS spread,
    f.yes_amount_usd - f.no_amount_usd AS yes_no_usd_delta,
    f.yes_share_amount - f.no_share_amount AS yes_no_share_amount_delta,
    SHA2_HEX(CONCAT(f.six_hour_period, f.question)) AS polymarket_y_n_unique_key,
    -- Event metadata from enriched orders table
    f.dim_condition_id,
    f.event_id,
    f.event_slug,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    final_2 f

{{ config(
    materialized = 'view',
    tags = ['silver','curated','polymarket']
) }}

SELECT
    -- Market base fields
    condition_id,
    question,
    description,
    tokens,
    token_1_token_id,
    token_1_outcome,
    token_1_winner,
    token_2_token_id,
    token_2_outcome,
    token_2_winner,
    enable_order_book,
    active,
    closed,
    archived,
    accepting_orders,
    accepting_order_timestamp,
    minimum_order_size,
    minimum_tick_size,
    question_id,
    market_slug,
    end_date_iso,
    game_start_time,
    seconds_delay,
    fpmm,
    maker_base_fee,
    taker_base_fee,
    notifications_enabled,
    neg_risk,
    neg_risk_market_id,
    neg_risk_request_id,
    rewards,
    tags,
    -- Event-level fields
    event_id,
    event_slug,
    event_title,
    event_subtitle,
    event_description,
    event_category,
    event_subcategory,
    event_active,
    event_closed,
    event_restricted,
    event_archived,
    event_featured,
    event_enable_neg_risk,
    event_start_date,
    event_end_date,
    event_creation_date,
    event_tags,
    -- Market fields from events
    market_id,
    market_slug_from_event,
    market_clob_token_ids,
    market_outcomes,
    market_outcome_prices,
    group_item_title,
    group_item_threshold,
    market_active,
    market_closed,
    market_restricted,
    market_neg_risk_other,
    market_end_date_from_event,
    market_created_at,
    markets_in_event,
    -- Metadata
    dim_markets_id,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp
FROM
    {# {{ source(
        'external_silver',
        'polymarket_markets'
    ) }} #}
external_dev.silver.polymarket_markets
WHERE event_id IS NOT NULL
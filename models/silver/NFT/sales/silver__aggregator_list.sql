{{ config(
    materialized = 'incremental',
    unique_key = 'aggregator_identifier',
    merge_update_columns = ['aggregator_identifier', 'aggregator', 'aggregator_type'],
    full_refresh = false,
    tags = ['silver','nft','curated']
) }}


WITH calldata_aggregators AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('72db8c0b', 'Opensea Pro', 'calldata', '2024-03-07'),
                ('64617461', 'Rarible', 'calldata', '2022-08-23'),
                ('0e1c0c38', 'Magic Eden', 'calldata', '2024-03-07')
        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

platform_routers as (
SELECT
        *
    FROM
        (
            VALUES
                ('0x5e06c349a4a1b8dde8da31e0f167d1cb1d99967c', 'dew', 'router', '2024-03-07'),
                ('0x25956fd0a5fe281d921b1bb3499fc8d5efea6201', 'element', 'router', '2024-03-07')
        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

combined as (
SELECT * 
FROM
    calldata_aggregators

UNION ALL 

SELECT *
FROM
    platform_routers
)

SELECT 
    aggregator_identifier,
    aggregator, 
    aggregator_type,
    _inserted_timestamp
FROM combined

qualify row_number() over (partition by aggregator_identifier order by _inserted_timestamp desc ) = 1 

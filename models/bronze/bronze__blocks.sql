{{ config (
    materialized = 'view'
) }}

SELECT
    record_id,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    header,
    ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        'prod',
        'polygon_blocks'
    ) }}

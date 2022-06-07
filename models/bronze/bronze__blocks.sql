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
    ingested_at
FROM
    {{ source(
        'prod',
        'polygon_blocks'
    ) }}
    qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
    ingested_at DESC)) = 1

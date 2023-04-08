{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BADGER' }} }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    _log_id,
    network_address,
    badge_id,
    previous_owner,
    latest_owner
FROM
    {{ ref('silver__try_badger_badge_movements') }}
silver__try_badger_badge_movements
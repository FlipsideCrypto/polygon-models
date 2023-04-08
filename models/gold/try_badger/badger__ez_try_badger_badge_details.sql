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
    event_index,
    contract_address as network_address,
    badge_id,
    badge_ipfs
FROM
    {{ ref('silver__try_badger_badge_details') }}

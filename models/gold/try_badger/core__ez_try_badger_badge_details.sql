{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BADGER' }} }
) }}

SELECT
    _log_id,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address as network_address,
    badge_id,
    badge_ipfs
FROM
    {{ ref('silver__try_badger_badge_details') }}

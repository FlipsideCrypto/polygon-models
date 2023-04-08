{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BADGER' }} }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    network_address,
    network_name
FROM
    {{ ref('silver__try_badger_networks') }}

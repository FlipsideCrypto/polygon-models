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
    network_address,
    network_name
FROM
    {{ ref('silver__try_badger_networks') }}

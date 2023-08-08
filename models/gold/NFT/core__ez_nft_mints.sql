{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    a.contract_address AS nft_address,
    token_name AS project_name,
    from_address AS nft_from_address,
    to_address AS nft_to_address,
    tokenId,
    erc1155_value,
    event_index
FROM
    {{ ref('silver__nft_transfers') }} A
    LEFT JOIN {{ ref('silver__contracts') }}
    b
    ON A.contract_address = b.contract_address
WHERE
    event_type = 'mint'
    AND from_address = '0x0000000000000000000000000000000000000000'

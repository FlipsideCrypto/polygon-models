{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    event_index,
    intra_event_index,
    tx_hash,
    event_type,
    contract_address,--new column
    project_name AS NAME,--new column
    from_address,--new column
    to_address,--new column
    tokenId AS token_id,--new column
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity,--new column
    CASE
        WHEN erc1155_value IS NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard,--new column    
    COALESCE (
        nft_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }}
    ) AS ez_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    contract_address AS nft_address, -- deprecate   
    project_name, -- deprecate
    from_address AS nft_from_address, -- deprecate
    to_address AS nft_to_address, -- deprecate
    tokenId, -- deprecate
    erc1155_value -- deprecate
FROM
    {{ ref('silver__nft_transfers') }} 
WHERE
    event_type = 'mint'
    AND from_address = '0x0000000000000000000000000000000000000000'



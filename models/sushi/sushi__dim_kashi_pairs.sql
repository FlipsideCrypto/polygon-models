{{ config(
    materialized = 'table',
    enabled = false,
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

    SELECT
        pair_address, 
        pair_symbol as pair_name,
        pair_decimal as pair_decimals,
        asset_symbol,
        asset_address,
        collateral_symbol,
        collateral_address,
        asset_decimal as asset_decimals,
        collateral_decimal as collateral_decimals  
    FROM
         {{ source(
            'polygon_dex_pools',
            'SUSHI_DIM_KASHI_PAIRS'
        ) }} 
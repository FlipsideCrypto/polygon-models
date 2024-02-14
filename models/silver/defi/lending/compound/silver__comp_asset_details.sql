{{ config(
    materialized = 'view'
) }}

SELECT
    LOWER('0xF25212E676D1F7F89Cd72fFEe66158f541246445') AS compound_market_address,
    'Compound USDC' AS compound_market_name,
    'cUSDCv3' AS compound_market_symbol,
    6 AS compound_market_decimals,
    LOWER('0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359') AS underlying_asset_address,
    'USDC' AS underlying_asset_name,
    'USDC' AS underlying_asset_symbol,
    6 AS underlying_asset_decimals

{{ config(
    materialized = 'table'
) }}

    SELECT
        pair_address, 
        kashi_pair as pair_name,
        asset as asset_symbol,
        asset_address,
        collateral as collateral_symbol,
        collateral_address,
        Case when asset in (select distinct b.symbol
                                from {{ source('ethereum','DIM_DEX_LIQUIDITY_POOLS') }}  a
                                left join {{ source('ethereum','DIM_CONTRACTS') }}  b
                                on a.token0 = b.address
                                where platform = 'sushiswap'
                                and b.decimals is not null and b.decimals = 18) then 18
            when asset = 'WMATIC' then 18
            when asset = 'WBTC' then 8
            when asset in ('USDC','USDC','USDR','USDT') then 6
        End as asset_decimals,
        Case when collateral in (select distinct b.symbol
                            from {{ source('ethereum','DIM_DEX_LIQUIDITY_POOLS') }}  a
                            left join {{ source('ethereum','DIM_CONTRACTS') }} b
                            on a.token0 = b.address
                            where platform = 'sushiswap'
                            and b.decimals is not null and b.decimals = 18) then 18
        when collateral = 'WMATIC' then 18
        when collateral = 'WBTC' then 8
        when collateral in ('USDC','USDC','USDR','USDT') then 6
        End as collateral_decimals  
    FROM
         {{ source(
            'polygon_dex_pools',
            'SUSHI_DIM_KASHI_POOLS'
        ) }} 
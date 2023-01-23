{{ config(
    materialized = 'table',
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
        pair_address as pool_address,
        pair_name as pool_name,
        tokens_0_address as token0_address,
        tokens_0_name as token0_symbol,
        tokens_1_address as token1_address,
        tokens_1_name as token1_symbol,
        Case when tokens_0_name in (select distinct b.symbol
                                from ETHEREUM.CORE.DIM_DEX_LIQUIDITY_POOLS a
                                left join ETHEREUM.CORE.DIM_CONTRACTS b
                                on a.token0 = b.address
                                where platform = 'sushiswap'
                                and b.decimals is not null and b.decimals = 18) then 18
            when tokens_0_name = 'WMATIC' then 18
            when tokens_0_name = 'WBTC' then 8
            when tokens_0_name in ('USDC','USDC','USDR','USDT') then 6
        End as token0_decimals,
        Case when tokens_1_name in (select distinct b.symbol
                            from ETHEREUM.CORE.DIM_DEX_LIQUIDITY_POOLS a
                            left join ETHEREUM.CORE.DIM_CONTRACTS b
                            on a.token0 = b.address
                            where platform = 'sushiswap'
                            and b.decimals is not null and b.decimals = 18) then 18
        when tokens_0_name = 'WMATIC' then 18
        when tokens_1_name = 'WBTC' then 8
        when tokens_1_name in ('USDC','USDC','USDR','USDT') then 6
        End as token1_decimals  
    FROM
         {{ source(
            'polygon_dex_pools',
            'SUSHI_DIM_DEX_POOLS'
        ) }} 
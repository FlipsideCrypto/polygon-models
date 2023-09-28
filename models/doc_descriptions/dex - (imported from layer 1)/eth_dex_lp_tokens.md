{% docs eth_dex_lp_tokens %}

The address for the token included in the liquidity pool, as a JSON object. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM polygon.defi.dim_dex_liquidity_pools
WHERE token0 = '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619'
;

{% enddocs %}
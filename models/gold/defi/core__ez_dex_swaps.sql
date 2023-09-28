{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'WOOFI, KYBERSWAP, DODO, QUICKSWAP, FRAX, UNISWAP, BALANCER, HASHFLOW, SUSHI, CURVE',
                'PURPOSE': 'DEX, SWAPS'
            }
        }
    }
) }}

SELECT
  *
FROM {{ ref('defi__ez_dex_swaps') }}
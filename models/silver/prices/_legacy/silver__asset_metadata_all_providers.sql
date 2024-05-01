{{ config(
    materialized = 'incremental',
    unique_key = ['token_address','symbol','id','provider'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
) }}

SELECT
    token_address,
    id,
    COALESCE(
        C.token_symbol,
        p.symbol
    ) AS symbol,
    token_name AS NAME,
    token_decimals AS decimals,
    provider,
    p._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address','symbol','id','provider']
    ) }} AS asset_metadata_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__asset_metadata_all_providers') }}
    p
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON LOWER(
        C.contract_address
    ) = p.token_address
WHERE
    1 = 1

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, id, COALESCE(C.token_symbol, p.symbol), provider
ORDER BY
    p._inserted_timestamp DESC)) = 1

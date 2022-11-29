{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    cluster_by = "_inserted_timestamp::date",
    merge_update_columns = ["contract_address"]
) }}

WITH base AS (

    SELECT
        contract_address,
        abi_data AS full_data,
        abi_data :data :result AS abi,
        _inserted_timestamp
    FROM
        {{ source(
            "bronze_api",
            "contract_abis"
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    contract_address,
    full_data,
    abi,
    _inserted_timestamp
FROM
    base
WHERE
    abi :: STRING <> 'Contract source code not verified' qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _inserted_timestamp DESC)) = 1

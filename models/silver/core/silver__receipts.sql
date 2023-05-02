-- depends_on: {{ ref('bronze__streamline_receipts') }}
{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    full_refresh = False
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze__streamline_FR_receipts') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        DATA :blockHash :: STRING AS block_hash,
        PUBLIC.udf_hex_to_int(
            DATA :blockNumber :: STRING
        ) :: INT AS blockNumber,
        PUBLIC.udf_hex_to_int(
            DATA :cumulativeGasUsed :: STRING
        ) :: INT AS cumulative_gas_used,
        PUBLIC.udf_hex_to_int(
            DATA :effectiveGasPrice :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS effective_gas_price,
        DATA :from :: STRING AS from_address,
        PUBLIC.udf_hex_to_int(
            DATA :gasUsed :: STRING
        ) :: INT AS gas_used,
        DATA :logs AS logs,
        DATA :logsBloom :: STRING AS logs_bloom,
        PUBLIC.udf_hex_to_int(
            DATA :status :: STRING
        ) :: INT AS status,
        CASE
            WHEN status = 1 THEN TRUE
            ELSE FALSE
        END AS tx_success,
        CASE
            WHEN status = 1 THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        DATA :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        DATA :transactionHash :: STRING AS tx_hash,
        PUBLIC.udf_hex_to_int(
            DATA :transactionIndex :: STRING
        ) :: INT AS POSITION,
        PUBLIC.udf_hex_to_int(
            DATA :type :: STRING
        ) :: INT AS TYPE,
        _inserted_timestamp
    FROM
        base
)
SELECT
    *
FROM
    FINAL
WHERE
    tx_hash IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1

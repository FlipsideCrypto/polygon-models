-- depends_on: {{ ref('bronze__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date"
) }}

SELECT
    block_number,
    utils.udf_hex_to_int(
        DATA :baseFeePerGas :: STRING
    ) :: INT AS base_fee_per_gas,
    utils.udf_hex_to_int(
        DATA :difficulty :: STRING
    ) :: INT AS difficulty,
    DATA :extraData :: STRING AS extra_data,
    utils.udf_hex_to_int(
        DATA :gasLimit :: STRING
    ) :: INT AS gas_limit,
    utils.udf_hex_to_int(
        DATA :gasUsed :: STRING
    ) :: INT AS gas_used,
    DATA :hash :: STRING AS HASH,
    DATA :logsBloom :: STRING AS logs_bloom,
    DATA :miner :: STRING AS miner,
    utils.udf_hex_to_int(
        DATA :nonce :: STRING
    ) :: INT AS nonce,
    utils.udf_hex_to_int(
        DATA :number :: STRING
    ) :: INT AS NUMBER,
    DATA :parentHash :: STRING AS parent_hash,
    DATA :receiptsRoot :: STRING AS receipts_root,
    DATA :sha3Uncles :: STRING AS sha3_uncles,
    utils.udf_hex_to_int(
        DATA :size :: STRING
    ) :: INT AS SIZE,
    DATA :stateRoot :: STRING AS state_root,
    utils.udf_hex_to_int(
        DATA :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    utils.udf_hex_to_int(
        DATA :totalDifficulty :: STRING
    ) :: INT AS total_difficulty,
    DATA :transactionsRoot :: STRING AS transactions_root,
    DATA :uncles AS uncles,
    _inserted_timestamp -- need a txs column and block_header_json column(do we though?)
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1

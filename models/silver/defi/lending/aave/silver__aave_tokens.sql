{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH DECODE AS (

    SELECT
        block_number AS atoken_created_block,
        origin_from_address AS token_creator_address,
        contract_address AS a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS aave_version_pool,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [2] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS atoken_decimals,
        utils.udf_hex_to_string (
            segmented_data [7] :: STRING
        ) :: STRING AS atoken_name,
        utils.udf_hex_to_string (
            segmented_data [9] :: STRING
        ) :: STRING AS atoken_symbol,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
    AND
        aave_version_pool IN 
        ('0x794a61358d6845594f94dc1db02a252b5b4814ad',
        '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
a_token_step_1 AS (
    SELECT
        atoken_created_block,
        token_creator_address,
        a_token_address,
        segmented_data,
        underlying_asset,
        aave_version_pool,
        atoken_decimals,
        atoken_name,
        atoken_symbol,
        _inserted_timestamp,
        _log_id
    FROM
        DECODE
    WHERE
        atoken_name LIKE '%Aave%'
),
debt_tokens AS (
    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS atoken_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
            SELECT
                a_token_address
            FROM
                a_token_step_1
        )
),
a_token_step_2 AS (
    SELECT
        atoken_created_block,
        token_creator_address,
        a_token_address,
        segmented_data,
        underlying_asset,
        aave_version_pool,
        atoken_decimals,
        atoken_name,
        atoken_symbol,
        _inserted_timestamp,
        _log_id,
        CASE 
            WHEN aave_version_pool = '0x794a61358d6845594f94dc1db02a252b5b4814ad' THEN 'Aave V3'
            WHEN aave_version_pool = '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf' THEN 'Aave V2'
            ELSE 'Error' 
        END AS protocol
    FROM
        a_token_step_1
)
SELECT
    A.atoken_created_block,
    a.token_creator_address,
    a.aave_version_pool,
    A.atoken_symbol AS atoken_symbol,
    A.a_token_address AS atoken_address,
    b.atoken_stable_debt_address,
    b.atoken_variable_debt_address,
    A.atoken_decimals AS atoken_decimals,
    A.protocol AS atoken_version,
    atoken_name AS atoken_name,
    C.token_symbol AS underlying_symbol,
    A.underlying_asset AS underlying_address,
    C.token_decimals AS underlying_decimals,
    C.token_name AS underlying_name,
    A._inserted_timestamp,
    A._log_id
FROM
    a_token_step_2 A
    INNER JOIN debt_tokens b
    ON A.a_token_address = b.atoken_address
    INNER JOIN {{ ref('silver__contracts') }} C
    ON contract_address = A.underlying_asset

{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__transactions2') }}
WHERE
    _inserted_timestamp :: DATE >= CURRENT_DATE() - 1

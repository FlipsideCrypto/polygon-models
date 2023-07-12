{{ config(
    materialized = 'view'
) }}

SELECT
    column1 AS block_number
FROM
    (
        VALUES
            (31697306),
            (31697810)
    ) AS block_number(column1)

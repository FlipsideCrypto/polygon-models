{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    1 AS temp
{{ config (
    materialized = "view"
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ decode_logs_history(
    start,
    stop
) }}

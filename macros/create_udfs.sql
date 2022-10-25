{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_hex_to_int(
        schema = "public"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
    {% if target.database != "POLYGON_COMMUNITY_DEV" %}
        {% set sql %}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_get_blocks() }}

        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
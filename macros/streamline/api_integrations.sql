{% macro create_aws_polygon_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_polygon_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/polygon-api-prod-rolesnowflakeudfsXXX' api_allowed_prefixes = (
            'https://XXX.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_polygon_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/polygon-api-dev-rolesnowflakeudfsAF733095-10H2D361D3DJD' api_allowed_prefixes = (
            'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}

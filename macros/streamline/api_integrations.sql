{% macro create_aws_polygon_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_polygon_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-polygon' api_allowed_prefixes = (
            'https://avl1rax159.execute-api.us-east-1.amazonaws.com/prod/',
            'https://jml4wcap5f.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}

{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_polygon_api AS {% if target.name == "prod" %}
        'https://avl1rax159.execute-api.us-east-1.amazonaws.com/prod//get_chainhead'
    {% else %}
        'https://jml4wcap5f.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_blocks() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_blocks(
        json variant
    ) returns text api_integration = aws_polygon_api AS {% if target.name == "prod" %}
        'https://avl1rax159.execute-api.us-east-1.amazonaws.com/prod/bulk_get_blocks'
    {% else %}
        'https://jml4wcap5f.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %};
{% endmacro %}
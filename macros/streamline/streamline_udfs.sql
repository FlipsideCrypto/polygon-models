{% macro create_udf_get_chainhead() %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead(
    ) returns variant api_integration = aws_polygon_api AS {% if target.name == "prod" %}
        'https://088pv40k78.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://ug2z7nx4bi.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_blocks() %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_blocks(
        json variant
    ) returns text api_integration = aws_polygon_api AS {% if target.name == "prod" %}
        'https://088pv40k78.execute-api.us-east-1.amazonaws.com/prod/bulk_get_blocks'
    {% else %}
        'https://ug2z7nx4bi.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_transactions() %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_transactions(
        json variant
    ) returns text api_integration = aws_polygon_api AS {% if target.name == "prod" %}
        'https://088pv40k78.execute-api.us-east-1.amazonaws.com/prod/bulk_get_transactions'
    {% else %}
        'https://ug2z7nx4bi.execute-api.us-east-1.amazonaws.com/dev/bulk_get_transactions'
    {%- endif %};
{% endmacro %}
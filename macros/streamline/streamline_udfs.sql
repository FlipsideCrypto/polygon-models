{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration =
    {% if target.name == "prod" %}
        aws_polygon_api AS 'https://p6dhi5vxn4.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        aws_polygon_api_dev AS 'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = {% if target.name == "prod" %}
        aws_polygon_api AS 'https://p6dhi5vxn4.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        aws_polygon_api_dev AS 'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_string() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode(
        abi ARRAY,
        DATA STRING
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_polygon_api AS 'https://p6dhi5vxn4.execute-api.us-east-1.amazonaws.com/prod/decode_function'
    {% else %}
        aws_polygon_api_dev AS 'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/decode_function'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_object() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode(
        abi ARRAY,
        DATA OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_polygon_api AS 'https://p6dhi5vxn4.execute-api.us-east-1.amazonaws.com/prod/decode_log'
    {% else %}
        aws_polygon_api_dev AS 'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/decode_log'
    {%- endif %};
{% endmacro %}


{% macro create_udf_bulk_decode_logs() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_logs(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_polygon_api AS 'https://p6dhi5vxn4.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_logs'
    {% else %}
        aws_polygon_api_dev AS'https://rzyjrd54s6.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_logs'
    {%- endif %};
{% endmacro %}

{% macro create_udf_introspect(
        drop_ = False
    ) %}
    {% set name_ = 'silver.udf_introspect' %}
    {% set signature = [('json', 'variant')] %}
    {% set return_type = 'text' %}
    {% set sql_ = construct_api_route("introspect") %}
    {% if not drop_ %}
        {{ create_sql_function(
            name_ = name_,
            signature = signature,
            return_type = return_type,
            sql_ = sql_,
            api_integration = var("API_INTEGRATION")
        ) }}
    {% else %}
        {{ drop_function(
            name_,
            signature = signature,
        ) }}
    {% endif %}
{% endmacro %}

{% macro gha_task_history() %}
    {% set query %}
SELECT
    DISTINCT task_name
FROM
    {{ ref('silver__gha_workflows') }}

    {% endset %}
    {% set results = run_query(query) %}
    {# execute is a Jinja variable that returns True when dbt is in "execute" mode #}
    {% if execute and results is not none %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    WITH task_history_data AS (
        SELECT
            *
        FROM
            ({% for result in results_list %}
            SELECT
                NAME AS task_name, completed_time, return_value, state, database_name, schema_name, scheduled_time, query_start_time
            FROM
                TABLE(information_schema.task_history(scheduled_time_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()), task_name => '{{ result[0]}}')) {% if not loop.last %}
                UNION ALL
                {% endif %}
            {% endfor %}) AS subquery
        WHERE
            database_name = 'POLYGON' -- '{{ target.database }}' -- replace for prod
            AND schema_name = 'SILVER')
        SELECT
            *
        FROM
            task_history_data
{% endmacro %}

{% macro gha_task_schedule() %}
    WITH base AS (
        SELECT
            w.workflow_name AS workflow_name,
            w.workflow_schedule AS workflow_schedule,
            t.timestamp AS scheduled_time
        FROM
            {{ ref('silver__gha_workflows') }} AS w
            CROSS JOIN TABLE(
                silver.cron_to_timestamps(
                    w.workflow_name,
                    w.workflow_schedule
                )
            ) AS t
    )
SELECT
    concat_ws('_', 'TRIGGER', UPPER(SUBSTR(workflow_name, 9)), 'WORKFLOW') AS task_name,
    workflow_name,
    workflow_schedule,
    scheduled_time
FROM
    base
{% endmacro %}

{% macro gha_task_results() %}
SELECT
    s.task_name,
    s.workflow_name,
    s.scheduled_time,
    h.return_value
FROM
    {{ ref('silver__gha_task_schedule') }}
    s
    LEFT JOIN {{ ref('silver__gha_task_history') }}
    h
    ON s.task_name = h.task_name
    AND TO_TIMESTAMP_NTZ(DATE_TRUNC('second', s.scheduled_time)) = TO_TIMESTAMP_NTZ(DATE_TRUNC('second', h.scheduled_time))
    AND h.return_value = 204
    AND h.state = 'SUCCEEDED'
ORDER BY
    task_name,
    scheduled_time
{% endmacro %}

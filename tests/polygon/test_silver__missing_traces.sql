-- depends_on: {{ ref('test_silver__transactions_full') }}
{{ missing_traces(ref("test_gold__fact_traces_full")) }}

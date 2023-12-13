-- depends_on: {{ ref('test_silver__transactions_recent') }}
{{ recent_missing_traces2(ref("test_silver__traces_recent")) }}

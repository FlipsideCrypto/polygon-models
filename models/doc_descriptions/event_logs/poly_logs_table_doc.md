{% docs poly_logs_table_doc %}

This table contains flattened event logs from transactions on the Polygon Blockchain. Transactions may have multiple events, which are denoted by the event index for a transaction hash. Therefore, this table is unique on the combination of transaction hash and event index. Please see `fact_decoded_event_logs` or `ez_decoded_event_logs` for the decoded event logs.
{% enddocs %}
# Single-job Glue DQ hash-stage reconciliation

This package keeps the final output contract unchanged while using an
internal Parquet hash stage for performance.

Main script:

```text
src/glue/main_dq_hash_orchestrator.py
```

The main script is orchestration only. Heavy logic is split into
service and utility files so the code is easier to review.

Final output contract remains:

```text
run_summary.json
column_results.json
pk_results.json
rowcount_results.json
html_summary.json
LATEST.json
evidence/*
```

Internal hash-stage location is derived from the existing config
hierarchy:

```text
s3://<CONFIG_BUCKET>/<CONFIG_BASE_PREFIX>/<CONFIG_ENV>/hash_stage/
```

Recommended mapping values:

```json
{
  "hash_stage_repartition": 300,
  "join_repartition": 300,
  "column_compare_batch_size": 25
}
```

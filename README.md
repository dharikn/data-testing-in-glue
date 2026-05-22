# Glue DQ Two-Job Hash Stage Design

This package converts the current single Glue DQ job into a two-job design.

## Job 1: `hash_stage_job.py`

Reads:
- Bronze S3 CSV
- Redshift filtered table

Does:
- header canonicalisation
- existing-style normalisation
- PK normalisation
- PII-safe PK mask
- `pk_hash`
- `row_hash`
- per-column hashes `h_<column>`
- writes internal Parquet hash stage

Output location is derived automatically from the existing config/mapping hierarchy:

```text
s3://<CONFIG_BUCKET>/<CONFIG_BASE_PREFIX>/<CONFIG_ENV>/hash_stage/<schema.table>/<file_name>/<run_ts>/
  s3_hash/
  redshift_hash/
  hash_stage_manifest.json
```

No extra `hash_stage_root` config is required.

## Job 2: `hash_reconcile_job.py`

Reads the latest hash stage manifest, then:
- performs PK-only reconciliation
- performs row-hash mismatch detection
- performs per-column hash mismatch counts
- hydrates raw values only for failed-column evidence
- writes the same final output contract as before:
  - `run_summary.json`
  - `column_results.json`
  - `pk_results.json`
  - `rowcount_results.json`
  - `html_summary.json`
  - `LATEST.json`
  - `evidence/*`

## Required Glue args for both jobs

```text
--JOB_NAME
--BOOTSTRAP_CONFIG_BUCKET
--BOOTSTRAP_CONFIG_PREFIX
```

## Optional mapping JSON fields

```json
{
  "hash_stage_enabled": true,
  "column_compare_batch_size": 25,
  "join_repartition": 300,
  "only_sample_limit": 200,
  "hash_mismatch_pk_sample_limit": 200,
  "per_column_mismatch_sample_limit": 100,
  "value_mismatch_global_sample_limit": 100000
}
```

## IAM

No `s3:DeleteObject` required if each run uses a unique timestamp folder.
Needs:
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`

on the config/hash_stage/output prefixes.

# Glue DQ Reconciliation Engine - Scalable Modular Version

Modular AWS Glue 4.0 PySpark redesign for Bronze S3 CSV vs Redshift reconciliation.

Keeps the same output contract: run_summary.json, column_results.json, pk_results.json, rowcount_results.json, html_summary.json, LATEST.json and evidence folders.

Performance design: narrow PK/hash full_outer reconciliation, aggregate-first column counts, failed-column-only raw evidence generation.

Deploy files under glue_dq/ as your Glue Python module zip or with --extra-py-files. Entry script: glue_dq/entry.py.

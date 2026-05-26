# CURRENT STATE — Glue DQ Framework

## Project Overview

High-performance AWS Glue PySpark based Data Quality (DQ)
validation framework for validating large-scale datasets
between:
- S3 source datasets
- Redshift target datasets

The framework validates:
- PK consistency
- row-level consistency
- column-level consistency
- row count consistency

while generating:
- evidence files
- JSON summaries
- HTML-compatible outputs
- PII-safe masked evidence

---

# Current Architecture

## Final Design

Single orchestration Glue job with:
- internal hash-stage persistence layer
- reusable utility/service modules
- parquet-based reconciliation architecture

---

# Core Architecture Principle

Instead of repeatedly processing raw datasets:

- Normalize once
- Hash once
- Persist parquet stage once
- Reconcile lightweight datasets

This significantly reduced runtime.

---

# Current Performance

## Previous Runtime

~1 hour 40 mins

## Current Runtime

### Hash Stage
~14 mins 53 seconds

### Reconciliation
~6 mins

### Overall Runtime
~21 mins

Approximate improvement:
~79% runtime reduction

---

# Current Features

- PK duplicate validation
- S3-only record detection
- Redshift-only record detection
- row_hash comparison
- column-level mismatch detection
- configurable sampling
- configurable batching
- configurable repartitioning

---

# Current Repository Design

## Main Script

main_dq_hash_orchestrator.py

Purpose:
- orchestration only
- lightweight entrypoint
- minimal business logic

---

# Utility Modules

- dq_config_utils.py
- dq_hash_stage_service.py
- dq_hash_stage_utils.py
- dq_mask_utils.py
- dq_normalization_utils.py
- dq_reconcile_service.py
- dq_redshift_utils.py
- dq_s3_utils.py
- dq_spark_utils.py

---

# Configuration Design

Configuration-driven architecture using:
- bootstrap.json
- s3_config.json
- redshift_config.json
- mapping JSON

---

# Current Unit Testing Status

Unit tests added for:
- config utils
- S3 utils
- Spark utils
- normalization utils
- masking utils
- Redshift utils
- hash-stage utils
- hash-stage services
- reconcile services
- orchestration script

Notes:
- Some local Windows Spark tests were skipped due to
  HADOOP_HOME/winutils limitations.
- Local testing performed using Python 3.11.
- PySpark 3.5.1 used for local unit tests.

---

# Important Notes For Future Chat

This repository already contains:
- latest working hash-stage architecture
- optimized reconciliation flow
- modularized utility structure
- PR-ready formatting
- type annotations
- docstrings
- line-length improvements
- unit test framework

Future implementation should continue from this baseline.

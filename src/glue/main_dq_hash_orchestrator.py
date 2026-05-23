"""Glue entrypoint for single-job DQ reconciliation.

The script intentionally contains orchestration only.  The expensive
work is delegated to utility/service modules so the job remains easy
to review, test, and support.
"""

from __future__ import annotations

import sys
from typing import Dict

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from dq_config_utils import load_runtime_config_from_s3
from dq_hash_stage_service import build_hash_stage
from dq_reconcile_service import reconcile_hash_stage
from dq_spark_utils import configure_spark, get_logger

logger = get_logger("main_dq_hash_orchestrator")


def _get_args() -> Dict[str, str]:
    """Return mandatory Glue job arguments."""
    return getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BOOTSTRAP_CONFIG_BUCKET",
            "BOOTSTRAP_CONFIG_PREFIX",
        ],
    )


def main() -> None:
    """Run hash-stage creation and reconciliation in one job."""
    args = _get_args()
    s3_client = boto3.client("s3")

    # GlueContext is created once and passed to service layers.
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], {"JOB_NAME": args["JOB_NAME"]})

    logger.info("STEP_001: loading runtime configuration")
    cfg = load_runtime_config_from_s3(
        s3_client=s3_client,
        job_name=args["JOB_NAME"],
        bootstrap_config_bucket=args["BOOTSTRAP_CONFIG_BUCKET"],
        bootstrap_config_prefix=args["BOOTSTRAP_CONFIG_PREFIX"],
    )

    # Spark tuning is centralised to avoid drift across jobs.
    configure_spark(spark, cfg["JOIN_REPARTITION"])

    logger.info("STEP_002: building internal hash stage")
    manifest = build_hash_stage(
        spark=spark,
        glue_context=glue_context,
        s3_client=s3_client,
        cfg=cfg,
        job_name=args["JOB_NAME"],
    )

    logger.info("STEP_003: reconciling staged hash data")
    dq_status = reconcile_hash_stage(
        spark=spark,
        s3_client=s3_client,
        cfg=cfg,
        manifest=manifest,
        job_name=args["JOB_NAME"],
    )

    logger.info("STEP_004: final DQ status=%s", dq_status)

    # Data-quality failures are normally reported in JSON, not as Glue
    # job failures.  Runtime/system issues still raise exceptions.
    if dq_status == "FAIL" and cfg["FAIL_JOB_ON_DQ"]:
        raise RuntimeError("DQ FAIL configured to fail Glue job.")

    job.commit()


if __name__ == "__main__":
    main()

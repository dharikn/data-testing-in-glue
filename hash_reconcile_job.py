import sys
import uuid

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from config_loader import load_runtime_config_from_s3
from evidence_writer import build_value_samples_for_failed_columns
from normalization_utils import canon_header
from s3_io import s3_get_json, s3_put_json, spark_write_csv
from spark_utils import chunks, configure_spark, get_logger, now_utc_iso, run_ts_folder_utc, safe_unpersist

logger = get_logger("hash_reconcile_job")


def write_empty_csv(spark, path, schema):
    spark_write_csv(spark.createDataFrame([], schema), path, 1)


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BOOTSTRAP_CONFIG_BUCKET", "BOOTSTRAP_CONFIG_PREFIX"])
    s3_client = boto3.client("s3")
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], {"JOB_NAME": args["JOB_NAME"]})

    cfg = load_runtime_config_from_s3(
        s3_client,
        args["JOB_NAME"],
        args["BOOTSTRAP_CONFIG_BUCKET"],
        args["BOOTSTRAP_CONFIG_PREFIX"],
    )
    configure_spark(spark, cfg["JOIN_REPARTITION"])

    # Locate latest hash stage from the same config hierarchy.
    schema_tbl = f"{canon_header(cfg['REDSHIFT_SCHEMA'])}.{canon_header(cfg['REDSHIFT_TABLE'])}"
    # Resolve file name by reading hash_stage latest is difficult without filename; use mapping regex not possible here.
    # Recommended: set HASH_STAGE_MANIFEST_URI as optional Glue arg later if multiple files.
    # Current version expects one latest under hash_stage/* and is normally triggered right after hash_stage_job.
    # For simplicity, derive via latest path from current single file naming in manifest is handled by optional env below.
    raise RuntimeError(
        "Use hash_reconcile_from_manifest.py with --HASH_STAGE_MANIFEST_URI, or trigger this job with the manifest path from Job 1."
    )


if __name__ == "__main__":
    main()

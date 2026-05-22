import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from config_loader import load_runtime_config_from_s3
from hash_stage_utils import build_hash_stage_dataset, build_raw_stage_dataset
from normalization_utils import canon_header, normalize_headers
from redshift_reader import build_redshift_query
from s3_io import ensure_trailing_slash, resolve_single_s3_file, s3_join, s3_put_json
from spark_utils import configure_spark, get_logger, now_utc_iso, run_ts_folder_utc

logger = get_logger("hash_stage_job")


def read_redshift(glue, cfg, query):
    return glue.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "connectionName": cfg["REDSHIFT_GLUE_CONNECTION_NAME"],
            "database": cfg["REDSHIFT_DATABASE"],
            "query": query,
            "redshiftTmpDir": cfg["REDSHIFT_TMP_DIR"],
            "useConnectionProperties": "true",
        },
        transformation_ctx="read_redshift_filtered",
    ).toDF()


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

    run_ts = run_ts_folder_utc()
    key = resolve_single_s3_file(s3_client, cfg["BRONZE_S3_BUCKET"], cfg["BRONZE_S3_PREFIX"], cfg["FILENAME_REGEX"])
    file_name = key.rsplit("/", 1)[-1]
    file_name_no_ext = file_name[:-4] if file_name.lower().endswith(".csv") else file_name
    file_uri = s3_join(cfg["BRONZE_S3_BUCKET"], key)

    schema_tbl = f"{canon_header(cfg['REDSHIFT_SCHEMA'])}.{canon_header(cfg['REDSHIFT_TABLE'])}"
    safe_file = canon_header(file_name)
    hash_root = (
        f"s3://{cfg['CONFIG_BUCKET']}/{cfg['CONFIG_BASE_PREFIX']}/{cfg['CONFIG_ENV']}/"
        f"hash_stage/{schema_tbl}/{safe_file}/{run_ts}/"
    )
    s3_hash_path = hash_root + "s3_hash/"
    rs_hash_path = hash_root + "redshift_hash/"
    s3_raw_path = hash_root + "s3_raw/"
    rs_raw_path = hash_root + "redshift_raw/"
    manifest_uri = hash_root + "hash_stage_manifest.json"
    latest_manifest_uri = (
        f"s3://{cfg['CONFIG_BUCKET']}/{cfg['CONFIG_BASE_PREFIX']}/{cfg['CONFIG_ENV']}/"
        f"hash_stage/{schema_tbl}/{safe_file}/LATEST_HASH_STAGE.json"
    )

    logger.info("READ_S3 started")
    bronze = normalize_headers(
        spark.read.option("header", "true")
        .option("sep", cfg["CSV_DELIMITER"])
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", "false")
        .csv(file_uri)
    )

    logger.info("READ_REDSHIFT started")
    rs_query = build_redshift_query(
        file_name,
        file_name_no_ext,
        cfg["REDSHIFT_SCHEMA"],
        cfg["REDSHIFT_TABLE"],
        cfg["REDSHIFT_FILE_NAME_COL"],
    )
    redshift = normalize_headers(read_redshift(glue, cfg, rs_query))

    pk_cols = [canon_header(c) for c in cfg["PK_COLUMNS"]]
    ignore_set = {canon_header(c) for c in cfg["IGNORE_COLUMNS"]} | {"_source_file"}
    pii_set = {canon_header(c) for c in cfg["PII_COLUMNS"]}
    bronze_cols = {c for c in bronze.columns if c not in ignore_set}
    rs_cols = {c for c in redshift.columns if c not in ignore_set}
    compare_cols = sorted([c for c in bronze_cols if c in rs_cols and c not in set(pk_cols)])

    for c in pk_cols:
        if c not in bronze.columns:
            raise RuntimeError(f"PK column missing in S3: {c}")
        if c not in redshift.columns:
            raise RuntimeError(f"PK column missing in Redshift: {c}")

    scale_by_col = {}  # keep simple; add metadata scale extraction later if required.

    logger.info("BUILD_HASH_STAGE started")
    s3_hash = build_hash_stage_dataset(bronze, pk_cols, compare_cols, scale_by_col, pii_set)
    rs_hash = build_hash_stage_dataset(redshift, pk_cols, compare_cols, scale_by_col, pii_set)
    s3_raw = build_raw_stage_dataset(bronze, pk_cols, compare_cols)
    rs_raw = build_raw_stage_dataset(redshift, pk_cols, compare_cols)

    logger.info("WRITE_HASH_STAGE_PARQUET started")
    s3_hash.write.mode("errorifexists").parquet(s3_hash_path)
    rs_hash.write.mode("errorifexists").parquet(rs_hash_path)
    s3_raw.write.mode("errorifexists").parquet(s3_raw_path)
    rs_raw.write.mode("errorifexists").parquet(rs_raw_path)

    manifest = {
        "run_ts_utc": run_ts,
        "created_at_utc": now_utc_iso(),
        "job_name": args["JOB_NAME"],
        "schema": cfg["REDSHIFT_SCHEMA"],
        "table": cfg["REDSHIFT_TABLE"],
        "schema_table": schema_tbl,
        "file_name": file_name,
        "bronze_s3_key": key,
        "pk_columns": pk_cols,
        "compare_columns": compare_cols,
        "pii_columns": sorted(list(pii_set)),
        "hash_root": hash_root,
        "s3_hash_path": s3_hash_path,
        "redshift_hash_path": rs_hash_path,
        "s3_raw_path": s3_raw_path,
        "redshift_raw_path": rs_raw_path,
        "mapping_uri": cfg["MAPPING_URI"],
        "output_root": cfg["OUTPUT_ROOT"],
    }
    s3_put_json(s3_client, manifest_uri, manifest)
    s3_put_json(s3_client, latest_manifest_uri, manifest)
    logger.info(f"HASH_STAGE_COMPLETE manifest={manifest_uri}")
    job.commit()


if __name__ == "__main__":
    main()

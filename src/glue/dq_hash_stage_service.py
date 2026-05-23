"""Service that builds the internal hash-stage Parquet datasets."""

from __future__ import annotations

from typing import Any, Dict, Set

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from dq_hash_stage_utils import (
    build_hash_stage_dataset,
    build_raw_stage_dataset,
)
from dq_normalization_utils import canon_header, normalize_headers
from dq_redshift_utils import build_redshift_query
from dq_s3_utils import resolve_single_s3_file, s3_join, s3_put_json
from dq_spark_utils import get_logger, now_utc_iso, run_ts_folder_utc

logger = get_logger("dq_hash_stage_service")


def _read_redshift(
    glue_context: Any,
    cfg: Dict[str, Any],
    query: str,
):
    """Read Redshift records through the existing Glue connection."""
    return glue_context.create_dynamic_frame.from_options(
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


def _hash_stage_root(
    cfg: Dict[str, Any],
    schema_table: str,
    safe_file: str,
    run_ts: str,
) -> str:
    """Return unique hash-stage root for the current run."""
    return (
        f"s3://{cfg['CONFIG_BUCKET']}/"
        f"{cfg['CONFIG_BASE_PREFIX']}/{cfg['CONFIG_ENV']}/"
        f"hash_stage/{schema_table}/{safe_file}/{run_ts}/"
    )


def build_hash_stage(
    spark: SparkSession,
    glue_context: Any,
    s3_client: Any,
    cfg: Dict[str, Any],
    job_name: str,
) -> Dict[str, Any]:
    """Build internal hash-stage Parquet and return its manifest."""
    run_ts = run_ts_folder_utc()

    # Resolve one source file using the same mapping regex as before.
    key = resolve_single_s3_file(
        s3_client,
        cfg["BRONZE_S3_BUCKET"],
        cfg["BRONZE_S3_PREFIX"],
        cfg["FILENAME_REGEX"],
    )

    file_name = key.rsplit("/", 1)[-1]
    if file_name.lower().endswith(".csv"):
        file_name_no_ext = file_name[:-4]
    else:
        file_name_no_ext = file_name

    file_uri = s3_join(cfg["BRONZE_S3_BUCKET"], key)
    schema_table = (
        f"{canon_header(cfg['REDSHIFT_SCHEMA'])}."
        f"{canon_header(cfg['REDSHIFT_TABLE'])}"
    )
    safe_file = canon_header(file_name)
    hash_root = _hash_stage_root(cfg, schema_table, safe_file, run_ts)

    s3_hash_path = hash_root + "s3_hash/"
    rs_hash_path = hash_root + "redshift_hash/"
    s3_raw_path = hash_root + "s3_raw/"
    rs_raw_path = hash_root + "redshift_raw/"
    manifest_uri = hash_root + "hash_stage_manifest.json"

    latest_manifest_uri = (
        f"s3://{cfg['CONFIG_BUCKET']}/"
        f"{cfg['CONFIG_BASE_PREFIX']}/{cfg['CONFIG_ENV']}/"
        f"hash_stage/{schema_table}/{safe_file}/"
        "LATEST_HASH_STAGE.json"
    )

    logger.info("HASH_STAGE_READ_S3 started")
    bronze = normalize_headers(
        spark.read.option("header", "true")
        .option("sep", cfg["CSV_DELIMITER"])
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", "false")
        .csv(file_uri)
    )

    logger.info("HASH_STAGE_READ_REDSHIFT started")
    rs_query = build_redshift_query(
        file_name=file_name,
        file_name_no_ext=file_name_no_ext,
        schema=cfg["REDSHIFT_SCHEMA"],
        table=cfg["REDSHIFT_TABLE"],
        file_col=cfg["REDSHIFT_FILE_NAME_COL"],
    )
    redshift_raw = _read_redshift(glue_context, cfg, rs_query)
    redshift = normalize_headers(redshift_raw)

    pk_cols = [canon_header(col) for col in cfg["PK_COLUMNS"]]
    pii_set: Set[str] = {
        canon_header(col) for col in cfg["PII_COLUMNS"]
    }
    ignore_set = {canon_header(col) for col in cfg["IGNORE_COLUMNS"]}
    ignore_set.add("_source_file")

    for col_name in pk_cols:
        if col_name not in bronze.columns:
            raise RuntimeError(f"PK column missing in S3: {col_name}")
        if col_name not in redshift.columns:
            raise RuntimeError(
                f"PK column missing in Redshift: {col_name}"
            )

    bronze_cols = {
        col for col in bronze.columns if col not in ignore_set
    }
    rs_cols = {
        col for col in redshift.columns if col not in ignore_set
    }
    compare_cols = sorted(
        col
        for col in bronze_cols
        if col in rs_cols and col not in pk_cols
    )

    if not compare_cols:
        raise RuntimeError("No comparable columns found.")

    # Scale map is intentionally retained as an extension point.  The
    # current implementation uses automatic decimal canonicalisation.
    scale_by_col: Dict[str, int] = {}

    logger.info(
        "HASH_STAGE_BUILD started: compare_cols=%s",
        len(compare_cols),
    )
    s3_hash = build_hash_stage_dataset(
        bronze,
        pk_cols,
        compare_cols,
        scale_by_col,
        pii_set,
    )
    rs_hash = build_hash_stage_dataset(
        redshift,
        pk_cols,
        compare_cols,
        scale_by_col,
        pii_set,
    )
    s3_raw = build_raw_stage_dataset(bronze, pk_cols, compare_cols)
    rs_raw = build_raw_stage_dataset(redshift, pk_cols, compare_cols)

    repartition_count = int(cfg["HASH_STAGE_REPARTITION"])
    pk_norm_cols = [f"pk_n_{col}" for col in pk_cols]

    # Repartition by normalised PKs to produce join-friendly Parquet.
    s3_hash = s3_hash.repartition(
        repartition_count,
        *[F.col(col) for col in pk_norm_cols],
    )
    rs_hash = rs_hash.repartition(
        repartition_count,
        *[F.col(col) for col in pk_norm_cols],
    )
    s3_raw = s3_raw.repartition(
        repartition_count,
        *[F.col(col) for col in pk_norm_cols],
    )
    rs_raw = rs_raw.repartition(
        repartition_count,
        *[F.col(col) for col in pk_norm_cols],
    )

    logger.info("WRITE_001: S3 hash parquet: %s", s3_hash_path)
    s3_hash.write.mode("errorifexists").parquet(s3_hash_path)
    logger.info("WRITE_001_DONE: S3 hash parquet written")

    logger.info("WRITE_002: Redshift hash parquet: %s", rs_hash_path)
    rs_hash.write.mode("errorifexists").parquet(rs_hash_path)
    logger.info("WRITE_002_DONE: Redshift hash parquet written")

    logger.info("WRITE_003: S3 raw parquet: %s", s3_raw_path)
    s3_raw.write.mode("errorifexists").parquet(s3_raw_path)
    logger.info("WRITE_003_DONE: S3 raw parquet written")

    logger.info("WRITE_004: Redshift raw parquet: %s", rs_raw_path)
    rs_raw.write.mode("errorifexists").parquet(rs_raw_path)
    logger.info("WRITE_004_DONE: Redshift raw parquet written")

    manifest = {
        "run_ts_utc": run_ts,
        "created_at_utc": now_utc_iso(),
        "job_name": job_name,
        "schema": cfg["REDSHIFT_SCHEMA"],
        "table": cfg["REDSHIFT_TABLE"],
        "schema_table": schema_table,
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

    logger.info("WRITE_005: manifest: %s", manifest_uri)
    s3_put_json(s3_client, manifest_uri, manifest)

    logger.info("WRITE_006: latest manifest: %s", latest_manifest_uri)
    s3_put_json(s3_client, latest_manifest_uri, manifest)

    logger.info("HASH_STAGE_COMPLETE manifest=%s", manifest_uri)
    return manifest

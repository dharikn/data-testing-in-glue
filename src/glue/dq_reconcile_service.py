"""Service that reconciles staged hash datasets."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq_s3_utils import s3_put_json, spark_write_csv
from dq_spark_utils import (
    chunks,
    get_logger,
    now_utc_iso,
    run_ts_folder_utc,
    safe_unpersist,
)

logger = get_logger("dq_reconcile_service")


def _write_empty_csv(
    spark: SparkSession,
    path: str,
    schema: str,
) -> None:
    """Write an empty evidence CSV with the required schema."""
    spark_write_csv(spark.createDataFrame([], schema), path, 1)


def _build_output_paths(
    cfg: Dict[str, Any],
    manifest: Dict[str, Any],
    run_ts: str,
) -> Dict[str, Any]:
    """Build final output paths matching the existing contract."""
    schema_table = manifest["schema_table"]
    file_name = (
        manifest["file_name"]
        .lower()
        .replace("/", "_")
        .replace(" ", "_")
    )

    out_root = cfg["OUTPUT_ROOT"]
    if not out_root.endswith("/"):
        out_root += "/"

    base_root = f"{out_root}{schema_table}/{file_name}/"
    run_root = f"{base_root}{run_ts}/"
    evidence_root = run_root + "evidence/"

    evidence = {
        "s3_pk_duplicates": evidence_root + "s3_pk_duplicates",
        "redshift_pk_duplicates": (
            evidence_root + "redshift_pk_duplicates"
        ),
        "s3_only_pk": evidence_root + "s3_only_pk",
        "redshift_only_pk": evidence_root + "redshift_only_pk",
        "hash_mismatch_pk": evidence_root + "hash_mismatch_pk",
        "column_mismatch_counts": evidence_root
        + "column_mismatch_counts",
        "value_mismatch_samples": evidence_root
        + "value_mismatch_samples",
    }

    return {
        "base_root": base_root,
        "run_root": run_root,
        "summary_uri": run_root + "run_summary.json",
        "column_results_uri": run_root + "column_results.json",
        "pk_results_uri": run_root + "pk_results.json",
        "rowcount_results_uri": run_root + "rowcount_results.json",
        "html_summary_uri": run_root + "html_summary.json",
        "latest_uri": base_root + "LATEST.json",
        "evidence_root": evidence_root,
        "evidence": evidence,
    }


def _write_duplicate_evidence(
    spark: SparkSession,
    df: DataFrame,
    pk_norm_cols: List[str],
    path: str,
    limit: int,
) -> None:
    """Write duplicate-PK evidence with masked PK text."""
    if df.count() <= 0:
        _write_empty_csv(spark, path, "pk_mask string, count long")
        return

    output_df = df.select(
        F.concat_ws(
            ",",
            *[F.col(col).cast("string") for col in pk_norm_cols],
        ).alias("pk_mask"),
        F.col("count"),
    )
    spark_write_csv(output_df, path, limit)


def _compare_column_hashes(
    spark: SparkSession,
    cfg: Dict[str, Any],
    manifest: Dict[str, Any],
    hash_mismatch: DataFrame,
    s3_hash: DataFrame,
    rs_hash: DataFrame,
    pk_norm_cols: List[str],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Count column mismatches using precomputed column hashes."""
    compare_cols = manifest["compare_columns"]
    rows: List[Dict[str, Any]] = []
    failed_columns: List[str] = []

    if hash_mismatch.count() <= 0:
        for col_name in compare_cols:
            rows.append(
                {"column_name": col_name, "mismatch_count": 0}
            )
        return rows, failed_columns

    mm_pk = hash_mismatch.select(*pk_norm_cols).distinct()
    mm_pk = mm_pk.persist(StorageLevel.MEMORY_AND_DISK)

    b_hash = (
        s3_hash.join(mm_pk, pk_norm_cols, "inner")
        .select(*pk_norm_cols, *[f"h_{col}" for col in compare_cols])
        .alias("b")
    )
    r_hash = (
        rs_hash.join(mm_pk, pk_norm_cols, "inner")
        .select(*pk_norm_cols, *[f"h_{col}" for col in compare_cols])
        .alias("r")
    )

    joined = b_hash.join(r_hash, pk_norm_cols, "inner")
    joined = joined.persist(StorageLevel.MEMORY_AND_DISK)
    _ = joined.count()

    batch_size = int(cfg["COLUMN_COMPARE_BATCH_SIZE"])
    for batch_no, batch_cols in enumerate(
        chunks(compare_cols, batch_size),
        start=1,
    ):
        logger.info(
            "COLUMN_HASH_COUNTS_BATCH_%s: columns=%s",
            batch_no,
            len(batch_cols),
        )

        aggs = []
        for col_name in batch_cols:
            diff = ~F.col(f"b.h_{col_name}").eqNullSafe(
                F.col(f"r.h_{col_name}")
            )
            aggs.append(
                F.sum(F.when(diff, F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias(col_name)
            )

        result = joined.agg(*aggs).collect()[0].asDict()

        for col_name in batch_cols:
            mismatch_count = int(result.get(col_name) or 0)
            rows.append(
                {
                    "column_name": col_name,
                    "mismatch_count": mismatch_count,
                }
            )
            if mismatch_count > 0:
                failed_columns.append(col_name)

    safe_unpersist(joined, mm_pk)
    return rows, failed_columns


def _write_value_evidence(
    spark: SparkSession,
    cfg: Dict[str, Any],
    paths: Dict[str, Any],
    hash_mismatch: DataFrame,
    s3_hash: DataFrame,
    rs_hash: DataFrame,
    s3_raw: DataFrame,
    rs_raw: DataFrame,
    pk_norm_cols: List[str],
    failed_columns: List[str],
) -> None:
    """Hydrate raw evidence only for failed columns."""
    evidence = paths["evidence"]

    if not failed_columns or hash_mismatch.count() <= 0:
        _write_empty_csv(
            spark,
            evidence["value_mismatch_samples"],
            "pk_mask string, column_name string, "
            "s3_value string, redshift_value string",
        )
        return

    mm_pk = hash_mismatch.select(*pk_norm_cols).distinct()
    mm_pk = mm_pk.persist(StorageLevel.MEMORY_AND_DISK)

    b_raw = s3_raw.join(mm_pk, pk_norm_cols, "inner").alias("br")
    r_raw = rs_raw.join(mm_pk, pk_norm_cols, "inner").alias("rr")

    b_hash = (
        s3_hash.select(
            *pk_norm_cols,
            *[f"h_{c}" for c in failed_columns],
        )
        .join(mm_pk, pk_norm_cols, "inner")
        .alias("b")
    )
    r_hash = (
        rs_hash.select(
            *pk_norm_cols,
            *[f"h_{c}" for c in failed_columns],
        )
        .join(mm_pk, pk_norm_cols, "inner")
        .alias("r")
    )

    evidence_join = (
        b_hash.join(r_hash, pk_norm_cols, "inner")
        .join(b_raw, pk_norm_cols, "inner")
        .join(r_raw, pk_norm_cols, "inner")
    )

    sample_dfs: List[DataFrame] = []
    sample_limit = int(cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"])

    for col_name in failed_columns:
        diff = ~F.col(f"b.h_{col_name}").eqNullSafe(
            F.col(f"r.h_{col_name}")
        )
        sample_df = (
            evidence_join.where(diff)
            .select(
                F.concat_ws(
                    ",",
                    *[F.col(c).cast("string") for c in pk_norm_cols],
                ).alias("pk_mask"),
                F.lit(col_name).alias("column_name"),
                F.col(f"br.raw_{col_name}")
                .cast("string")
                .alias("s3_value"),
                F.col(f"rr.raw_{col_name}")
                .cast("string")
                .alias("redshift_value"),
            )
            .limit(sample_limit)
        )
        sample_dfs.append(sample_df)

    samples = sample_dfs[0]
    for sample_df in sample_dfs[1:]:
        samples = samples.unionByName(sample_df)

    spark_write_csv(
        samples,
        evidence["value_mismatch_samples"],
        cfg["VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT"],
    )
    safe_unpersist(mm_pk)


def _build_column_results(
    rows: List[Dict[str, Any]],
    failed_columns: List[str],
    compare_cols: List[str],
    evidence_path: str,
    sample_limit: int,
) -> Dict[str, Any]:
    """Build column_results.json payload."""
    return {
        "total_compare_columns": int(len(compare_cols)),
        "failed_columns": int(len(failed_columns)),
        "passed_columns": int(
            len(compare_cols) - len(failed_columns)
        ),
        "results": [
            {
                "column_name": row["column_name"],
                "status": (
                    "FAIL"
                    if int(row["mismatch_count"]) > 0
                    else "PASS"
                ),
                "mismatch_count": int(row["mismatch_count"]),
                "samples_csv": evidence_path,
                "samples_limit": int(sample_limit),
            }
            for row in rows
        ],
    }


def _write_result_payloads(
    s3_client: Any,
    paths: Dict[str, Any],
    payload: Dict[str, Any],
    html_summary: Dict[str, Any],
) -> None:
    """Write final JSON outputs expected by the HTML report."""
    s3_put_json(s3_client, paths["summary_uri"], payload)
    s3_put_json(
        s3_client,
        paths["column_results_uri"],
        payload["columns"],
    )
    s3_put_json(s3_client, paths["pk_results_uri"], payload["pk"])
    s3_put_json(
        s3_client,
        paths["rowcount_results_uri"],
        payload["rowcount"],
    )
    s3_put_json(s3_client, paths["html_summary_uri"], html_summary)

    latest = {
        "run_ts_utc": payload["run_ts_utc"],
        "run_root": paths["run_root"],
        "status": payload["status"],
        "summary_uri": paths["summary_uri"],
        "html_summary_uri": paths["html_summary_uri"],
    }
    s3_put_json(s3_client, paths["latest_uri"], latest)


def reconcile_hash_stage(
    spark: SparkSession,
    s3_client: Any,
    cfg: Dict[str, Any],
    manifest: Dict[str, Any],
    job_name: str,
) -> str:
    """Reconcile staged hash parquet and write final outputs."""
    run_ts = run_ts_folder_utc()
    run_id = "single_job_" + run_ts

    pk_cols = manifest["pk_columns"]
    pk_norm_cols = [f"pk_n_{col}" for col in pk_cols]
    compare_cols = manifest["compare_columns"]
    paths = _build_output_paths(cfg, manifest, run_ts)
    evidence = paths["evidence"]

    logger.info("RECON_READ_HASH_STAGE started")
    s3_hash = spark.read.parquet(manifest["s3_hash_path"])
    s3_hash = s3_hash.persist(StorageLevel.MEMORY_AND_DISK)
    rs_hash = spark.read.parquet(manifest["redshift_hash_path"])
    rs_hash = rs_hash.persist(StorageLevel.MEMORY_AND_DISK)
    s3_raw = spark.read.parquet(manifest["s3_raw_path"])
    rs_raw = spark.read.parquet(manifest["redshift_raw_path"])

    bronze_rows = int(s3_hash.count())
    redshift_rows = int(rs_hash.count())

    logger.info("RECON_PK_DUP_CHECK started")
    s3_dups = s3_hash.groupBy(*pk_norm_cols).count()
    s3_dups = s3_dups.where(F.col("count") > 1)
    s3_dups = s3_dups.persist(StorageLevel.MEMORY_AND_DISK)

    rs_dups = rs_hash.groupBy(*pk_norm_cols).count()
    rs_dups = rs_dups.where(F.col("count") > 1)
    rs_dups = rs_dups.persist(StorageLevel.MEMORY_AND_DISK)

    s3_dup_cnt = int(s3_dups.count())
    rs_dup_cnt = int(rs_dups.count())

    _write_duplicate_evidence(
        spark,
        s3_dups,
        pk_norm_cols,
        evidence["s3_pk_duplicates"],
        cfg["PK_DUP_SAMPLE_LIMIT"],
    )
    _write_duplicate_evidence(
        spark,
        rs_dups,
        pk_norm_cols,
        evidence["redshift_pk_duplicates"],
        cfg["PK_DUP_SAMPLE_LIMIT"],
    )

    logger.info("RECON_KEY_RECONCILE started")
    join_rep = int(cfg["JOIN_REPARTITION"])

    s3_n = s3_hash.select(
        *pk_norm_cols,
        "pk_hash",
        "row_hash",
        "pk_mask",
    )
    s3_n = s3_n.repartition(
        join_rep,
        *[F.col(c) for c in pk_norm_cols],
    )
    s3_n = s3_n.persist(StorageLevel.MEMORY_AND_DISK)

    rs_n = rs_hash.select(
        *pk_norm_cols,
        "pk_hash",
        "row_hash",
        "pk_mask",
    )
    rs_n = rs_n.repartition(
        join_rep,
        *[F.col(c) for c in pk_norm_cols],
    )
    rs_n = rs_n.persist(StorageLevel.MEMORY_AND_DISK)

    rs_keys = rs_n.select(*pk_norm_cols).distinct()
    s3_keys = s3_n.select(*pk_norm_cols).distinct()

    s3_only = s3_n.join(rs_keys, pk_norm_cols, "left_anti")
    s3_only = s3_only.persist(StorageLevel.MEMORY_AND_DISK)
    rs_only = rs_n.join(s3_keys, pk_norm_cols, "left_anti")
    rs_only = rs_only.persist(StorageLevel.MEMORY_AND_DISK)

    matched = (
        s3_n.alias("s")
        .join(rs_n.alias("r"), pk_norm_cols, "inner")
        .select(
            *[F.col(col) for col in pk_norm_cols],
            F.col("s.pk_hash").alias("pk_hash"),
            F.col("s.row_hash").alias("s3_row_hash"),
            F.col("r.row_hash").alias("rs_row_hash"),
            F.col("s.pk_mask").alias("pk_mask"),
        )
    )
    matched = matched.persist(StorageLevel.MEMORY_AND_DISK)

    hash_mismatch = matched.where(
        ~F.col("s3_row_hash").eqNullSafe(F.col("rs_row_hash"))
    )
    hash_mismatch = hash_mismatch.select(
        *pk_norm_cols,
        "pk_hash",
        "pk_mask",
    )
    hash_mismatch = hash_mismatch.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    s3_only_cnt = int(s3_only.count())
    rs_only_cnt = int(rs_only.count())
    mismatch_pk_count = int(hash_mismatch.count())

    if s3_only_cnt > 0:
        spark_write_csv(
            s3_only.select("pk_mask"),
            evidence["s3_only_pk"],
            cfg["ONLY_SAMPLE_LIMIT"],
        )
    else:
        _write_empty_csv(
            spark,
            evidence["s3_only_pk"],
            "pk_mask string",
        )

    if rs_only_cnt > 0:
        spark_write_csv(
            rs_only.select("pk_mask"),
            evidence["redshift_only_pk"],
            cfg["ONLY_SAMPLE_LIMIT"],
        )
    else:
        _write_empty_csv(
            spark,
            evidence["redshift_only_pk"],
            "pk_mask string",
        )

    if mismatch_pk_count > 0:
        spark_write_csv(
            hash_mismatch.select("pk_mask"),
            evidence["hash_mismatch_pk"],
            cfg["HASH_MISMATCH_PK_SAMPLE_LIMIT"],
        )
    else:
        _write_empty_csv(
            spark,
            evidence["hash_mismatch_pk"],
            "pk_mask string",
        )

    logger.info("RECON_COLUMN_HASH_COUNTS started")
    per_col_rows, failed_cols = _compare_column_hashes(
        spark,
        cfg,
        manifest,
        hash_mismatch,
        s3_hash,
        rs_hash,
        pk_norm_cols,
    )

    col_counts = spark.createDataFrame(
        per_col_rows,
        "column_name string, mismatch_count long",
    )
    spark_write_csv(
        col_counts,
        evidence["column_mismatch_counts"],
        1000000,
    )

    logger.info("RECON_VALUE_EVIDENCE started")
    _write_value_evidence(
        spark,
        cfg,
        paths,
        hash_mismatch,
        s3_hash,
        rs_hash,
        s3_raw,
        rs_raw,
        pk_norm_cols,
        failed_cols,
    )

    col_results = _build_column_results(
        per_col_rows,
        failed_cols,
        compare_cols,
        evidence["value_mismatch_samples"],
        cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"],
    )

    pk_results = {
        "s3_pk_duplicates": int(s3_dup_cnt),
        "redshift_pk_duplicates": int(rs_dup_cnt),
        "s3_only_pk": int(s3_only_cnt),
        "redshift_only_pk": int(rs_only_cnt),
        "hash_mismatch_pk": int(mismatch_pk_count),
        "sample_limits": {
            "pk_dup": int(cfg["PK_DUP_SAMPLE_LIMIT"]),
            "pk_only": int(cfg["ONLY_SAMPLE_LIMIT"]),
            "hash_mismatch_pk": int(
                cfg["HASH_MISMATCH_PK_SAMPLE_LIMIT"]
            ),
        },
        "evidence": {
            "s3_pk_duplicates": evidence["s3_pk_duplicates"],
            "redshift_pk_duplicates": evidence[
                "redshift_pk_duplicates"
            ],
            "s3_only_pk": evidence["s3_only_pk"],
            "redshift_only_pk": evidence["redshift_only_pk"],
            "hash_mismatch_pk": evidence["hash_mismatch_pk"],
        },
    }

    diff = int(bronze_rows - redshift_rows)
    rowcount = {
        "bronze_rows": int(bronze_rows),
        "redshift_rows": int(redshift_rows),
        "difference": diff,
        "percent_diff": float(
            round(
                (float(diff) / float(redshift_rows or 1)) * 100.0,
                6,
            )
        ),
        "status": "FAIL" if bronze_rows != redshift_rows else "PASS",
    }

    fail = (
        s3_dup_cnt > 0
        or rs_dup_cnt > 0
        or s3_only_cnt > 0
        or rs_only_cnt > 0
        or mismatch_pk_count > 0
        or rowcount["status"] == "FAIL"
    )
    status = "FAIL" if fail else "PASS"

    payload = {
        "run_id": run_id,
        "run_ts_utc": run_ts,
        "timestamp_utc": now_utc_iso(),
        "job_name": job_name,
        "status": status,
        "message": (
            "DQ FAILED. Evidence written. Glue job exits "
            "even on FAIL."
            if fail
            else "DQ PASSED. No mismatches found."
        ),
        "hash_stage_manifest": manifest,
        "input": {
            "redshift_schema": manifest["schema"],
            "redshift_table": manifest["table"],
            "file_name": manifest["file_name"],
            "pk_columns": pk_cols,
            "compare_columns": compare_cols,
        },
        "paths": paths,
        "counts": {
            "bronze_rows": int(bronze_rows),
            "redshift_rows": int(redshift_rows),
            "compare_columns": int(len(compare_cols)),
            "s3_pk_duplicates": int(s3_dup_cnt),
            "redshift_pk_duplicates": int(rs_dup_cnt),
            "s3_only_pk": int(s3_only_cnt),
            "redshift_only_pk": int(rs_only_cnt),
            "hash_mismatch_pk": int(mismatch_pk_count),
        },
        "rowcount": rowcount,
        "pk": pk_results,
        "columns": col_results,
    }

    top_failed = sorted(
        [
            item
            for item in col_results["results"]
            if item["status"] == "FAIL"
        ],
        key=lambda item: int(item.get("mismatch_count", 0)),
        reverse=True,
    )[:20]

    html_summary = {
        "run_ts_utc": run_ts,
        "run_root": paths["run_root"],
        "status": status,
        "job_name": job_name,
        "schema": manifest["schema"],
        "table": manifest["table"],
        "file_name": manifest["file_name"],
        "rowcount": rowcount,
        "pk": pk_results,
        "failed_columns_top": top_failed,
        "paths": paths,
        "config": {
            "join_repartition": int(cfg["JOIN_REPARTITION"]),
            "column_compare_batch_size": int(
                cfg["COLUMN_COMPARE_BATCH_SIZE"]
            ),
        },
    }

    _write_result_payloads(s3_client, paths, payload, html_summary)

    safe_unpersist(
        s3_hash,
        rs_hash,
        s3_dups,
        rs_dups,
        s3_n,
        rs_n,
        s3_only,
        rs_only,
        matched,
        hash_mismatch,
    )

    logger.info("RECON_COMPLETE final_dq_status=%s", status)
    return status

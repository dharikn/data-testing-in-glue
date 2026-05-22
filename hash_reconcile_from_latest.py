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
from normalization_utils import canon_header
from s3_io import resolve_single_s3_file, s3_get_json, s3_put_json, spark_write_csv
from spark_utils import chunks, configure_spark, get_logger, now_utc_iso, run_ts_folder_utc, safe_unpersist

logger = get_logger("hash_reconcile_from_latest")


def write_empty_csv(spark, path, schema):
    spark_write_csv(spark.createDataFrame([], schema), path, 1)


def main():
    # No HASH_STAGE_MANIFEST_URI required.
    # This job automatically finds the latest hash-stage manifest from:
    # s3://<CONFIG_BUCKET>/<CONFIG_BASE_PREFIX>/<CONFIG_ENV>/hash_stage/<schema.table>/<file>/LATEST_HASH_STAGE.json

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BOOTSTRAP_CONFIG_BUCKET", "BOOTSTRAP_CONFIG_PREFIX"],
    )

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

    # Resolve the same source file as Job 1 using the mapping regex.
    source_key = resolve_single_s3_file(
        s3_client,
        cfg["BRONZE_S3_BUCKET"],
        cfg["BRONZE_S3_PREFIX"],
        cfg["FILENAME_REGEX"],
    )
    file_name = source_key.rsplit("/", 1)[-1]

    schema_tbl = f"{canon_header(cfg['REDSHIFT_SCHEMA'])}.{canon_header(cfg['REDSHIFT_TABLE'])}"
    safe_file = canon_header(file_name)

    latest_manifest_uri = (
        f"s3://{cfg['CONFIG_BUCKET']}/"
        f"{cfg['CONFIG_BASE_PREFIX']}/{cfg['CONFIG_ENV']}/"
        f"hash_stage/{schema_tbl}/{safe_file}/LATEST_HASH_STAGE.json"
    )

    logger.info(f"Reading latest hash-stage manifest: {latest_manifest_uri}")
    manifest = s3_get_json(s3_client, latest_manifest_uri)

    run_ts = run_ts_folder_utc()
    run_id = str(uuid.uuid4())

    pk_cols = manifest["pk_columns"]
    compare_cols = manifest["compare_columns"]
    file_name = manifest["file_name"]
    schema = manifest["schema"]
    table = manifest["table"]
    schema_tbl = manifest["schema_table"]

    safe_file = canon_header(file_name)

    out_root = cfg["OUTPUT_ROOT"]
    if not out_root.endswith("/"):
        out_root += "/"

    base_root = f"{out_root}{schema_tbl}/{safe_file}/"
    run_root = f"{base_root}{run_ts}/"
    evidence_root = run_root + "evidence/"

    ev = {
        "s3_pk_duplicates": evidence_root + "s3_pk_duplicates",
        "redshift_pk_duplicates": evidence_root + "redshift_pk_duplicates",
        "s3_only_pk": evidence_root + "s3_only_pk",
        "redshift_only_pk": evidence_root + "redshift_only_pk",
        "hash_mismatch_pk": evidence_root + "hash_mismatch_pk",
        "column_mismatch_counts": evidence_root + "column_mismatch_counts",
        "value_mismatch_samples": evidence_root + "value_mismatch_samples",
    }

    paths = {
        "base_root": base_root,
        "run_root": run_root,
        "summary_uri": run_root + "run_summary.json",
        "column_results_uri": run_root + "column_results.json",
        "pk_results_uri": run_root + "pk_results.json",
        "rowcount_results_uri": run_root + "rowcount_results.json",
        "html_summary_uri": run_root + "html_summary.json",
        "latest_uri": base_root + "LATEST.json",
        "evidence_root": evidence_root,
        "evidence": ev,
    }

    logger.info("DEBUG_001: READ_HASH_STAGE started")
    s3_hash = spark.read.parquet(manifest["s3_hash_path"]).persist(StorageLevel.MEMORY_AND_DISK)
    rs_hash = spark.read.parquet(manifest["redshift_hash_path"]).persist(StorageLevel.MEMORY_AND_DISK)

    s3_raw = spark.read.parquet(manifest["s3_raw_path"])
    rs_raw = spark.read.parquet(manifest["redshift_raw_path"])

    bronze_rows = int(s3_hash.count())
    redshift_rows = int(rs_hash.count())

    logger.info("DEBUG_002: PK_DUP_CHECK started")
    s3_dups = (
        s3_hash.groupBy(*pk_cols)
        .count()
        .where(F.col("count") > 1)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_dups = (
        rs_hash.groupBy(*pk_cols)
        .count()
        .where(F.col("count") > 1)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    s3_dup_cnt = int(s3_dups.count())
    rs_dup_cnt = int(rs_dups.count())

    if s3_dup_cnt > 0:
        spark_write_csv(
            s3_dups.select(
                F.concat_ws(",", *[F.col(c).cast("string") for c in pk_cols]).alias("pk_mask"),
                F.col("count"),
            ),
            ev["s3_pk_duplicates"],
            cfg["PK_DUP_SAMPLE_LIMIT"],
        )
    else:
        write_empty_csv(spark, ev["s3_pk_duplicates"], "pk_mask string, count long")

    if rs_dup_cnt > 0:
        spark_write_csv(
            rs_dups.select(
                F.concat_ws(",", *[F.col(c).cast("string") for c in pk_cols]).alias("pk_mask"),
                F.col("count"),
            ),
            ev["redshift_pk_duplicates"],
            cfg["PK_DUP_SAMPLE_LIMIT"],
        )
    else:
        write_empty_csv(spark, ev["redshift_pk_duplicates"], "pk_mask string, count long")

    logger.info("DEBUG_003: KEY_RECONCILE started")
    join_rep = int(cfg["JOIN_REPARTITION"])

    s3_n = (
        s3_hash.select(*pk_cols, "pk_hash", "row_hash", "pk_mask")
        .repartition(join_rep, *[F.col(c) for c in pk_cols])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_n = (
        rs_hash.select(*pk_cols, "pk_hash", "row_hash", "pk_mask")
        .repartition(join_rep, *[F.col(c) for c in pk_cols])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_keys = rs_n.select(*pk_cols).distinct()
    s3_keys = s3_n.select(*pk_cols).distinct()

    s3_only = s3_n.join(rs_keys, pk_cols, "left_anti").persist(StorageLevel.MEMORY_AND_DISK)
    rs_only = rs_n.join(s3_keys, pk_cols, "left_anti").persist(StorageLevel.MEMORY_AND_DISK)

    matched = (
        s3_n.alias("s")
        .join(rs_n.alias("r"), pk_cols, "inner")
        .select(
            *[F.col(c) for c in pk_cols],
            F.col("s.pk_hash").alias("pk_hash"),
            F.col("s.row_hash").alias("s3_row_hash"),
            F.col("r.row_hash").alias("rs_row_hash"),
            F.col("s.pk_mask").alias("pk_mask"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    hash_mismatch = (
        matched.where(~F.col("s3_row_hash").eqNullSafe(F.col("rs_row_hash")))
        .select(*pk_cols, "pk_hash", "pk_mask")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    s3_only_cnt = int(s3_only.count())
    rs_only_cnt = int(rs_only.count())
    mismatch_pk_count = int(hash_mismatch.count())

    if s3_only_cnt > 0:
        spark_write_csv(s3_only.select("pk_mask"), ev["s3_only_pk"], cfg["ONLY_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["s3_only_pk"], "pk_mask string")

    if rs_only_cnt > 0:
        spark_write_csv(rs_only.select("pk_mask"), ev["redshift_only_pk"], cfg["ONLY_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["redshift_only_pk"], "pk_mask string")

    if mismatch_pk_count > 0:
        spark_write_csv(hash_mismatch.select("pk_mask"), ev["hash_mismatch_pk"], cfg["HASH_MISMATCH_PK_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["hash_mismatch_pk"], "pk_mask string")

    logger.info("DEBUG_004: COLUMN_HASH_COUNTS started")
    per_col_rows = []
    failed_columns = []

    if mismatch_pk_count > 0:
        mm_pk = hash_mismatch.select(*pk_cols).distinct().persist(StorageLevel.MEMORY_AND_DISK)

        b_hash = (
            s3_hash.join(mm_pk, pk_cols, "inner")
            .select(*pk_cols, *[f"h_{c}" for c in compare_cols])
            .alias("b")
        )

        r_hash = (
            rs_hash.join(mm_pk, pk_cols, "inner")
            .select(*pk_cols, *[f"h_{c}" for c in compare_cols])
            .alias("r")
        )

        jr = b_hash.join(r_hash, pk_cols, "inner").persist(StorageLevel.MEMORY_AND_DISK)
        _ = jr.count()

        for batch_no, batch_cols in enumerate(chunks(compare_cols, int(cfg["COLUMN_COMPARE_BATCH_SIZE"])), start=1):
            logger.info(f"DEBUG_004_BATCH_{batch_no}: comparing {len(batch_cols)} columns")

            aggs = []
            for c in batch_cols:
                diff = ~F.col(f"b.h_{c}").eqNullSafe(F.col(f"r.h_{c}"))
                aggs.append(F.sum(F.when(diff, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(c))

            result = jr.agg(*aggs).collect()[0].asDict()

            for c in batch_cols:
                cnt = int(result.get(c) or 0)
                per_col_rows.append({"column_name": c, "mismatch_count": cnt})
                if cnt > 0:
                    failed_columns.append(c)

        safe_unpersist(jr, mm_pk)
    else:
        for c in compare_cols:
            per_col_rows.append({"column_name": c, "mismatch_count": 0})

    col_counts = spark.createDataFrame(per_col_rows, "column_name string, mismatch_count long")
    spark_write_csv(col_counts, ev["column_mismatch_counts"], 1000000)

    logger.info("DEBUG_005: VALUE_EVIDENCE started")
    if failed_columns and mismatch_pk_count > 0:
        mm_pk = hash_mismatch.select(*pk_cols).distinct().persist(StorageLevel.MEMORY_AND_DISK)

        b_raw = s3_raw.join(mm_pk, pk_cols, "inner").alias("br")
        r_raw = rs_raw.join(mm_pk, pk_cols, "inner").alias("rr")

        b_hash_failed = (
            s3_hash.select(*pk_cols, *[f"h_{c}" for c in failed_columns])
            .join(mm_pk, pk_cols, "inner")
            .alias("b")
        )

        r_hash_failed = (
            rs_hash.select(*pk_cols, *[f"h_{c}" for c in failed_columns])
            .join(mm_pk, pk_cols, "inner")
            .alias("r")
        )

        ev_jr = b_hash_failed.join(r_hash_failed, pk_cols, "inner").join(b_raw, pk_cols, "inner").join(r_raw, pk_cols, "inner")

        sample_dfs = []
        for c in failed_columns:
            diff = ~F.col(f"b.h_{c}").eqNullSafe(F.col(f"r.h_{c}"))
            sample_dfs.append(
                ev_jr.where(diff)
                .select(
                    F.concat_ws(",", *[F.col(x).cast("string") for x in pk_cols]).alias("pk_mask"),
                    F.lit(c).alias("column_name"),
                    F.col(f"br.raw_{c}").cast("string").alias("s3_value"),
                    F.col(f"rr.raw_{c}").cast("string").alias("redshift_value"),
                )
                .limit(int(cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"]))
            )

        samples = sample_dfs[0]
        for df in sample_dfs[1:]:
            samples = samples.unionByName(df)

        spark_write_csv(samples, ev["value_mismatch_samples"], cfg["VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT"])
        safe_unpersist(mm_pk)
    else:
        write_empty_csv(spark, ev["value_mismatch_samples"], "pk_mask string, column_name string, s3_value string, redshift_value string")

    col_results = {
        "total_compare_columns": int(len(compare_cols)),
        "failed_columns": int(len(failed_columns)),
        "passed_columns": int(len(compare_cols) - len(failed_columns)),
        "results": [
            {
                "column_name": r["column_name"],
                "status": "FAIL" if int(r["mismatch_count"]) > 0 else "PASS",
                "mismatch_count": int(r["mismatch_count"]),
                "samples_csv": ev["value_mismatch_samples"],
                "samples_limit": int(cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"]),
            }
            for r in per_col_rows
        ],
    }

    pk_results = {
        "s3_pk_duplicates": int(s3_dup_cnt),
        "redshift_pk_duplicates": int(rs_dup_cnt),
        "s3_only_pk": int(s3_only_cnt),
        "redshift_only_pk": int(rs_only_cnt),
        "hash_mismatch_pk": int(mismatch_pk_count),
        "sample_limits": {
            "pk_dup": int(cfg["PK_DUP_SAMPLE_LIMIT"]),
            "pk_only": int(cfg["ONLY_SAMPLE_LIMIT"]),
            "hash_mismatch_pk": int(cfg["HASH_MISMATCH_PK_SAMPLE_LIMIT"]),
        },
        "evidence": {
            "s3_pk_duplicates": ev["s3_pk_duplicates"],
            "redshift_pk_duplicates": ev["redshift_pk_duplicates"],
            "s3_only_pk": ev["s3_only_pk"],
            "redshift_only_pk": ev["redshift_only_pk"],
            "hash_mismatch_pk": ev["hash_mismatch_pk"],
        },
    }

    rowcount = {
        "bronze_rows": int(bronze_rows),
        "redshift_rows": int(redshift_rows),
        "difference": int(bronze_rows - redshift_rows),
        "percent_diff": float(round(((float(bronze_rows - redshift_rows) / float(redshift_rows or 1)) * 100.0), 6)),
        "status": "FAIL" if bronze_rows != redshift_rows else "PASS",
    }

    fail = s3_dup_cnt > 0 or rs_dup_cnt > 0 or s3_only_cnt > 0 or rs_only_cnt > 0 or mismatch_pk_count > 0 or rowcount["status"] == "FAIL"
    status = "FAIL" if fail else "PASS"

    payload = {
        "run_id": run_id,
        "run_ts_utc": run_ts,
        "timestamp_utc": now_utc_iso(),
        "job_name": args["JOB_NAME"],
        "status": status,
        "message": "DQ FAILED. Evidence written. Glue job exits successfully even on FAIL." if fail else "DQ PASSED. No mismatches found.",
        "hash_stage_latest_manifest_uri": latest_manifest_uri,
        "hash_stage_manifest": manifest,
        "input": {
            "redshift_schema": schema,
            "redshift_table": table,
            "file_name": file_name,
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
        [x for x in col_results["results"] if x["status"] == "FAIL"],
        key=lambda x: int(x.get("mismatch_count", 0)),
        reverse=True,
    )[:20]

    html_summary = {
        "run_ts_utc": run_ts,
        "run_root": run_root,
        "status": status,
        "job_name": args["JOB_NAME"],
        "schema": schema,
        "table": table,
        "file_name": file_name,
        "rowcount": rowcount,
        "pk": pk_results,
        "failed_columns_top": top_failed,
        "paths": paths,
        "config": {
            "join_repartition": int(cfg["JOIN_REPARTITION"]),
            "column_compare_batch_size": int(cfg["COLUMN_COMPARE_BATCH_SIZE"]),
        },
    }

    s3_put_json(s3_client, paths["summary_uri"], payload)
    s3_put_json(s3_client, paths["column_results_uri"], col_results)
    s3_put_json(s3_client, paths["pk_results_uri"], pk_results)
    s3_put_json(s3_client, paths["rowcount_results_uri"], rowcount)
    s3_put_json(s3_client, paths["html_summary_uri"], html_summary)

    latest_payload = {
        "run_ts_utc": run_ts,
        "run_root": run_root,
        "status": status,
        "summary_uri": paths["summary_uri"],
        "html_summary_uri": paths["html_summary_uri"],
    }
    s3_put_json(s3_client, paths["latest_uri"], latest_payload)

    safe_unpersist(s3_hash, rs_hash, s3_dups, rs_dups, s3_n, rs_n, s3_only, rs_only, matched, hash_mismatch)

    logger.info(f"FINAL_DQ_STATUS={status}")

    if fail and cfg["FAIL_JOB_ON_DQ"]:
        raise RuntimeError("DQ FAIL configured to fail Glue job.")

    job.commit()


if __name__ == "__main__":
    main()

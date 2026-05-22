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
from s3_io import s3_get_json, s3_put_json, spark_write_csv
from spark_utils import chunks, configure_spark, get_logger, now_utc_iso, run_ts_folder_utc, safe_unpersist

logger = get_logger("hash_reconcile_from_manifest")


def write_empty_csv(spark, path, schema):
    spark_write_csv(spark.createDataFrame([], schema), path, 1)


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BOOTSTRAP_CONFIG_BUCKET", "BOOTSTRAP_CONFIG_PREFIX", "HASH_STAGE_MANIFEST_URI"],
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

    manifest = s3_get_json(s3_client, args["HASH_STAGE_MANIFEST_URI"])
    run_ts = run_ts_folder_utc()
    run_id = str(uuid.uuid4())

    pk_cols = manifest["pk_columns"]
    compare_cols = manifest["compare_columns"]
    pii_cols = set(manifest.get("pii_columns", []))
    file_name = manifest["file_name"]
    schema = manifest["schema"]
    table = manifest["table"]
    schema_tbl = manifest["schema_table"]

    safe_file = file_name.lower().replace("/", "_").replace(" ", "_")
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

    logger.info("READ_HASH_STAGE started")
    s3_hash = spark.read.parquet(manifest["s3_hash_path"]).persist(StorageLevel.MEMORY_AND_DISK)
    rs_hash = spark.read.parquet(manifest["redshift_hash_path"]).persist(StorageLevel.MEMORY_AND_DISK)
    s3_raw = spark.read.parquet(manifest["s3_raw_path"])
    rs_raw = spark.read.parquet(manifest["redshift_raw_path"])

    bronze_rows = s3_hash.count()
    redshift_rows = rs_hash.count()

    logger.info("PK_DUP_CHECK started")
    s3_dups = s3_hash.groupBy(*pk_cols).count().where(F.col("count") > 1).persist(StorageLevel.MEMORY_AND_DISK)
    rs_dups = rs_hash.groupBy(*pk_cols).count().where(F.col("count") > 1).persist(StorageLevel.MEMORY_AND_DISK)
    s3_dup_cnt = s3_dups.count()
    rs_dup_cnt = rs_dups.count()

    if s3_dup_cnt:
        spark_write_csv(s3_dups.select(F.concat_ws(",", *[F.col(c) for c in pk_cols]).alias("pk_mask"), "count"), ev["s3_pk_duplicates"], cfg["PK_DUP_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["s3_pk_duplicates"], "pk_mask string, count long")

    if rs_dup_cnt:
        spark_write_csv(rs_dups.select(F.concat_ws(",", *[F.col(c) for c in pk_cols]).alias("pk_mask"), "count"), ev["redshift_pk_duplicates"], cfg["PK_DUP_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["redshift_pk_duplicates"], "pk_mask string, count long")

    logger.info("KEY_RECONCILE started")
    join_rep = int(cfg["JOIN_REPARTITION"])
    s3_n = s3_hash.select(*pk_cols, "pk_hash", "row_hash", "pk_mask").repartition(join_rep, *[F.col(c) for c in pk_cols])
    rs_n = rs_hash.select(*pk_cols, "pk_hash", "row_hash", "pk_mask").repartition(join_rep, *[F.col(c) for c in pk_cols])

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

    hash_mismatch = matched.where(~F.col("s3_row_hash").eqNullSafe(F.col("rs_row_hash"))).select(*pk_cols, "pk_hash", "pk_mask").persist(StorageLevel.MEMORY_AND_DISK)

    s3_only_cnt = s3_only.count()
    rs_only_cnt = rs_only.count()
    mismatch_pk_count = hash_mismatch.count()

    if s3_only_cnt:
        spark_write_csv(s3_only.select("pk_mask"), ev["s3_only_pk"], cfg["ONLY_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["s3_only_pk"], "pk_mask string")
    if rs_only_cnt:
        spark_write_csv(rs_only.select("pk_mask"), ev["redshift_only_pk"], cfg["ONLY_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["redshift_only_pk"], "pk_mask string")
    if mismatch_pk_count:
        spark_write_csv(hash_mismatch.select("pk_mask"), ev["hash_mismatch_pk"], cfg["HASH_MISMATCH_PK_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["hash_mismatch_pk"], "pk_mask string")

    logger.info("COLUMN_HASH_COUNTS started")
    rows = []
    failed_columns = []
    if mismatch_pk_count:
        mm_pk = hash_mismatch.select(*pk_cols).distinct()
        b = s3_hash.join(mm_pk, pk_cols, "inner")
        r = rs_hash.join(mm_pk, pk_cols, "inner")

        jr = b.alias("b").join(r.alias("r"), pk_cols, "inner").persist(StorageLevel.MEMORY_AND_DISK)
        _ = jr.count()

        for batch_cols in chunks(compare_cols, int(cfg["COLUMN_COMPARE_BATCH_SIZE"])):
            aggs = []
            for c in batch_cols:
                diff = ~F.col(f"b.h_{c}").eqNullSafe(F.col(f"r.h_{c}"))
                aggs.append(F.sum(F.when(diff, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(c))
            got = jr.agg(*aggs).collect()[0].asDict()
            for c in batch_cols:
                cnt = int(got.get(c) or 0)
                rows.append({"column_name": c, "mismatch_count": cnt})
                if cnt:
                    failed_columns.append(c)
        safe_unpersist(jr)
    else:
        rows = [{"column_name": c, "mismatch_count": 0} for c in compare_cols]

    col_counts = spark.createDataFrame(rows, "column_name string, mismatch_count long")
    spark_write_csv(col_counts, ev["column_mismatch_counts"], 1000000)

    logger.info("VALUE_EVIDENCE started")
    if failed_columns and mismatch_pk_count:
        mm_pk = hash_mismatch.select(*pk_cols).distinct()
        b_raw = s3_raw.join(mm_pk, pk_cols, "inner")
        r_raw = rs_raw.join(mm_pk, pk_cols, "inner")
        b_hash = s3_hash.select(*pk_cols, *[f"h_{c}" for c in failed_columns]).join(mm_pk, pk_cols, "inner")
        r_hash = rs_hash.select(*pk_cols, *[f"h_{c}" for c in failed_columns]).join(mm_pk, pk_cols, "inner")

        ev_jr = (
            b_hash.alias("b")
            .join(r_hash.alias("r"), pk_cols, "inner")
            .join(b_raw.alias("br"), pk_cols, "inner")
            .join(r_raw.alias("rr"), pk_cols, "inner")
        )

        sample_dfs = []
        for c in failed_columns:
            diff = ~F.col(f"b.h_{c}").eqNullSafe(F.col(f"r.h_{c}"))
            sample_dfs.append(
                ev_jr.where(diff).select(
                    F.concat_ws(",", *[F.col(x) for x in pk_cols]).alias("pk_mask"),
                    F.lit(c).alias("column_name"),
                    F.col(f"br.raw_{c}").alias("s3_value"),
                    F.col(f"rr.raw_{c}").alias("redshift_value"),
                ).limit(int(cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"]))
            )
        samples = sample_dfs[0]
        for df in sample_dfs[1:]:
            samples = samples.unionByName(df)
        spark_write_csv(samples, ev["value_mismatch_samples"], cfg["VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT"])
    else:
        write_empty_csv(spark, ev["value_mismatch_samples"], "pk_mask string, column_name string, s3_value string, redshift_value string")

    col_results = {
        "total_compare_columns": len(compare_cols),
        "failed_columns": len(failed_columns),
        "passed_columns": len(compare_cols) - len(failed_columns),
        "results": [
            {
                "column_name": r["column_name"],
                "status": "FAIL" if int(r["mismatch_count"]) else "PASS",
                "mismatch_count": int(r["mismatch_count"]),
                "samples_csv": ev["value_mismatch_samples"],
                "samples_limit": int(cfg["PER_COLUMN_MISMATCH_SAMPLE_LIMIT"]),
            }
            for r in rows
        ],
    }

    pk_results = {
        "s3_pk_duplicates": int(s3_dup_cnt),
        "redshift_pk_duplicates": int(rs_dup_cnt),
        "s3_only_pk": int(s3_only_cnt),
        "redshift_only_pk": int(rs_only_cnt),
        "hash_mismatch_pk": int(mismatch_pk_count),
        "evidence": ev,
    }

    rowcount = {
        "bronze_rows": int(bronze_rows),
        "redshift_rows": int(redshift_rows),
        "difference": int(bronze_rows - redshift_rows),
        "status": "FAIL" if bronze_rows != redshift_rows else "PASS",
    }

    fail = any(
        [
            s3_dup_cnt,
            rs_dup_cnt,
            s3_only_cnt,
            rs_only_cnt,
            mismatch_pk_count,
            rowcount["status"] == "FAIL",
        ]
    )

    payload = {
        "run_id": run_id,
        "run_ts_utc": run_ts,
        "timestamp_utc": now_utc_iso(),
        "job_name": args["JOB_NAME"],
        "status": "FAIL" if fail else "PASS",
        "message": "DQ FAILED. Glue job completed successfully." if fail else "DQ PASSED.",
        "hash_stage_manifest_uri": args["HASH_STAGE_MANIFEST_URI"],
        "input": {
            "redshift_schema": schema,
            "redshift_table": table,
            "bronze_file_name": file_name,
            "pk_columns": pk_cols,
            "compare_columns": compare_cols,
        },
        "paths": paths,
        "counts": {
            "bronze_rows": int(bronze_rows),
            "redshift_rows": int(redshift_rows),
            "compare_columns": len(compare_cols),
            "s3_only_pk": int(s3_only_cnt),
            "redshift_only_pk": int(rs_only_cnt),
            "hash_mismatch_pk": int(mismatch_pk_count),
        },
        "rowcount": rowcount,
        "pk": pk_results,
        "columns": col_results,
    }

    html_summary = {
        "run_ts_utc": run_ts,
        "run_root": run_root,
        "status": payload["status"],
        "job_name": args["JOB_NAME"],
        "schema": schema,
        "table": table,
        "file_name": file_name,
        "rowcount": rowcount,
        "pk": pk_results,
        "failed_columns_top": sorted(
            [x for x in col_results["results"] if x["status"] == "FAIL"],
            key=lambda x: x["mismatch_count"],
            reverse=True,
        )[:20],
        "paths": paths,
    }

    s3_put_json(s3_client, paths["summary_uri"], payload)
    s3_put_json(s3_client, paths["column_results_uri"], col_results)
    s3_put_json(s3_client, paths["pk_results_uri"], pk_results)
    s3_put_json(s3_client, paths["rowcount_results_uri"], rowcount)
    s3_put_json(s3_client, paths["html_summary_uri"], html_summary)
    s3_put_json(
        s3_client,
        paths["latest_uri"],
        {
            "run_ts_utc": run_ts,
            "run_root": run_root,
            "status": payload["status"],
            "summary_uri": paths["summary_uri"],
            "html_summary_uri": paths["html_summary_uri"],
        },
    )

    safe_unpersist(s3_hash, rs_hash, s3_dups, rs_dups, s3_only, rs_only, matched, hash_mismatch)
    if fail and cfg["FAIL_JOB_ON_DQ"]:
        raise RuntimeError("DQ FAIL configured to fail Glue job.")
    logger.info(f"FINAL_DQ_STATUS={payload['status']}")
    job.commit()


if __name__ == "__main__":
    main()

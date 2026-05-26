import dq_reconcile_service as svc
import pytest

def _cfg():
    return {
        "OUTPUT_ROOT": "s3://out/root/",
        "PK_DUP_SAMPLE_LIMIT": 10,
        "ONLY_SAMPLE_LIMIT": 10,
        "HASH_MISMATCH_PK_SAMPLE_LIMIT": 10,
        "PER_COLUMN_MISMATCH_SAMPLE_LIMIT": 10,
        "VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT": 1000,
        "COLUMN_COMPARE_BATCH_SIZE": 2,
        "JOIN_REPARTITION": 2,
    }


def _manifest(tmp_path):
    root = f"file://{tmp_path}/stage/"
    return {
        "schema": "bank",
        "table": "deal",
        "schema_table": "bank.deal",
        "file_name": "file.csv",
        "pk_columns": ["id"],
        "compare_columns": ["amount", "status"],
        "s3_hash_path": root + "s3_hash",
        "redshift_hash_path": root + "redshift_hash",
        "s3_raw_path": root + "s3_raw",
        "redshift_raw_path": root + "redshift_raw",
    }


def test_build_output_paths():
    paths = svc._build_output_paths(
        {"OUTPUT_ROOT": "s3://out/root"},
        {"schema_table": "s.t", "file_name": "My File.csv"},
        "01012026_010203",
    )
    assert paths["run_root"] == "s3://out/root/s.t/my_file.csv/01012026_010203/"
    assert paths["latest_uri"].endswith("LATEST.json")


def test_build_column_results():
    result = svc._build_column_results(
        rows=[
            {"column_name": "a", "mismatch_count": 0},
            {"column_name": "b", "mismatch_count": 3},
        ],
        failed_columns=["b"],
        compare_cols=["a", "b"],
        evidence_path="s3://ev",
        sample_limit=20,
    )
    assert result["failed_columns"] == 1
    assert result["passed_columns"] == 1
    assert result["results"][0]["status"] == "PASS"
    assert result["results"][1]["status"] == "FAIL"


def test_write_result_payloads(monkeypatch):
    calls = []
    monkeypatch.setattr(
        svc,
        "s3_put_json",
        lambda client, uri, payload: calls.append((uri, payload)),
    )
    paths = {
        "summary_uri": "s3://out/run_summary.json",
        "column_results_uri": "s3://out/column_results.json",
        "pk_results_uri": "s3://out/pk_results.json",
        "rowcount_results_uri": "s3://out/rowcount_results.json",
        "html_summary_uri": "s3://out/html_summary.json",
        "latest_uri": "s3://out/LATEST.json",
        "run_root": "s3://out/run/",
    }
    payload = {
        "run_ts_utc": "01012026_010203",
        "status": "PASS",
        "columns": {},
        "pk": {},
        "rowcount": {},
    }
    svc._write_result_payloads(object(), paths, payload, {})
    assert len(calls) == 6
    assert calls[-1][0].endswith("LATEST.json")


def test_compare_column_hashes_no_mismatches(spark):
    hm = spark.createDataFrame([], "pk_n_id string")
    s3 = spark.createDataFrame([], "pk_n_id string, h_a string")
    rs = spark.createDataFrame([], "pk_n_id string, h_a string")
    rows, failed = svc._compare_column_hashes(
        spark=spark,
        cfg={"COLUMN_COMPARE_BATCH_SIZE": 2},
        manifest={"compare_columns": ["a"]},
        hash_mismatch=hm,
        s3_hash=s3,
        rs_hash=rs,
        pk_norm_cols=["pk_n_id"],
    )
    assert rows == [{"column_name": "a", "mismatch_count": 0}]
    assert failed == []


def test_compare_column_hashes_counts_mismatches(spark):
    hm = spark.createDataFrame([("1",)], ["pk_n_id"])
    s3 = spark.createDataFrame(
        [("1", "A", "SAME")],
        ["pk_n_id", "h_amount", "h_status"],
    )
    rs = spark.createDataFrame(
        [("1", "B", "SAME")],
        ["pk_n_id", "h_amount", "h_status"],
    )
    rows, failed = svc._compare_column_hashes(
        spark=spark,
        cfg={"COLUMN_COMPARE_BATCH_SIZE": 2},
        manifest={"compare_columns": ["amount", "status"]},
        hash_mismatch=hm,
        s3_hash=s3,
        rs_hash=rs,
        pk_norm_cols=["pk_n_id"],
    )
    assert rows == [
        {"column_name": "amount", "mismatch_count": 1},
        {"column_name": "status", "mismatch_count": 0},
    ]
    assert failed == ["amount"]


def test_write_empty_csv_and_duplicate_evidence(monkeypatch, spark):
    calls = []
    monkeypatch.setattr(
        svc,
        "spark_write_csv",
        lambda df, path, limit: calls.append((df.count(), path, limit)),
    )
    empty = spark.createDataFrame([], "pk_n_id string, count long")
    svc._write_duplicate_evidence(spark, empty, ["pk_n_id"], "s3://dup", 10)
    dup = spark.createDataFrame([("1", 2)], ["pk_n_id", "count"])
    svc._write_duplicate_evidence(spark, dup, ["pk_n_id"], "s3://dup", 10)
    assert calls[0][1] == "s3://dup"
    assert calls[1][0] == 1


def test_write_value_evidence_empty(monkeypatch, spark):
    calls = []
    monkeypatch.setattr(
        svc,
        "spark_write_csv",
        lambda df, path, limit: calls.append((df.count(), path, limit)),
    )
    hm = spark.createDataFrame([], "pk_n_id string")
    empty = spark.createDataFrame([], "pk_n_id string")
    svc._write_value_evidence(
        spark=spark,
        cfg={"PER_COLUMN_MISMATCH_SAMPLE_LIMIT": 1},
        paths={"evidence": {"value_mismatch_samples": "s3://ev"}},
        hash_mismatch=hm,
        s3_hash=empty,
        rs_hash=empty,
        s3_raw=empty,
        rs_raw=empty,
        pk_norm_cols=["pk_n_id"],
        failed_columns=[],
    )
    assert calls[0][1] == "s3://ev"

@pytest.mark.skip(
    reason="Local parquet write needs winutils/HADOOP_HOME on Windows"
)
def test_reconcile_hash_stage_end_to_end(spark, tmp_path, monkeypatch):
    cfg = _cfg()
    manifest = _manifest(tmp_path)
    stage = tmp_path / "stage"
    s3_hash = spark.createDataFrame(
        [("1", "pk1", "row1", "1", "A", "SAME"),
         ("2", "pk2", "row2", "2", "X", "SAME")],
        ["pk_n_id", "pk_hash", "row_hash", "pk_mask",
         "h_amount", "h_status"],
    )
    rs_hash = spark.createDataFrame(
        [("1", "pk1", "rowX", "1", "B", "SAME"),
         ("3", "pk3", "row3", "3", "Y", "SAME")],
        ["pk_n_id", "pk_hash", "row_hash", "pk_mask",
         "h_amount", "h_status"],
    )
    s3_raw = spark.createDataFrame(
        [("1", "10", "A"), ("2", "20", "X")],
        ["pk_n_id", "raw_amount", "raw_status"],
    )
    rs_raw = spark.createDataFrame(
        [("1", "11", "A"), ("3", "30", "Y")],
        ["pk_n_id", "raw_amount", "raw_status"],
    )
    s3_hash.write.parquet(str(stage / "s3_hash"))
    rs_hash.write.parquet(str(stage / "redshift_hash"))
    s3_raw.write.parquet(str(stage / "s3_raw"))
    rs_raw.write.parquet(str(stage / "redshift_raw"))
    writes = []
    monkeypatch.setattr(
        svc,
        "s3_put_json",
        lambda client, uri, payload: writes.append((uri, payload)),
    )
    monkeypatch.setattr(svc, "spark_write_csv", lambda df, path, limit: None)
    status = svc.reconcile_hash_stage(spark, object(), cfg, manifest, "job")
    summary = writes[0][1]
    assert status == "FAIL"
    assert summary["counts"]["s3_only_pk"] == 1
    assert summary["counts"]["redshift_only_pk"] == 1
    assert summary["counts"]["hash_mismatch_pk"] == 1
    assert summary["columns"]["failed_columns"] == 1

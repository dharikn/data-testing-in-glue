from pathlib import Path

import dq_hash_stage_service as svc


class FakeGlueDynamicFrame:
    def __init__(self, df):
        self.df = df

    def toDF(self):
        return self.df


class FakeGlueReader:
    def __init__(self, df):
        self.df = df
        self.calls = []

    def from_options(self, **kwargs):
        self.calls.append(kwargs)
        return FakeGlueDynamicFrame(self.df)


class FakeGlueContext:
    def __init__(self, df):
        self.create_dynamic_frame = FakeGlueReader(df)


def _cfg():
    return {
        "CONFIG_BUCKET": "bucket",
        "CONFIG_BASE_PREFIX": "Fusion_Test",
        "CONFIG_ENV": "dev_config",
        "REDSHIFT_SCHEMA": "bank_cdl",
        "REDSHIFT_TABLE": "deals",
        "REDSHIFT_FILE_NAME_COL": "file_name",
        "REDSHIFT_GLUE_CONNECTION_NAME": "conn",
        "REDSHIFT_DATABASE": "db",
        "REDSHIFT_TMP_DIR": "s3://tmp/",
        "BRONZE_S3_BUCKET": "bronze",
        "BRONZE_S3_PREFIX": "input/",
        "FILENAME_REGEX": "^input\\.csv$",
        "CSV_DELIMITER": "|",
        "PK_COLUMNS": ["id"],
        "IGNORE_COLUMNS": [],
        "PII_COLUMNS": [],
        "HASH_STAGE_REPARTITION": 2,
        "MAPPING_URI": "s3://bucket/map.json",
        "OUTPUT_ROOT": "s3://out/root/",
    }


def test_read_redshift_uses_glue_connection(spark):
    df = spark.createDataFrame([("1",)], ["id"])
    glue = FakeGlueContext(df)
    out = svc._read_redshift(glue, _cfg(), "select 1")
    assert out.count() == 1
    call = glue.create_dynamic_frame.calls[0]
    assert call["connection_type"] == "redshift"
    assert call["connection_options"]["query"] == "select 1"


def test_hash_stage_root():
    cfg = {"CONFIG_BUCKET": "b", "CONFIG_BASE_PREFIX": "p", "CONFIG_ENV": "e"}
    root = svc._hash_stage_root(cfg, "s.t", "f_csv", "01012026_010203")
    assert root == "s3://b/p/e/hash_stage/s.t/f_csv/01012026_010203/"


def test_build_hash_stage_writes_manifest(spark, tmp_path, monkeypatch):
    input_path = tmp_path / "input.csv"
    input_path.write_text("id|amount\n1|10.00\n", encoding="utf-8")
    redshift = spark.createDataFrame([("1", "10.00")], ["id", "amount"])
    glue = FakeGlueContext(redshift)
    written = {}
    monkeypatch.setattr(
        svc,
        "resolve_single_s3_file",
        lambda *a, **k: "input/input.csv",
    )
    monkeypatch.setattr(svc, "s3_join", lambda bucket, key: str(input_path))
    stage_root = (tmp_path / "hash_stage").as_uri() + "/"

    monkeypatch.setattr(
        svc,
        "_hash_stage_root",
        lambda cfg, schema_table, safe_file, run_ts: (
            f"{stage_root}{schema_table}/{safe_file}/{run_ts}/"
        ),
    )
    monkeypatch.setattr(
        svc,
        "s3_put_json",
        lambda client, uri, payload: written.setdefault(uri, payload),
    )

    class DummyWriter:
        def mode(self, mode):
            return self

        def parquet(self, path):
            return None


    def fake_write(self):
        return DummyWriter()


    monkeypatch.setattr(
        "pyspark.sql.dataframe.DataFrame.write",
        property(fake_write),
    )

    manifest = svc.build_hash_stage(spark, glue, object(), _cfg(), "job")
    assert manifest["pk_columns"] == ["id"]
    assert manifest["compare_columns"] == ["amount"]
    assert manifest["s3_hash_path"].startswith("file://")
    assert any(uri.endswith("hash_stage_manifest.json") for uri in written)

import pytest

import dq_config_utils as cfg_mod


def test_private_value_converters():
    assert cfg_mod._str(" abc ", "x") == "abc"
    assert cfg_mod._str(None, "x", "d", required=False) == "d"
    assert cfg_mod._int("10", 1) == 10
    assert cfg_mod._int(None, 7) == 7
    assert cfg_mod._bool(True) is True
    assert cfg_mod._bool("yes") is True
    assert cfg_mod._bool("no") is False
    assert cfg_mod._list([" a ", "", "b"]) == ["a", "b"]


def test_private_converters_raise_on_invalid():
    with pytest.raises(RuntimeError):
        cfg_mod._str(None, "missing")
    with pytest.raises(RuntimeError):
        cfg_mod._list("abc")


def test_build_bootstrap_uri():
    uri = cfg_mod._build_bootstrap_uri("job", "bucket", "a/b/")
    assert uri == "s3://bucket/a/b/job/bootstrap.json"


def test_load_runtime_config_from_s3(monkeypatch):
    docs = {
        "s3://boot/prefix/job/bootstrap.json": {
            "config_bucket": "config-bucket",
            "config_base_prefix": "Fusion_Test",
            "config_env": "dev_config",
            "mapping_name": "map1",
            "output_root": "s3://out/root",
            "fail_job_on_dq": False,
        },
        "s3://config-bucket/Fusion_Test/dev_config/s3_config.json": {
            "bronze_s3_bucket": "bronze",
            "bronze_s3_prefix": "input",
            "csv_delimiter": "|",
        },
        "s3://config-bucket/Fusion_Test/dev_config/"
        "redshift_config.json": {
            "redshift_glue_connection_name": "conn",
            "redshift_database": "db",
            "redshift_tmp_dir": "s3://tmp/",
        },
        "s3://config-bucket/Fusion_Test/dev_config/"
        "mappings/map1.json": {
            "filename_regex": "^file.*\\.csv$",
            "redshift_schema": "sch",
            "redshift_table": "tbl",
            "redshift_file_name_col": "file_name",
            "pk_columns": ["id"],
            "ignore_columns": ["etl_load_ts"],
            "pii_columns": ["id"],
            "column_compare_batch_size": 25,
            "join_repartition": 300,
        },
    }
    monkeypatch.setattr(cfg_mod, "s3_get_json", lambda c, uri: docs[uri])
    cfg = cfg_mod.load_runtime_config_from_s3(
        s3_client=object(),
        job_name="job",
        bootstrap_config_bucket="boot",
        bootstrap_config_prefix="prefix",
    )
    assert cfg["CONFIG_BUCKET"] == "config-bucket"
    assert cfg["BRONZE_S3_PREFIX"] == "input/"
    assert cfg["PK_COLUMNS"] == ["id"]
    assert cfg["JOIN_REPARTITION"] == 300
    assert cfg["HASH_STAGE_REPARTITION"] == 300

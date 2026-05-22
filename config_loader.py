from typing import Any, Dict, List

from s3_io import ensure_trailing_slash, s3_get_json


def _str(v: Any, name: str, default: str = "", required: bool = True) -> str:
    if v is None:
        if required:
            raise RuntimeError(f"Missing config: {name}")
        return default
    s = str(v).strip()
    if required and not s:
        raise RuntimeError(f"Missing config: {name}")
    return s or default


def _int(v: Any, default: int) -> int:
    if v is None or v == "":
        return int(default)
    return int(v)


def _bool(v: Any, default: bool = False) -> bool:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"true", "1", "yes", "y"}


def _list(v: Any) -> List[str]:
    if not v:
        return []
    if not isinstance(v, list):
        raise RuntimeError("Expected list config")
    return [str(x).strip() for x in v if str(x).strip()]


def build_bootstrap_uri(job_name: str, bucket: str, prefix: str) -> str:
    return f"s3://{bucket.strip()}/{prefix.strip().strip('/')}/{job_name.strip()}/bootstrap.json"


def load_runtime_config_from_s3(s3_client, job_name: str, bootstrap_config_bucket: str, bootstrap_config_prefix: str) -> Dict[str, Any]:
    bootstrap_uri = build_bootstrap_uri(job_name, bootstrap_config_bucket, bootstrap_config_prefix)
    bootstrap = s3_get_json(s3_client, bootstrap_uri)

    config_bucket = _str(bootstrap.get("config_bucket"), "config_bucket")
    config_base_prefix = _str(bootstrap.get("config_base_prefix"), "config_base_prefix", "config", required=False).strip("/")
    config_env = _str(bootstrap.get("config_env"), "config_env")
    mapping_name = _str(bootstrap.get("mapping_name"), "mapping_name")
    output_root = ensure_trailing_slash(_str(bootstrap.get("output_root"), "output_root"))
    fail_job_on_dq = _bool(bootstrap.get("fail_job_on_dq"), False)

    base_uri = f"s3://{config_bucket}/{config_base_prefix}/{config_env}/"
    s3_cfg_uri = base_uri + "s3_config.json"
    rs_cfg_uri = base_uri + "redshift_config.json"
    mapping_uri = base_uri + f"mappings/{mapping_name}.json"

    s3_cfg = s3_get_json(s3_client, s3_cfg_uri)
    rs_cfg = s3_get_json(s3_client, rs_cfg_uri)
    mapping = s3_get_json(s3_client, mapping_uri)

    delimiter = str(mapping.get("csv_delimiter") or s3_cfg.get("csv_delimiter") or s3_cfg.get("CSV_DELIMITER") or "|").strip()

    cfg = {
        "BOOTSTRAP_URI": bootstrap_uri,
        "S3_CONFIG_URI": s3_cfg_uri,
        "REDSHIFT_CONFIG_URI": rs_cfg_uri,
        "MAPPING_URI": mapping_uri,
        "CONFIG_BUCKET": config_bucket,
        "CONFIG_BASE_PREFIX": config_base_prefix,
        "CONFIG_ENV": config_env,
        "MAPPING_NAME": mapping_name,
        "OUTPUT_ROOT": output_root,
        "FAIL_JOB_ON_DQ": fail_job_on_dq,
        "BRONZE_S3_BUCKET": _str(s3_cfg.get("bronze_s3_bucket"), "bronze_s3_bucket"),
        "BRONZE_S3_PREFIX": ensure_trailing_slash(_str(s3_cfg.get("bronze_s3_prefix"), "bronze_s3_prefix")),
        "CSV_DELIMITER": delimiter,
        "REDSHIFT_GLUE_CONNECTION_NAME": _str(rs_cfg.get("redshift_glue_connection_name"), "redshift_glue_connection_name"),
        "REDSHIFT_DATABASE": _str(rs_cfg.get("redshift_database"), "redshift_database"),
        "REDSHIFT_TMP_DIR": _str(rs_cfg.get("redshift_tmp_dir"), "redshift_tmp_dir"),
        "FILENAME_REGEX": _str(mapping.get("filename_regex"), "filename_regex"),
        "REDSHIFT_SCHEMA": _str(mapping.get("redshift_schema"), "redshift_schema"),
        "REDSHIFT_TABLE": _str(mapping.get("redshift_table"), "redshift_table"),
        "REDSHIFT_FILE_NAME_COL": _str(mapping.get("redshift_file_name_col"), "redshift_file_name_col"),
        "PK_COLUMNS": _list(mapping.get("pk_columns")),
        "IGNORE_COLUMNS": _list(mapping.get("ignore_columns")),
        "PII_COLUMNS": _list(mapping.get("pii_columns")),
        "PK_DUP_SAMPLE_LIMIT": _int(mapping.get("pk_dup_sample_limit"), 200),
        "ONLY_SAMPLE_LIMIT": _int(mapping.get("only_sample_limit"), 200),
        "HASH_MISMATCH_PK_SAMPLE_LIMIT": _int(mapping.get("hash_mismatch_pk_sample_limit"), 200),
        "PER_COLUMN_MISMATCH_SAMPLE_LIMIT": _int(mapping.get("per_column_mismatch_sample_limit"), 100),
        "VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT": _int(mapping.get("value_mismatch_global_sample_limit"), 100000),
        "COLUMN_COMPARE_BATCH_SIZE": _int(mapping.get("column_compare_batch_size"), 25),
        "JOIN_REPARTITION": _int(mapping.get("join_repartition"), 300),
        "HASH_STAGE_ENABLED": _bool(mapping.get("hash_stage_enabled"), True),
    }
    if not cfg["PK_COLUMNS"]:
        raise RuntimeError("pk_columns cannot be empty")
    return cfg

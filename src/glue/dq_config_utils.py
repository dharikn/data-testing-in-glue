"""Runtime configuration loader for Glue DQ jobs."""

from __future__ import annotations

from typing import Any, Dict, List

from dq_s3_utils import ensure_trailing_slash, s3_get_json


def _str(
    value: Any,
    name: str,
    default: str = "",
    required: bool = True,
) -> str:
    """Read a string config value with validation."""
    if value is None:
        if required:
            raise RuntimeError(f"Missing config field: {name}")
        return default
    text = str(value).strip()
    if required and not text:
        raise RuntimeError(f"Missing config field: {name}")
    return text or default


def _int(value: Any, default: int) -> int:
    """Read an integer config value with default."""
    if value is None or value == "":
        return int(default)
    return int(value)


def _bool(value: Any, default: bool = False) -> bool:
    """Read a boolean config value with default."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _list(value: Any) -> List[str]:
    """Read a list of non-empty strings."""
    if not value:
        return []
    if not isinstance(value, list):
        raise RuntimeError("Expected list config value")
    return [str(item).strip() for item in value if str(item).strip()]


def _build_bootstrap_uri(
    job_name: str,
    bucket: str,
    prefix: str,
) -> str:
    """Build the S3 URI for this job's bootstrap file."""
    return (
        f"s3://{bucket.strip()}/"
        f"{prefix.strip().strip('/')}/"
        f"{job_name.strip()}/bootstrap.json"
    )


def load_runtime_config_from_s3(
    s3_client: Any,
    job_name: str,
    bootstrap_config_bucket: str,
    bootstrap_config_prefix: str,
) -> Dict[str, Any]:
    """Load bootstrap, S3, Redshift, and mapping configs."""
    bootstrap_uri = _build_bootstrap_uri(
        job_name,
        bootstrap_config_bucket,
        bootstrap_config_prefix,
    )
    bootstrap = s3_get_json(s3_client, bootstrap_uri)

    config_bucket = _str(
        bootstrap.get("config_bucket"),
        "config_bucket",
    )
    config_base_prefix = _str(
        bootstrap.get("config_base_prefix"),
        "config_base_prefix",
        "config",
        required=False,
    ).strip("/")
    config_env = _str(bootstrap.get("config_env"), "config_env")
    mapping_name = _str(bootstrap.get("mapping_name"), "mapping_name")
    output_root = ensure_trailing_slash(
        _str(bootstrap.get("output_root"), "output_root")
    )

    base_uri = (
        f"s3://{config_bucket}/"
        f"{config_base_prefix}/{config_env}/"
    )
    s3_cfg_uri = base_uri + "s3_config.json"
    rs_cfg_uri = base_uri + "redshift_config.json"
    mapping_uri = base_uri + f"mappings/{mapping_name}.json"

    s3_cfg = s3_get_json(s3_client, s3_cfg_uri)
    rs_cfg = s3_get_json(s3_client, rs_cfg_uri)
    mapping = s3_get_json(s3_client, mapping_uri)

    delimiter = str(
        mapping.get("csv_delimiter")
        or s3_cfg.get("csv_delimiter")
        or s3_cfg.get("CSV_DELIMITER")
        or "|"
    ).strip()

    join_repartition = _int(mapping.get("join_repartition"), 300)

    cfg: Dict[str, Any] = {
        "BOOTSTRAP_URI": bootstrap_uri,
        "S3_CONFIG_URI": s3_cfg_uri,
        "REDSHIFT_CONFIG_URI": rs_cfg_uri,
        "MAPPING_URI": mapping_uri,
        "CONFIG_BUCKET": config_bucket,
        "CONFIG_BASE_PREFIX": config_base_prefix,
        "CONFIG_ENV": config_env,
        "MAPPING_NAME": mapping_name,
        "OUTPUT_ROOT": output_root,
        "FAIL_JOB_ON_DQ": _bool(
            bootstrap.get("fail_job_on_dq"),
            False,
        ),
        "BRONZE_S3_BUCKET": _str(
            s3_cfg.get("bronze_s3_bucket"),
            "bronze_s3_bucket",
        ),
        "BRONZE_S3_PREFIX": ensure_trailing_slash(
            _str(s3_cfg.get("bronze_s3_prefix"), "bronze_s3_prefix")
        ),
        "CSV_DELIMITER": delimiter,
        "REDSHIFT_GLUE_CONNECTION_NAME": _str(
            rs_cfg.get("redshift_glue_connection_name"),
            "redshift_glue_connection_name",
        ),
        "REDSHIFT_DATABASE": _str(
            rs_cfg.get("redshift_database"),
            "redshift_database",
        ),
        "REDSHIFT_TMP_DIR": _str(
            rs_cfg.get("redshift_tmp_dir"),
            "redshift_tmp_dir",
        ),
        "FILENAME_REGEX": _str(
            mapping.get("filename_regex"),
            "filename_regex",
        ),
        "REDSHIFT_SCHEMA": _str(
            mapping.get("redshift_schema"),
            "redshift_schema",
        ),
        "REDSHIFT_TABLE": _str(
            mapping.get("redshift_table"),
            "redshift_table",
        ),
        "REDSHIFT_FILE_NAME_COL": _str(
            mapping.get("redshift_file_name_col"),
            "redshift_file_name_col",
        ),
        "PK_COLUMNS": _list(mapping.get("pk_columns")),
        "IGNORE_COLUMNS": _list(mapping.get("ignore_columns")),
        "PII_COLUMNS": _list(mapping.get("pii_columns")),
        "PK_DUP_SAMPLE_LIMIT": _int(
            mapping.get("pk_dup_sample_limit"),
            200,
        ),
        "ONLY_SAMPLE_LIMIT": _int(
            mapping.get("only_sample_limit"),
            200,
        ),
        "HASH_MISMATCH_PK_SAMPLE_LIMIT": _int(
            mapping.get("hash_mismatch_pk_sample_limit"),
            200,
        ),
        "PER_COLUMN_MISMATCH_SAMPLE_LIMIT": _int(
            mapping.get("per_column_mismatch_sample_limit"),
            100,
        ),
        "VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT": _int(
            mapping.get("value_mismatch_global_sample_limit"),
            100000,
        ),
        "COLUMN_COMPARE_BATCH_SIZE": _int(
            mapping.get("column_compare_batch_size"),
            25,
        ),
        "JOIN_REPARTITION": join_repartition,
        "HASH_STAGE_REPARTITION": _int(
            mapping.get("hash_stage_repartition"),
            join_repartition,
        ),
    }

    if not cfg["PK_COLUMNS"]:
        raise RuntimeError("pk_columns cannot be empty")

    return cfg

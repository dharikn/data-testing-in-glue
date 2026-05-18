from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from .s3_io import ensure_trailing_slash, s3_get_json


def _as_str(v: Any, field_name: str, required: bool = True, default: str = "") -> str:
    if v is None:
        if required:
            raise RuntimeError(f"Missing required config field: {field_name}")
        return default
    s = str(v).strip()
    if required and not s:
        raise RuntimeError(f"Missing required config field: {field_name}")
    return s if s else default


def _as_int(v: Any, field_name: str, default: int) -> int:
    if v is None or v == "":
        return int(default)
    try:
        return int(v)
    except Exception as exc:
        raise RuntimeError(f"Config field '{field_name}' must be an integer.") from exc


def _as_bool(v: Any, field_name: str, default: bool) -> bool:
    if v is None or v == "":
        return bool(default)
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    raise RuntimeError(f"Config field '{field_name}' must be boolean.")


def _as_list_of_str(v: Any, field_name: str) -> List[str]:
    if v is None:
        return []
    if not isinstance(v, list):
        raise RuntimeError(f"Config field '{field_name}' must be a list.")
    return [str(x).strip() for x in v if str(x).strip()]


def _parse_filter_expr(v: Any, field_name: str) -> Tuple[str, str]:
    s = _as_str(v, field_name)
    m = re.match(r"^(<=|>=|=|<|>)(.+)$", s)
    if not m:
        raise RuntimeError(
            f"Config field '{field_name}' must start with <=, >=, =, <, >. Got: {s}"
        )
    return m.group(1).strip(), m.group(2).strip()


def _build_bootstrap_config_uri(job_name: str, bucket: str, prefix: str) -> str:
    jn = str(job_name).strip()
    bucket = str(bucket).strip()
    prefix = str(prefix).strip().strip("/")
    if not jn or not bucket or not prefix:
        raise RuntimeError(
            "JOB_NAME / BOOTSTRAP_CONFIG_BUCKET / BOOTSTRAP_CONFIG_PREFIX cannot be empty"
        )
    return f"s3://{bucket}/{prefix}/{jn}/bootstrap.json"


def load_runtime_config_from_s3(
    s3_client, job_name: str, bootstrap_config_bucket: str, bootstrap_config_prefix: str
) -> Dict[str, Any]:
    bootstrap_uri = _build_bootstrap_config_uri(
        job_name, bootstrap_config_bucket, bootstrap_config_prefix
    )
    bootstrap = s3_get_json(s3_client, bootstrap_uri)
    config_bucket = _as_str(bootstrap.get("config_bucket"), "config_bucket")
    config_base_prefix = _as_str(
        bootstrap.get("config_base_prefix"), "config_base_prefix", False, "config"
    ).strip("/")
    config_env = _as_str(bootstrap.get("config_env"), "config_env")
    mapping_name = _as_str(bootstrap.get("mapping_name"), "mapping_name")
    output_root = ensure_trailing_slash(
        _as_str(bootstrap.get("output_root"), "output_root")
    )
    base_uri = f"s3://{config_bucket}/{config_base_prefix}/{config_env}/"
    s3_cfg_uri = base_uri + "s3_config.json"
    rs_cfg_uri = base_uri + "redshift_config.json"
    mapping_uri = base_uri + f"mappings/{mapping_name}.json"
    s3_cfg = s3_get_json(s3_client, s3_cfg_uri)
    rs_cfg = s3_get_json(s3_client, rs_cfg_uri)
    mapping_cfg = s3_get_json(s3_client, mapping_uri)
    cfg = {}
    cfg.update(s3_cfg)
    cfg.update(rs_cfg)
    cfg.update(mapping_cfg)
    csv_delim = str(
        mapping_cfg.get("csv_delimiter")
        or cfg.get("CSV_DELIMITER")
        or cfg.get("csv_delimiter")
        or "|"
    ).strip()
    dt_cfg = mapping_cfg.get("date_time_filter") or {}
    if dt_cfg and not isinstance(dt_cfg, dict):
        raise RuntimeError("Config field 'date_time_filter' must be an object.")
    dt_enabled = _as_bool(dt_cfg.get("enabled"), "date_time_filter.enabled", False)
    dt_column = dt_from_raw = dt_to_raw = dt_from_op = dt_from_val = dt_to_op = (
        dt_to_val
    ) = ""
    if dt_enabled:
        dt_column = _as_str(dt_cfg.get("column"), "date_time_filter.column")
        dt_from_raw = _as_str(dt_cfg.get("from"), "date_time_filter.from", False, "")
        dt_to_raw = _as_str(dt_cfg.get("to"), "date_time_filter.to", False, "")
        if not dt_from_raw and not dt_to_raw:
            raise RuntimeError(
                "date_time_filter.enabled=true but neither 'from' nor 'to' is provided."
            )
        if dt_from_raw:
            dt_from_op, dt_from_val = _parse_filter_expr(
                dt_from_raw, "date_time_filter.from"
            )
        if dt_to_raw:
            dt_to_op, dt_to_val = _parse_filter_expr(dt_to_raw, "date_time_filter.to")
    out = {
        "BOOTSTRAP_URI": bootstrap_uri,
        "S3_CONFIG_URI": s3_cfg_uri,
        "REDSHIFT_CONFIG_URI": rs_cfg_uri,
        "MAPPING_URI": mapping_uri,
        "CONFIG_BUCKET": config_bucket,
        "CONFIG_BASE_PREFIX": config_base_prefix,
        "CONFIG_ENV": config_env,
        "MAPPING_NAME": mapping_name,
        "BRONZE_S3_BUCKET": _as_str(s3_cfg.get("bronze_s3_bucket"), "bronze_s3_bucket"),
        "BRONZE_S3_PREFIX": ensure_trailing_slash(
            _as_str(s3_cfg.get("bronze_s3_prefix"), "bronze_s3_prefix")
        ),
        "CSV_DELIMITER": csv_delim,
        "REDSHIFT_GLUE_CONNECTION_NAME": _as_str(
            rs_cfg.get("redshift_glue_connection_name"), "redshift_glue_connection_name"
        ),
        "REDSHIFT_DATABASE": _as_str(
            rs_cfg.get("redshift_database"), "redshift_database"
        ),
        "REDSHIFT_TMP_DIR": _as_str(rs_cfg.get("redshift_tmp_dir"), "redshift_tmp_dir"),
        "FILENAME_REGEX": _as_str(mapping_cfg.get("filename_regex"), "filename_regex"),
        "REDSHIFT_SCHEMA": _as_str(
            mapping_cfg.get("redshift_schema"), "redshift_schema"
        ),
        "REDSHIFT_TABLE": _as_str(mapping_cfg.get("redshift_table"), "redshift_table"),
        "REDSHIFT_FILE_NAME_COL": _as_str(
            mapping_cfg.get("redshift_file_name_col"), "redshift_file_name_col"
        ),
        "PK_COLUMNS": _as_list_of_str(mapping_cfg.get("pk_columns"), "pk_columns"),
        "IGNORE_COLUMNS": _as_list_of_str(
            mapping_cfg.get("ignore_columns"), "ignore_columns"
        ),
        "PII_COLUMNS": _as_list_of_str(mapping_cfg.get("pii_columns"), "pii_columns"),
        "PK_DUP_SAMPLE_LIMIT": _as_int(
            mapping_cfg.get("pk_dup_sample_limit"), "pk_dup_sample_limit", 200
        ),
        "ONLY_SAMPLE_LIMIT": _as_int(
            mapping_cfg.get("only_sample_limit"), "only_sample_limit", 200
        ),
        "HASH_MISMATCH_PK_SAMPLE_LIMIT": _as_int(
            mapping_cfg.get("hash_mismatch_pk_sample_limit"),
            "hash_mismatch_pk_sample_limit",
            200,
        ),
        "PER_COLUMN_MISMATCH_SAMPLE_LIMIT": _as_int(
            mapping_cfg.get("per_column_mismatch_sample_limit"),
            "per_column_mismatch_sample_limit",
            100,
        ),
        "MAX_HASH_MISMATCH_ROWS_FOR_COLUMN_COMPARE": _as_int(
            mapping_cfg.get("max_hash_mismatch_rows_for_column_compare"),
            "max_hash_mismatch_rows_for_column_compare",
            10000,
        ),
        "COLUMN_COMPARE_BATCH_SIZE": _as_int(
            mapping_cfg.get("column_compare_batch_size"),
            "column_compare_batch_size",
            25,
        ),
        "VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT": _as_int(
            mapping_cfg.get("value_mismatch_global_sample_limit"),
            "value_mismatch_global_sample_limit",
            100000,
        ),
        "JOIN_REPARTITION": _as_int(
            mapping_cfg.get("join_repartition"), "join_repartition", 150
        ),
        "OUTPUT_ROOT": output_root,
        "FAIL_JOB_ON_DQ": _as_bool(
            bootstrap.get("fail_job_on_dq"), "fail_job_on_dq", False
        ),
        "DATE_TIME_FILTER_ENABLED": dt_enabled,
        "DATE_TIME_FILTER_COLUMN": dt_column,
        "DATE_TIME_FILTER_FROM_RAW": dt_from_raw,
        "DATE_TIME_FILTER_TO_RAW": dt_to_raw,
        "DATE_TIME_FILTER_FROM_OP": dt_from_op,
        "DATE_TIME_FILTER_FROM_VAL": dt_from_val,
        "DATE_TIME_FILTER_TO_OP": dt_to_op,
        "DATE_TIME_FILTER_TO_VAL": dt_to_val,
        "CHECKPOINT_ENABLED": _as_bool(
            mapping_cfg.get("checkpoint_enabled"), "checkpoint_enabled", True
        ),
    }
    if not out["PK_COLUMNS"]:
        raise RuntimeError("Config field 'pk_columns' must not be empty.")
    return out

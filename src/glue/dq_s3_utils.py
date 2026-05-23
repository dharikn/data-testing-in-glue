"""S3 helper functions used by Glue DQ jobs."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame


def ensure_trailing_slash(value: str) -> str:
    """Ensure an S3 prefix-like string ends with slash."""
    return value if value.endswith("/") else value + "/"


def s3_parse_uri(uri: str) -> Tuple[str, str]:
    """Split an S3 URI into bucket and key."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an S3 URI: {uri}")
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key


def s3_join(bucket: str, key: str) -> str:
    """Build an S3 URI from bucket and key."""
    return f"s3://{bucket}/{key.lstrip('/')}"


def s3_get_json(s3_client: Any, uri: str) -> Dict[str, Any]:
    """Read a JSON object from S3."""
    bucket, key = s3_parse_uri(uri)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def s3_put_json(
    s3_client: Any,
    uri: str,
    payload: Dict[str, Any],
) -> None:
    """Write a JSON object to S3."""
    bucket, key = s3_parse_uri(uri)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            payload,
            indent=2,
            default=str,
        ).encode("utf-8"),
        ContentType="application/json",
    )


def s3_list_keys(
    s3_client: Any,
    bucket: str,
    prefix: str,
) -> List[str]:
    """List all S3 keys below a prefix using pagination."""
    keys: List[str] = []
    token = None
    while True:
        kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3_client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []) or []:
            key = obj.get("Key")
            if key:
                keys.append(key)
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return keys


def resolve_single_s3_file(
    s3_client: Any,
    bucket: str,
    prefix: str,
    filename_regex: str,
) -> str:
    """Resolve exactly one S3 key by matching regex on filename."""
    normal_prefix = prefix.lstrip("/")
    if normal_prefix and not normal_prefix.endswith("/"):
        normal_prefix += "/"

    pattern = re.compile(filename_regex)
    matched: List[str] = []

    for key in s3_list_keys(s3_client, bucket, normal_prefix):
        file_name = key.rsplit("/", 1)[-1]
        if pattern.match(file_name):
            matched.append(key)

    if not matched:
        raise RuntimeError(
            "No file matched regex "
            f"{filename_regex} under s3://{bucket}/{normal_prefix}"
        )

    if len(matched) > 1:
        raise RuntimeError(
            "More than one file matched regex "
            f"{filename_regex}. sample={matched[:10]}"
        )

    return matched[0]


def spark_write_csv(
    df: DataFrame,
    s3_uri: str,
    limit: int,
) -> None:
    """Write capped CSV evidence with one output part."""
    (
        df.limit(int(limit))
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .csv(s3_uri)
    )

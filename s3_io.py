from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame


def ensure_trailing_slash(s: str) -> str:
    return s if s.endswith("/") else s + "/"


def s3_parse_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an s3 uri: {uri}")
    rest = uri[5:]
    parts = rest.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def s3_join(bucket: str, key: str) -> str:
    return f's3://{bucket}/{key.lstrip("/")}'


def s3_get_json(s3_client, uri: str) -> Dict[str, Any]:
    b, k = s3_parse_uri(uri)
    obj = s3_client.get_object(Bucket=b, Key=k)
    return json.loads(obj["Body"].read().decode("utf-8"))


def s3_put_json(s3_client, uri: str, payload: Dict[str, Any]) -> None:
    b, k = s3_parse_uri(uri)
    s3_client.put_object(
        Bucket=b,
        Key=k,
        Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def s3_list_keys(s3_client, bucket: str, prefix: str) -> List[str]:
    keys = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kw)
        for obj in resp.get("Contents", []) or []:
            if obj.get("Key"):
                keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def resolve_single_s3_file(
    s3_client, bucket: str, prefix: str, filename_regex: str
) -> str:
    pfx = prefix.lstrip("/")
    if pfx and not pfx.endswith("/"):
        pfx += "/"
    rx = re.compile(filename_regex)
    matched = []
    for key in s3_list_keys(s3_client, bucket, pfx):
        if rx.match(key.rsplit("/", 1)[-1]):
            matched.append(key)
    if not matched:
        raise RuntimeError(
            f"No files matched regex under s3://{bucket}/{pfx}. regex={filename_regex}"
        )
    if len(matched) > 1:
        raise RuntimeError(
            f"More than one file matched regex. count={len(matched)} sample={matched[:10]}"
        )
    return matched[0]


def spark_write_csv(df: DataFrame, s3_uri: str, limit: int) -> None:
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

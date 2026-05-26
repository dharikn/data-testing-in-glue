import pytest

from dq_s3_utils import (
    ensure_trailing_slash,
    resolve_single_s3_file,
    s3_get_json,
    s3_join,
    s3_list_keys,
    s3_parse_uri,
    s3_put_json,
)


def test_ensure_trailing_slash():
    assert ensure_trailing_slash("abc") == "abc/"
    assert ensure_trailing_slash("abc/") == "abc/"


def test_s3_parse_uri_valid():
    bucket, key = s3_parse_uri("s3://bucket/a/b.json")
    assert bucket == "bucket"
    assert key == "a/b.json"


def test_s3_parse_uri_rejects_non_s3():
    with pytest.raises(ValueError):
        s3_parse_uri("file:///tmp/a")


def test_s3_join_builds_uri():
    assert s3_join("bucket", "/a/b") == "s3://bucket/a/b"


def test_s3_get_and_put_json(fake_s3_client):
    uri = "s3://bucket/config/a.json"
    payload = {"a": 1}
    s3_put_json(fake_s3_client, uri, payload)
    assert s3_get_json(fake_s3_client, uri) == payload


def test_s3_list_keys_handles_pagination(fake_s3_client):
    fake_s3_client.pages = [
        {
            "Contents": [{"Key": "a/one.csv"}],
            "IsTruncated": True,
            "NextContinuationToken": "n1",
        },
        {"Contents": [{"Key": "a/two.csv"}],
         "IsTruncated": False},
    ]
    assert s3_list_keys(fake_s3_client, "bucket", "a/") == [
        "a/one.csv", "a/two.csv"
    ]


def test_resolve_single_s3_file_matches_basename(fake_s3_client):
    fake_s3_client.pages = [
        {
            "Contents": [
                {"Key": "root/f1.csv"},
                {"Key": "root/f2.txt"},
            ],
            "IsTruncated": False,
        }
    ]
    key = resolve_single_s3_file(
        fake_s3_client, "bucket", "root", r"^f1\.csv$"
    )
    assert key == "root/f1.csv"


def test_resolve_single_s3_file_rejects_zero_matches(fake_s3_client):
    fake_s3_client.pages = [{"Contents": [], "IsTruncated": False}]
    with pytest.raises(RuntimeError):
        resolve_single_s3_file(
            fake_s3_client, "bucket", "root", r"^missing\.csv$"
        )


def test_resolve_single_s3_file_rejects_many_matches(fake_s3_client):
    fake_s3_client.pages = [
        {
            "Contents": [
                {"Key": "root/f1.csv"},
                {"Key": "root/f2.csv"},
            ],
            "IsTruncated": False,
        }
    ]
    with pytest.raises(RuntimeError):
        resolve_single_s3_file(
            fake_s3_client, "bucket", "root", r"^f\d\.csv$"
        )

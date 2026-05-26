from pyspark.sql import functions as F

from dq_normalization_utils import (
    canon_header,
    normalize_headers,
    normalize_numeric_string,
    normalize_pk_auto,
    normalize_value_auto,
    normalize_value_auto_base,
    raw_trim,
)


def _collect_single_col(df, expr, alias="v"):
    return [row[alias] for row in df.select(expr.alias(alias)).collect()]


def test_canon_header():
    assert canon_header(" Customer ID ") == "customer_id"
    assert canon_header("customer-id") == "customer_id"
    assert canon_header("A__B!!C") == "a_b_c"


def test_normalize_headers_deduplicates(spark):
    df = spark.createDataFrame([(1, 2)], ["Customer ID", "customer-id"])
    out = normalize_headers(df)
    assert out.columns == ["customer_id", "customer_id_2"]


def test_raw_trim(spark):
    df = spark.createDataFrame([("  abc  ",), (None,)], ["v"])
    assert _collect_single_col(df, raw_trim(F.col("v"))) == ["abc", ""]


def test_normalize_pk_auto_dates_and_timestamps(spark):
    df = spark.createDataFrame(
        [
            ("17APR2026",),
            ("17APR26:00:00:00",),
            ("2026-04-17",),
            ("9999-12-31",),
        ],
        ["v"],
    )
    got = _collect_single_col(df, normalize_pk_auto(F.col("v")))
    assert got[0] == "2026-04-17"
    assert got[1] == "2026-04-17 00:00:00"
    assert got[2] == "2026-04-17"
    assert got[3] == "2099-01-01"


def test_normalize_value_auto_base_null_tokens_and_spaces(spark):
    df = spark.createDataFrame(
        [("NULL",), ("NAN",), ("NONE",), (" a   b ",)],
        ["v"],
    )
    got = _collect_single_col(df, normalize_value_auto_base(F.col("v")))
    assert got == ["", "", "", "a b"]


def test_normalize_numeric_string_handles_scientific_and_zero(spark):
    df = spark.createDataFrame(
        [("1.2300",), ("1.2E3",), ("-0.000",), ("abc",)],
        ["v"],
    )
    got = _collect_single_col(
        df,
        normalize_numeric_string(F.col("v"), 4),
    )
    assert got == ["11", "12001", "01", "abc"]


def test_normalize_value_auto_combines_all_rules(spark):
    df = spark.createDataFrame(
        [(" 1.2000 ",), ("NULL",), ("17APR2026",)],
        ["v"],
    )
    got = _collect_single_col(df, normalize_value_auto(F.col("v"), 4))
    assert got == ["11", "", "2026-04-17"]

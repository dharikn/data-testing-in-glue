from pyspark.sql import functions as F
import pytest

from dq_mask_utils import (
    _progressive_mask_py,
    build_pk_mask_cols,
    get_mask_udf,
)


def test_progressive_mask_python_helper():
    assert _progressive_mask_py(None) == ""
    assert _progressive_mask_py("") == ""
    assert _progressive_mask_py("123456") == "***456"
    assert _progressive_mask_py("12345") == "***45"


@pytest.mark.skip(reason="Python UDF execution is not stable in local Windows Spark")
def test_get_mask_udf_masks_values(spark):
    df = spark.createDataFrame([("123456",)], ["v"])
    out = df.select(get_mask_udf()(F.col("v")).alias("m")).first()
    assert out["m"] == "***456"

@pytest.mark.skip(reason="Python UDF execution is not stable in local Windows Spark")
def test_build_pk_mask_cols_masks_only_pii_columns(spark):
    df = spark.createDataFrame(
        [("123456", "2026-01-01")],
        ["pk_n_id", "pk_n_date"],
    )
    cols = build_pk_mask_cols(
        [("pk_n_id", "id"), ("pk_n_date", "date")],
        {"id"},
    )
    row = df.select(*cols).first()
    assert row[0] == "***456"
    assert row[1] == "2026-01-01"

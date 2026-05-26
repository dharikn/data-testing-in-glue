from dq_hash_stage_utils import (
    build_hash_stage_dataset,
    build_raw_stage_dataset,
)


def test_build_hash_stage_dataset_creates_hash_columns(spark):
    df = spark.createDataFrame(
        [("123456", "10.00", "abc"), ("999999", "11.00", "def")],
        ["id", "amount", "status"],
    )
    out = build_hash_stage_dataset(
        df=df,
        pk_cols=["id"],
        compare_cols=["amount", "status"],
        scale_by_col={"amount": 2},
        pii_set=set(),
    )
    assert "pk_n_id" in out.columns
    assert "h_amount" in out.columns
    assert "h_status" in out.columns
    assert "pk_hash" in out.columns
    assert "row_hash" in out.columns
    assert "pk_mask" in out.columns
    
    


def test_build_raw_stage_dataset_creates_raw_columns(spark):
    df = spark.createDataFrame(
        [("1", "  abc  ", "10.00")],
        ["id", "name", "amount"],
    )
    out = build_raw_stage_dataset(
        df=df,
        pk_cols=["id"],
        compare_cols=["name", "amount"],
    )
    assert out.columns == ["pk_n_id", "raw_name", "raw_amount"]
    assert out.first()["raw_name"] == "abc"

import pytest

from dq_redshift_utils import _safe_ident, build_redshift_query


def test_safe_ident_accepts_safe_names():
    assert _safe_ident("abc_123") == "abc_123"


@pytest.mark.parametrize("value", ["", "abc-1", "abc.name"])
def test_safe_ident_rejects_unsafe_names(value):
    with pytest.raises(ValueError):
        _safe_ident(value)


def test_build_redshift_query_with_file_and_file_without_ext():
    query = build_redshift_query(
        file_name="abc.csv",
        file_name_no_ext="abc",
        schema="silver",
        table="customers",
        file_col="file_name",
    )
    assert "silver.customers" in query
    assert "file_name IN ('abc.csv', 'abc')" in query


def test_build_redshift_query_escapes_quotes():
    query = build_redshift_query(
        file_name="a'b.csv",
        file_name_no_ext="a'b",
        schema="silver",
        table="customers",
        file_col="file_name",
    )
    assert "a''b.csv" in query

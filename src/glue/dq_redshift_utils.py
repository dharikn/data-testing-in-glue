"""Redshift SQL construction utilities."""

from __future__ import annotations


def _safe_ident(name: str) -> str:
    """Validate a Redshift identifier used in generated SQL."""
    if not name:
        raise ValueError("Empty identifier")
    for char in name:
        if not (char.isalnum() or char == "_"):
            raise ValueError(f"Unsafe identifier: {name}")
    return name


def build_redshift_query(
    file_name: str,
    file_name_no_ext: str,
    schema: str,
    table: str,
    file_col: str,
) -> str:
    """Build Redshift query filtered to the target file."""
    safe_schema = _safe_ident(schema)
    safe_table = _safe_ident(table)
    safe_file_col = _safe_ident(file_col)

    file_with_ext = file_name.replace("'", "''")
    file_without_ext = file_name_no_ext.replace("'", "''")

    if file_with_ext != file_without_ext:
        where_clause = (
            f"{safe_file_col} IN "
            f"('{file_with_ext}', '{file_without_ext}')"
        )
    else:
        where_clause = f"{safe_file_col} = '{file_with_ext}'"

    return (
        f"SELECT * FROM {safe_schema}.{safe_table} "
        f"WHERE {where_clause}"
    )

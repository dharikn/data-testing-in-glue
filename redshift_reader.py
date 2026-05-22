def _safe_ident(name: str) -> str:
    if not name:
        raise ValueError("empty identifier")
    for ch in name:
        if not (ch.isalnum() or ch == "_"):
            raise ValueError(f"unsafe identifier: {name}")
    return name


def build_redshift_query(file_name: str, file_name_no_ext: str, schema: str, table: str, file_col: str) -> str:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    file_col = _safe_ident(file_col)
    f1 = file_name.replace("'", "''")
    f2 = file_name_no_ext.replace("'", "''")
    if f1 != f2:
        where = f"{file_col} IN ('{f1}', '{f2}')"
    else:
        where = f"{file_col} = '{f1}'"
    return f"SELECT * FROM {schema}.{table} WHERE {where}"

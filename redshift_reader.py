from __future__ import annotations

def _safe_ident(name: str) -> str:
    if not name: raise ValueError('Empty identifier')
    for ch in name:
        if not (ch.isalnum() or ch == '_'): raise ValueError(f'Unsafe identifier: {name!r}')
    return name

def build_redshift_query(file_name, file_name_no_ext, redshift_schema, redshift_table, redshift_file_name_col, date_time_filter_enabled=False, date_time_filter_column='', date_time_filter_from_op='', date_time_filter_from_val='', date_time_filter_to_op='', date_time_filter_to_val=''):
    schema=_safe_ident(redshift_schema); table=_safe_ident(redshift_table); filecol=_safe_ident(redshift_file_name_col)
    sf=str(file_name).replace("'","''"); sn=str(file_name_no_ext).replace("'","''")
    where=[f"{filecol} IN ('{sf}', '{sn}')"] if sf!=sn else [f"{filecol} = '{sf}'"]
    if date_time_filter_enabled:
        dt=_safe_ident(date_time_filter_column)
        if date_time_filter_from_op and date_time_filter_from_val: where.append(f"{dt} {date_time_filter_from_op} '{str(date_time_filter_from_val).replace(chr(39),chr(39)+chr(39))}'")
        if date_time_filter_to_op and date_time_filter_to_val: where.append(f"{dt} {date_time_filter_to_op} '{str(date_time_filter_to_val).replace(chr(39),chr(39)+chr(39))}'")
    return f"SELECT * FROM {schema}.{table} WHERE " + ' AND '.join(where)

def build_redshift_numeric_scale_query(redshift_schema, redshift_table):
    s=str(redshift_schema).replace("'","''"); t=str(redshift_table).replace("'","''")
    return f"SELECT column_name, data_type, numeric_scale FROM information_schema.columns WHERE table_schema = '{s}' AND table_name = '{t}'"

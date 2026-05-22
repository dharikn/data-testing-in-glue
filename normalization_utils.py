import re
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def canon_header(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def normalize_headers(df: DataFrame) -> DataFrame:
    seen = {}
    final = []
    for c in df.columns:
        n = canon_header(c)
        seen[n] = seen.get(n, 0) + 1
        final.append(n if seen[n] == 1 else f"{n}_{seen[n]}")
    out = df
    for old, new in zip(df.columns, final):
        if old != new:
            out = out.withColumnRenamed(old, new)
    return out


def _base_string(col: Column) -> Column:
    s = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    return F.regexp_replace(s, r"\s+", " ")


def normalize_pk_auto(col: Column) -> Column:
    s = _base_string(col)
    up = F.upper(s)

    ts_ddmonyyyy = F.when(up.rlike(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$"), F.to_timestamp(up, "ddMMMyyyy:HH:mm:ss"))
    ts_ddmonyy = F.when(
        up.rlike(r"^\d{2}[A-Z]{3}\d{2}:\d{2}:\d{2}:\d{2}$") & ~up.rlike(r"^\d{2}[A-Z]{3}99:\d{2}:\d{2}:\d{2}$"),
        F.to_timestamp(F.concat(F.substring(up, 1, 5), F.lit("20"), F.substring(up, 6, 2), F.substring(up, 8, 9)), "ddMMMyyyy:HH:mm:ss"),
    )
    ts_high_99 = F.when(
        up.rlike(r"^\d{2}[A-Z]{3}99:\d{2}:\d{2}:\d{2}$"),
        F.to_timestamp(F.concat(F.substring(up, 1, 5), F.lit("2099"), F.substring(up, 8, 9)), "ddMMMyyyy:HH:mm:ss"),
    )
    ts_iso_raw = F.when(s.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$"), F.to_timestamp(s))
    ts_iso = F.when(
        ts_iso_raw.isNotNull() & (F.year(ts_iso_raw) == 9999),
        F.to_timestamp(F.concat(F.lit("2099"), F.date_format(ts_iso_raw, "-MM-dd HH:mm:ss")), "yyyy-MM-dd HH:mm:ss"),
    ).otherwise(ts_iso_raw)

    d_ddmonyyyy = F.when(up.rlike(r"^\d{2}[A-Z]{3}\d{4}$"), F.to_date(up, "ddMMMyyyy"))
    d_yyyy_raw = F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s, "yyyy-MM-dd"))
    d_yyyy = F.when(d_yyyy_raw.isNotNull() & (F.year(d_yyyy_raw) == 9999), F.to_date(F.lit("2099-01-01"))).otherwise(d_yyyy_raw)
    d_ddmonyy = F.when(
        up.rlike(r"^\d{2}[A-Z]{3}\d{2}$") & ~up.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(F.concat(F.substring(up, 1, 5), F.lit("20"), F.substring(up, 6, 2)), "ddMMMyyyy"),
    )
    d_high_99 = F.when(
        up.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(F.concat(F.substring(up, 1, 5), F.lit("2099")), "ddMMMyyyy"),
    )

    return (
        F.when(ts_iso.isNotNull(), F.date_format(ts_iso, "yyyy-MM-dd HH:mm:ss"))
        .when(ts_ddmonyyyy.isNotNull(), F.date_format(ts_ddmonyyyy, "yyyy-MM-dd HH:mm:ss"))
        .when(ts_high_99.isNotNull(), F.date_format(ts_high_99, "yyyy-MM-dd HH:mm:ss"))
        .when(ts_ddmonyy.isNotNull(), F.date_format(ts_ddmonyy, "yyyy-MM-dd HH:mm:ss"))
        .when(d_ddmonyyyy.isNotNull(), F.date_format(d_ddmonyyyy, "yyyy-MM-dd"))
        .when(d_high_99.isNotNull(), F.date_format(d_high_99, "yyyy-MM-dd"))
        .when(d_ddmonyy.isNotNull(), F.date_format(d_ddmonyy, "yyyy-MM-dd"))
        .when(d_yyyy.isNotNull(), F.date_format(d_yyyy, "yyyy-MM-dd"))
        .otherwise(s)
    )


def normalize_value_auto_base(col: Column) -> Column:
    s = _base_string(col)
    up = F.upper(s)
    s2 = F.when(up.isin("NULL", "NAN", "NONE"), F.lit("")).otherwise(s)
    return normalize_pk_auto(s2)


def normalize_numeric_string(s: Column, scale: Optional[int]) -> Column:
    is_num = s.rlike(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$") | s.rlike(r"^[+-]?\d+[eE][+-]?\d+$")
    dec = s.cast(f"decimal(38,{int(scale)})") if scale is not None and int(scale) >= 0 else s.cast("decimal(38,18)")
    dec_str = dec.cast("string")
    dec_str = F.regexp_replace(dec_str, r"(\.\d*?)0+$", r"\1")
    dec_str = F.regexp_replace(dec_str, r"\.$", "")
    dec_str = F.when(dec_str == "-0", F.lit("0")).otherwise(dec_str)
    dec_str = F.when(dec_str.isNull(), s).otherwise(dec_str)
    return F.when(is_num, dec_str).otherwise(s)


def normalize_value_auto(col: Column, scale: Optional[int]) -> Column:
    return normalize_numeric_string(normalize_value_auto_base(col), scale)


def raw_trim(col: Column) -> Column:
    return F.trim(F.coalesce(col.cast("string"), F.lit("")))

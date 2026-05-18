from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def canon_header(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def normalize_headers(df: DataFrame) -> DataFrame:
    raw = df.columns
    new = [canon_header(c) for c in raw]
    seen = {}
    final = []
    for n in new:
        seen[n] = seen.get(n, 0) + 1
        final.append(n if seen[n] == 1 else f"{n}_{seen[n]}")
    out = df
    for old, nw in zip(raw, final):
        if old != nw:
            out = out.withColumnRenamed(old, nw)
    return out


def normalize_pk_auto(col: Column) -> Column:
    s = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    s = F.regexp_replace(s, r"\s+", " ")
    u = F.upper(s)
    d0 = F.when(
        u.rlike(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$"),
        F.to_date(u, "ddMMMyyyy:HH:mm:ss"),
    )
    d1 = F.when(u.rlike(r"^\d{2}[A-Z]{3}\d{4}$"), F.to_date(u, "ddMMMyyyy"))
    d2r = F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s, "yyyy-MM-dd"))
    d2 = F.when(
        d2r.isNotNull() & (F.year(d2r) == 9999), F.to_date(F.lit("2099-01-01"))
    ).otherwise(d2r)
    d3 = F.when(
        u.rlike(r"^\d{2}[A-Z]{3}\d{2}$") & ~u.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(
            F.concat(F.substring(u, 1, 5), F.lit("20"), F.substring(u, 6, 2)),
            "ddMMMyyyy",
        ),
    )
    d4 = F.when(
        u.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(F.concat(F.substring(u, 1, 5), F.lit("2099")), "ddMMMyyyy"),
    )
    d5 = F.when(u.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(u, "dd/MM/yyyy"))
    d6 = F.when(
        u.rlike(r"^\d{2}/\d{2}/\d{2}$") & ~u.rlike(r"^\d{2}/\d{2}/99$"),
        F.to_date(
            F.concat(F.substring(u, 1, 6), F.lit("20"), F.substring(u, 7, 2)),
            "dd/MM/yyyy",
        ),
    )
    d7 = F.when(
        u.rlike(r"^\d{2}/\d{2}/99$"),
        F.to_date(F.concat(F.substring(u, 1, 6), F.lit("2099")), "dd/MM/yyyy"),
    )
    return (
        F.when(d0.isNotNull(), F.date_format(d0, "yyyy-MM-dd"))
        .when(d5.isNotNull(), F.date_format(d5, "yyyy-MM-dd"))
        .when(d7.isNotNull(), F.date_format(d7, "yyyy-MM-dd"))
        .when(d6.isNotNull(), F.date_format(d6, "yyyy-MM-dd"))
        .when(d4.isNotNull(), F.date_format(d4, "yyyy-MM-dd"))
        .when(d3.isNotNull(), F.date_format(d3, "yyyy-MM-dd"))
        .when(d1.isNotNull(), F.date_format(d1, "yyyy-MM-dd"))
        .when(d2.isNotNull(), F.date_format(d2, "yyyy-MM-dd"))
        .otherwise(s)
    )


def normalize_value_auto_base(col: Column) -> Column:
    s = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    s = F.regexp_replace(s, r"\s+", " ")
    u = F.upper(s)
    s2 = F.when(u.isin("NULL", "NAN", "NONE"), F.lit("")).otherwise(s)
    u2 = F.upper(s2)
    d1 = F.when(u2.rlike(r"^\d{2}[A-Z]{3}\d{4}$"), F.to_date(u2, "ddMMMyyyy"))
    d2r = F.when(s2.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s2, "yyyy-MM-dd"))
    d2 = F.when(
        d2r.isNotNull() & (F.year(d2r) == 9999), F.to_date(F.lit("2099-01-01"))
    ).otherwise(d2r)
    d3 = F.when(
        u2.rlike(r"^\d{2}[A-Z]{3}\d{2}$") & ~u2.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(
            F.concat(F.substring(u2, 1, 5), F.lit("20"), F.substring(u2, 6, 2)),
            "ddMMMyyyy",
        ),
    )
    d4 = F.when(
        u2.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(F.concat(F.substring(u2, 1, 5), F.lit("2099")), "ddMMMyyyy"),
    )
    d5 = F.when(s2.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(s2, "dd/MM/yyyy"))
    d6 = F.when(
        s2.rlike(r"^\d{2}/\d{2}/\d{2}$") & ~s2.rlike(r"^\d{2}/\d{2}/99$"),
        F.to_date(
            F.concat(F.substring(s2, 1, 6), F.lit("20"), F.substring(s2, 7, 2)),
            "dd/MM/yyyy",
        ),
    )
    d7 = F.when(
        s2.rlike(r"^\d{2}/\d{2}/99$"),
        F.to_date(F.concat(F.substring(s2, 1, 6), F.lit("2099")), "dd/MM/yyyy"),
    )
    ts0 = F.when(
        u2.rlike(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$"),
        F.to_timestamp(u2, "ddMMMyyyy:HH:mm:ss"),
    )
    ts3r = F.when(
        s2.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
        F.to_timestamp(s2, "yyyy-MM-dd HH:mm:ss"),
    )
    ts3 = F.when(
        ts3r.isNotNull() & (F.year(ts3r) == 9999),
        F.to_timestamp(
            F.concat(F.lit("2099"), F.date_format(ts3r, "-MM-dd HH:mm:ss")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(ts3r)
    return (
        F.when(ts3.isNotNull(), F.date_format(ts3, "yyyy-MM-dd HH:mm:ss"))
        .when(ts0.isNotNull(), F.date_format(ts0, "yyyy-MM-dd HH:mm:ss"))
        .when(d5.isNotNull(), F.date_format(d5, "yyyy-MM-dd"))
        .when(d7.isNotNull(), F.date_format(d7, "yyyy-MM-dd"))
        .when(d6.isNotNull(), F.date_format(d6, "yyyy-MM-dd"))
        .when(d4.isNotNull(), F.date_format(d4, "yyyy-MM-dd"))
        .when(d3.isNotNull(), F.date_format(d3, "yyyy-MM-dd"))
        .when(d1.isNotNull(), F.date_format(d1, "yyyy-MM-dd"))
        .when(d2.isNotNull(), F.date_format(d2, "yyyy-MM-dd"))
        .otherwise(s2)
    )


def normalize_numeric_string(s: Column, scale: Optional[int]) -> Column:
    is_num = s.rlike(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$") | s.rlike(
        r"^[+-]?\d+[eE][+-]?\d+$"
    )
    dec = (
        s.cast(f"decimal(38,{int(scale)})")
        if scale is not None and int(scale) >= 0
        else s.cast("decimal(38,18)")
    )
    ds = dec.cast("string")
    ds = F.regexp_replace(ds, r"(\.\d*?)0+$", r"\1")
    ds = F.regexp_replace(ds, r"\.$", "")
    ds = F.when(ds == "-0", F.lit("0")).otherwise(ds)
    ds = F.when(ds.isNull(), s).otherwise(ds)
    return F.when(is_num, ds).otherwise(s)


def normalize_value_auto(col: Column, scale: Optional[int]) -> Column:
    return normalize_numeric_string(normalize_value_auto_base(col), scale)


def raw_trim(col: Column) -> Column:
    return F.trim(F.coalesce(col.cast("string"), F.lit("")))


def derive_scale_from_redshift(
    df: DataFrame, cols: List[str], max_scale_cap: int = 18
) -> Dict[str, int]:
    exprs = []
    for c in cols:
        s = F.trim(F.coalesce(F.col(c).cast("string"), F.lit("")))
        frac = F.regexp_extract(s, r"^[+-]?\d+\.(\d+)$", 1)
        exprs.append(F.greatest(F.max(F.length(frac)), F.lit(0)).alias(c))
    if not exprs:
        return {}
    row = df.agg(*exprs).collect()[0]
    out = {}
    for c in cols:
        try:
            out[c] = min(
                max(int(row[c]) if row[c] is not None else 0, 0), int(max_scale_cap)
            )
        except Exception:
            out[c] = 0
    return out

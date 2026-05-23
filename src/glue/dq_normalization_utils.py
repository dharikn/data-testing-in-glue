"""Column normalisation helpers for S3 and Redshift values."""

from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def canon_header(name: str) -> str:
    """Return canonical column name used by the DQ engine."""
    text = (name or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def normalize_headers(df: DataFrame) -> DataFrame:
    """Canonicalise Spark DataFrame headers safely."""
    seen = {}
    final_names = []

    for col_name in df.columns:
        canon = canon_header(col_name)
        seen[canon] = seen.get(canon, 0) + 1
        if seen[canon] == 1:
            final_names.append(canon)
        else:
            final_names.append(f"{canon}_{seen[canon]}")

    out = df
    for old_name, new_name in zip(df.columns, final_names):
        if old_name != new_name:
            out = out.withColumnRenamed(old_name, new_name)
    return out


def _base_string(col: Column) -> Column:
    """Apply common trim, null, and whitespace cleanup."""
    text = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    return F.regexp_replace(text, r"\s+", " ")


def normalize_pk_auto(col: Column) -> Column:
    """Normalise PK/date/timestamp-like values.

    This keeps the existing business behaviour for common date and
    timestamp formats, including high-date handling used in the
    current DQ implementation.
    """
    text = _base_string(col)
    upper_text = F.upper(text)

    ts_ddmonyyyy = F.when(
        upper_text.rlike(
            r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$"
        ),
        F.to_timestamp(upper_text, "ddMMMyyyy:HH:mm:ss"),
    )

    ts_ddmonyy = F.when(
        upper_text.rlike(
            r"^\d{2}[A-Z]{3}\d{2}:\d{2}:\d{2}:\d{2}$"
        )
        & ~upper_text.rlike(
            r"^\d{2}[A-Z]{3}99:\d{2}:\d{2}:\d{2}$"
        ),
        F.to_timestamp(
            F.concat(
                F.substring(upper_text, 1, 5),
                F.lit("20"),
                F.substring(upper_text, 6, 2),
                F.substring(upper_text, 8, 9),
            ),
            "ddMMMyyyy:HH:mm:ss",
        ),
    )

    ts_high_99 = F.when(
        upper_text.rlike(
            r"^\d{2}[A-Z]{3}99:\d{2}:\d{2}:\d{2}$"
        ),
        F.to_timestamp(
            F.concat(
                F.substring(upper_text, 1, 5),
                F.lit("2099"),
                F.substring(upper_text, 8, 9),
            ),
            "ddMMMyyyy:HH:mm:ss",
        ),
    )

    ts_iso_raw = F.when(
        text.rlike(
            r"^\d{4}-\d{2}-\d{2} "
            r"\d{2}:\d{2}:\d{2}(\.\d+)?$"
        ),
        F.to_timestamp(text),
    )

    ts_iso = F.when(
        ts_iso_raw.isNotNull() & (F.year(ts_iso_raw) == 9999),
        F.to_timestamp(
            F.concat(
                F.lit("2099"),
                F.date_format(ts_iso_raw, "-MM-dd HH:mm:ss"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(ts_iso_raw)

    d_ddmonyyyy = F.when(
        upper_text.rlike(r"^\d{2}[A-Z]{3}\d{4}$"),
        F.to_date(upper_text, "ddMMMyyyy"),
    )

    d_yyyy_raw = F.when(
        text.rlike(r"^\d{4}-\d{2}-\d{2}$"),
        F.to_date(text, "yyyy-MM-dd"),
    )

    d_yyyy = F.when(
        d_yyyy_raw.isNotNull() & (F.year(d_yyyy_raw) == 9999),
        F.to_date(F.lit("2099-01-01")),
    ).otherwise(d_yyyy_raw)

    d_ddmonyy = F.when(
        upper_text.rlike(r"^\d{2}[A-Z]{3}\d{2}$")
        & ~upper_text.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(
            F.concat(
                F.substring(upper_text, 1, 5),
                F.lit("20"),
                F.substring(upper_text, 6, 2),
            ),
            "ddMMMyyyy",
        ),
    )

    d_high_99 = F.when(
        upper_text.rlike(r"^\d{2}[A-Z]{3}99$"),
        F.to_date(
            F.concat(
                F.substring(upper_text, 1, 5),
                F.lit("2099"),
            ),
            "ddMMMyyyy",
        ),
    )

    return (
        F.when(
            ts_iso.isNotNull(),
            F.date_format(ts_iso, "yyyy-MM-dd HH:mm:ss"),
        )
        .when(
            ts_ddmonyyyy.isNotNull(),
            F.date_format(ts_ddmonyyyy, "yyyy-MM-dd HH:mm:ss"),
        )
        .when(
            ts_high_99.isNotNull(),
            F.date_format(ts_high_99, "yyyy-MM-dd HH:mm:ss"),
        )
        .when(
            ts_ddmonyy.isNotNull(),
            F.date_format(ts_ddmonyy, "yyyy-MM-dd HH:mm:ss"),
        )
        .when(
            d_ddmonyyyy.isNotNull(),
            F.date_format(d_ddmonyyyy, "yyyy-MM-dd"),
        )
        .when(
            d_high_99.isNotNull(),
            F.date_format(d_high_99, "yyyy-MM-dd"),
        )
        .when(
            d_ddmonyy.isNotNull(),
            F.date_format(d_ddmonyy, "yyyy-MM-dd"),
        )
        .when(d_yyyy.isNotNull(), F.date_format(d_yyyy, "yyyy-MM-dd"))
        .otherwise(text)
    )


def normalize_value_auto_base(col: Column) -> Column:
    """Normalise non-numeric textual/date values."""
    text = _base_string(col)
    upper_text = F.upper(text)
    cleaned = F.when(
        upper_text.isin("NULL", "NAN", "NONE"),
        F.lit(""),
    ).otherwise(text)
    return normalize_pk_auto(cleaned)


def normalize_numeric_string(
    text: Column,
    scale: Optional[int],
) -> Column:
    """Canonicalise decimal/scientific numeric text."""
    numeric_regex = r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"
    integer_exp_regex = r"^[+-]?\d+[eE][+-]?\d+$"
    is_numeric = text.rlike(numeric_regex) | text.rlike(
        integer_exp_regex
    )

    if scale is not None and int(scale) >= 0:
        decimal_value = text.cast(f"decimal(38,{int(scale)})")
    else:
        decimal_value = text.cast("decimal(38,18)")

    canonical = decimal_value.cast("string")
    canonical = F.regexp_replace(canonical, r"(\.\d*?)0+$", r"\1")
    canonical = F.regexp_replace(canonical, r"\.$", "")
    canonical = F.when(
        canonical == "-0",
        F.lit("0"),
    ).otherwise(canonical)
    canonical = F.when(canonical.isNull(), text).otherwise(canonical)

    return F.when(is_numeric, canonical).otherwise(text)


def normalize_value_auto(
    col: Column,
    scale: Optional[int],
) -> Column:
    """Normalise a business value using date and numeric rules."""
    base_value = normalize_value_auto_base(col)
    return normalize_numeric_string(base_value, scale)


def raw_trim(col: Column) -> Column:
    """Return raw string value used only for evidence samples."""
    return F.trim(F.coalesce(col.cast("string"), F.lit("")))

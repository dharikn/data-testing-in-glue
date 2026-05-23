"""Builders for internal Parquet hash-stage DataFrames."""

from __future__ import annotations

from typing import Dict, List, Set

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_mask_utils import build_pk_mask_cols
from dq_normalization_utils import (
    normalize_pk_auto,
    normalize_value_auto,
    raw_trim,
)


def build_hash_stage_dataset(
    df: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
    scale_by_col: Dict[str, int],
    pii_set: Set[str],
) -> DataFrame:
    """Create PK hash, row hash, and per-column hashes.

    This is the key performance layer.  Later reconciliation compares
    fixed-width hashes instead of full normalised business values.
    """
    pk_norm_cols = [f"pk_n_{col}" for col in pk_cols]
    pk_norm_pairs = list(zip(pk_norm_cols, pk_cols))
    expressions = []

    # Normalised PK columns are used for all joins downstream.
    for col_name in pk_cols:
        expressions.append(
            normalize_pk_auto(F.col(col_name)).alias(
                f"pk_n_{col_name}"
            )
        )

    # Business columns are stored only as hashes in the hash stage.
    for col_name in compare_cols:
        norm_expr = normalize_value_auto(
            F.col(col_name),
            scale_by_col.get(col_name),
        )
        expressions.append(
            F.sha2(F.coalesce(norm_expr, F.lit("∅")), 256)
            .alias(f"h_{col_name}")
        )

    out = df.select(*expressions)

    pk_exprs = [
        F.coalesce(F.col(col), F.lit("")).cast("string")
        for col in pk_norm_cols
    ]
    col_hash_exprs = [
        F.coalesce(F.col(f"h_{col}"), F.lit("∅"))
        for col in compare_cols
    ]

    return (
        out.withColumn(
            "pk_hash",
            F.sha2(F.concat_ws("||", *pk_exprs), 256),
        )
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *col_hash_exprs), 256),
        )
        .withColumn(
            "pk_mask",
            F.concat_ws(
                ",",
                *build_pk_mask_cols(pk_norm_pairs, pii_set),
            ),
        )
    )


def build_raw_stage_dataset(
    df: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
) -> DataFrame:
    """Create raw-value stage used only for evidence hydration."""
    expressions = []

    for col_name in pk_cols:
        expressions.append(
            normalize_pk_auto(F.col(col_name)).alias(
                f"pk_n_{col_name}"
            )
        )

    for col_name in compare_cols:
        expressions.append(
            raw_trim(F.col(col_name)).alias(f"raw_{col_name}")
        )

    return df.select(*expressions)

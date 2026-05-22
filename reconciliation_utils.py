from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hash_utils import add_hashes
from mask_utils import build_pk_mask_cols
from normalization_utils import (normalize_pk_auto, normalize_value_auto,
                                  raw_trim)
from spark_utils import checkpoint_if_enabled, chunks


def build_normalized_dataset(
    df: DataFrame,
    pk_cols: list[str],
    compare_cols: list[str],
    scale_by_col: dict[str, int],
    source_alias: str,
):
    pk_norm_cols = [f"pk_n_{c}" for c in pk_cols]
    norm_cols = [f"n_{c}" for c in compare_cols]
    hash_cols = [f"h_{c}" for c in compare_cols]

    exprs = []

    for c in pk_cols:
        exprs.append(normalize_pk_auto(F.col(c)).alias(f"pk_n_{c}"))

    for c in compare_cols:
        n_expr = normalize_value_auto(F.col(c), scale_by_col.get(c))
        exprs.append(n_expr.alias(f"n_{c}"))
        exprs.append(F.sha2(F.coalesce(n_expr, F.lit("∅")), 256).alias(f"h_{c}"))

    out = df.select(*exprs)

    pk_exprs = [F.coalesce(F.col(c), F.lit("")).cast("string") for c in pk_norm_cols]
    row_hash_exprs = [F.coalesce(F.col(c), F.lit("∅")).cast("string") for c in hash_cols]

    out = (
        out.withColumn("pk_hash", F.sha2(F.concat_ws("||", *pk_exprs), 256))
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *row_hash_exprs), 256))
    )

    return out, pk_norm_cols, norm_cols

def build_raw_dataset(
    df: DataFrame, pk_cols: List[str], compare_cols: List[str], source_alias: str
) -> DataFrame:
    exprs = [normalize_pk_auto(F.col(c)).alias(f"pk_n_{c}") for c in pk_cols] + [
        raw_trim(F.col(c)).alias(f"raw_{c}") for c in compare_cols
    ]
    return df.select(*exprs)


def build_key_hash_df(
    df_n: DataFrame,
    pk_norm_cols: List[str],
    pk_norm_pairs: List[Tuple[str, str]],
    pii_set: Set[str],
    side: str,
) -> DataFrame:
    return df_n.select(
        F.col("pk_hash"),
        *[F.col(c) for c in pk_norm_cols],
        F.concat_ws(",", *build_pk_mask_cols(pk_norm_pairs, pii_set)).alias("pk_mask"),
        F.col("row_hash").alias(f"{side}_row_hash"),
    )


def reconcile_keys(s3_df, rs_df, pk_cols, join_repartition):


    s3_hash_col = "row_hash" if "row_hash" in s3_df.columns else "s3_row_hash"
    rs_hash_col = "row_hash" if "row_hash" in rs_df.columns else "rs_row_hash"

    s3_n = (
        s3_df.select(
            *pk_cols,
            "pk_hash",
            F.col(s3_hash_col).alias("row_hash"),
            "pk_mask",
        )
        .repartition(join_repartition, *pk_cols)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_n = (
        rs_df.select(
            *pk_cols,
            "pk_hash",
            F.col(rs_hash_col).alias("row_hash"),
            "pk_mask",
        )
        .repartition(join_repartition, *pk_cols)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_keys = rs_n.select(*pk_cols).distinct()
    s3_keys = s3_n.select(*pk_cols).distinct()

    s3_only_df = (
        s3_n.join(rs_keys, on=pk_cols, how="left_anti")
        .select(*pk_cols, "pk_hash", "row_hash", "pk_mask")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rs_only_df = (
        rs_n.join(s3_keys, on=pk_cols, how="left_anti")
        .select(*pk_cols, "pk_hash", "row_hash", "pk_mask")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    matched_df = (
        s3_n.alias("s")
        .join(rs_n.alias("r"), on=pk_cols, how="inner")
        .select(
            *[F.col(c) for c in pk_cols],
            F.col("s.pk_hash").alias("pk_hash"),
            F.col("s.row_hash").alias("row_hash"),
            F.col("s.row_hash").alias("s3_row_hash"),
            F.col("r.row_hash").alias("rs_row_hash"),
            F.col("s.pk_mask").alias("pk_mask"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    hash_mismatch_df = (
        matched_df.filter(F.col("s3_row_hash") != F.col("rs_row_hash"))
        .select(*pk_cols, "pk_hash", "row_hash", "pk_mask")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    return {
        "s3_only": s3_only_df,
        "rs_only": rs_only_df,
        "matched": matched_df,
        "hash_mismatch": hash_mismatch_df,
    }

def duplicate_pk_df(df_n: DataFrame, pk_norm_cols: List[str]) -> DataFrame:
    return df_n.groupBy(*pk_norm_cols).count().where(F.col("count") > 1)


def build_mismatch_join_for_columns(
    b_norm: DataFrame,
    r_norm: DataFrame,
    mismatched_pk: DataFrame,
    pk_norm_cols: list[str],
    compare_cols: list[str],
    join_repartition: int,
    use_pairing: bool,
) -> DataFrame:
    hash_cols = [f"h_{c}" for c in compare_cols]

    b_base = (
        b_norm.select(*pk_norm_cols, *hash_cols, "row_hash")
        .join(mismatched_pk.select(*pk_norm_cols).distinct(), on=pk_norm_cols, how="inner")
    )

    r_base = (
        r_norm.select(*pk_norm_cols, *hash_cols, "row_hash")
        .join(mismatched_pk.select(*pk_norm_cols).distinct(), on=pk_norm_cols, how="inner")
    )

    if use_pairing:
        w = Window.partitionBy(*pk_norm_cols).orderBy(F.col("row_hash"))
        b_base = b_base.withColumn("_rn", F.row_number().over(w))
        r_base = r_base.withColumn("_rn", F.row_number().over(w))
        join_cols = pk_norm_cols + ["_rn"]
    else:
        join_cols = pk_norm_cols

    b_sel = b_base.select(
        *join_cols,
        *[F.col(f"h_{c}").alias(f"b_h_{c}") for c in compare_cols],
    )

    r_sel = r_base.select(
        *join_cols,
        *[F.col(f"h_{c}").alias(f"r_h_{c}") for c in compare_cols],
    )

    jr = (
        b_sel.repartition(int(join_repartition), *[F.col(c) for c in pk_norm_cols])
        .join(
            r_sel.repartition(int(join_repartition), *[F.col(c) for c in pk_norm_cols]),
            on=join_cols,
            how="inner",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    _ = jr.count()
    return jr

def compute_column_mismatch_counts(
    jr: DataFrame,
    compare_cols: list[str],
    batch_size: int,
):
    spark = jr.sparkSession
    rows = []
    failed = []

    for batch_cols in chunks(compare_cols, int(batch_size)):
        agg_exprs = []

        for c in batch_cols:
            diff_expr = ~F.col(f"b_h_{c}").eqNullSafe(F.col(f"r_h_{c}"))
            agg_exprs.append(
                F.sum(F.when(diff_expr, F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias(c)
            )

        batch_row = jr.agg(*agg_exprs).collect()[0].asDict()

        for c in batch_cols:
            mismatch_count = int(batch_row.get(c) or 0)
            rows.append({"column_name": c, "mismatch_count": mismatch_count})
            if mismatch_count > 0:
                failed.append(c)

    return spark.createDataFrame(rows, "column_name string, mismatch_count long"), rows, failed

def build_raw_mismatch_join_for_evidence(
    b_raw: DataFrame,
    r_raw: DataFrame,
    jr_norm: DataFrame,
    failed_cols: list[str],
    pk_norm_cols: list[str],
    pk_norm_pairs,
    pii_set,
    join_repartition: int,
) -> DataFrame:
    if not failed_cols:
        return jr_norm

    hash_cols = []
    for c in failed_cols:
        hash_cols.extend([f"b_h_{c}", f"r_h_{c}"])

    raw_cols = pk_norm_cols + [f"raw_{c}" for c in failed_cols]

    b_raw_s = b_raw.select(*raw_cols).select(
        *pk_norm_cols,
        *[F.col(f"raw_{c}").alias(f"b_raw_{c}") for c in failed_cols],
    )

    r_raw_s = r_raw.select(*raw_cols).select(
        *pk_norm_cols,
        *[F.col(f"raw_{c}").alias(f"r_raw_{c}") for c in failed_cols],
    )

    base = jr_norm.select(*pk_norm_cols, *hash_cols)

    ev = (
        base.repartition(int(join_repartition), *[F.col(c) for c in pk_norm_cols])
        .join(
            b_raw_s.repartition(int(join_repartition), *[F.col(c) for c in pk_norm_cols]),
            on=pk_norm_cols,
            how="inner",
        )
        .join(
            r_raw_s.repartition(int(join_repartition), *[F.col(c) for c in pk_norm_cols]),
            on=pk_norm_cols,
            how="inner",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    _ = ev.count()
    return ev
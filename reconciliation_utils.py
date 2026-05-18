from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .hash_utils import add_hashes
from .mask_utils import build_pk_mask_cols
from .normalization_utils import (normalize_pk_auto, normalize_value_auto,
                                  raw_trim)
from .spark_utils import checkpoint_if_enabled, chunks


def build_normalized_dataset(
    df: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
    scale_by_col: Dict[str, int],
    source_alias: str,
):
    pk_norm_cols = [f"pk_n_{c}" for c in pk_cols]
    norm_cols = [f"n_{c}" for c in compare_cols]
    exprs = []
    for c in pk_cols:
        exprs.append(normalize_pk_auto(F.col(c)).alias(f"pk_n_{c}"))
    for c in compare_cols:
        exprs.append(
            normalize_value_auto(F.col(c), scale_by_col.get(c)).alias(f"n_{c}")
        )
    out = add_hashes(df.select(*exprs), pk_norm_cols, norm_cols)
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


def reconcile_keys(
    b_key: DataFrame,
    r_key: DataFrame,
    pk_norm_cols: List[str],
    join_repartition: int,
    checkpoint_enabled: bool,
) -> Dict[str, DataFrame]:
    b = b_key.repartition(int(join_repartition), F.col("pk_hash")).alias("b")
    r = r_key.repartition(int(join_repartition), F.col("pk_hash")).alias("r")
    cond = [F.col("b.pk_hash") == F.col("r.pk_hash")] + [
        F.col(f"b.{c}").eqNullSafe(F.col(f"r.{c}")) for c in pk_norm_cols
    ]
    j = (
        b.join(r, on=cond, how="full_outer")
        .select(
            F.coalesce(F.col("b.pk_hash"), F.col("r.pk_hash")).alias("pk_hash"),
            *[
                F.coalesce(F.col(f"b.{c}"), F.col(f"r.{c}")).alias(c)
                for c in pk_norm_cols
            ],
            F.coalesce(F.col("b.pk_mask"), F.col("r.pk_mask")).alias("pk_mask"),
            F.col("b.s3_row_hash").alias("s3_row_hash"),
            F.col("r.rs_row_hash").alias("rs_row_hash"),
            F.col("b.pk_hash").isNotNull().alias("s3_exists"),
            F.col("r.pk_hash").isNotNull().alias("rs_exists"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    _ = j.count()
    j = checkpoint_if_enabled(j, checkpoint_enabled, True).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    _ = j.count()
    return {
        "joined": j,
        "s3_only": j.where(F.col("s3_exists") & ~F.col("rs_exists")).select(
            "pk_hash", *pk_norm_cols, "pk_mask"
        ),
        "rs_only": j.where(~F.col("s3_exists") & F.col("rs_exists")).select(
            "pk_hash", *pk_norm_cols, "pk_mask"
        ),
        "hash_mismatch": j.where(
            F.col("s3_exists")
            & F.col("rs_exists")
            & ~F.col("s3_row_hash").eqNullSafe(F.col("rs_row_hash"))
        ).select("pk_hash", *pk_norm_cols, "pk_mask"),
    }


def duplicate_pk_df(df_n: DataFrame, pk_norm_cols: List[str]) -> DataFrame:
    return df_n.groupBy(*pk_norm_cols).count().where(F.col("count") > 1)


def build_mismatch_join_for_columns(
    b_norm: DataFrame,
    r_norm: DataFrame,
    mismatched_pk: DataFrame,
    pk_norm_cols: List[str],
    compare_cols: List[str],
    join_repartition: int,
    use_pairing: bool,
) -> DataFrame:
    selected = pk_norm_cols + [f"n_{c}" for c in compare_cols] + ["row_hash"]
    b_base = b_norm.select(*selected).join(
        mismatched_pk.select(*pk_norm_cols), on=pk_norm_cols, how="inner"
    )
    r_base = r_norm.select(*selected).join(
        mismatched_pk.select(*pk_norm_cols), on=pk_norm_cols, how="inner"
    )
    if use_pairing:
        w = Window.partitionBy(*pk_norm_cols).orderBy(F.col("row_hash"))
        b_base = b_base.withColumn("_rn", F.row_number().over(w))
        r_base = r_base.withColumn("_rn", F.row_number().over(w))
        on_cols = pk_norm_cols + ["_rn"]
    else:
        on_cols = pk_norm_cols
    b_sel = b_base.select(
        *on_cols, *[F.col(f"n_{c}").alias(f"b_n_{c}") for c in compare_cols]
    )
    r_sel = r_base.select(
        *on_cols, *[F.col(f"n_{c}").alias(f"r_n_{c}") for c in compare_cols]
    )
    jr = (
        b_sel.repartition(int(join_repartition), F.col(pk_norm_cols[0]))
        .join(
            r_sel.repartition(int(join_repartition), F.col(pk_norm_cols[0])),
            on=on_cols,
            how="inner",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    _ = jr.count()
    return jr


def compute_column_mismatch_counts(
    jr: DataFrame, compare_cols: List[str], batch_size: int
):
    rows = []
    failed = []
    spark = jr.sparkSession
    for batch_cols in chunks(compare_cols, int(batch_size)):
        exprs = []
        for c in batch_cols:
            exprs.append(
                F.sum(
                    F.when(
                        ~F.col(f"b_n_{c}").eqNullSafe(F.col(f"r_n_{c}")), F.lit(1)
                    ).otherwise(F.lit(0))
                )
                .cast("long")
                .alias(c)
            )
        br = jr.agg(*exprs).collect()[0].asDict()
        for c, v in br.items():
            mc = int(v or 0)
            rows.append({"column_name": c, "mismatch_count": mc})
            if mc > 0:
                failed.append(c)
    return (
        spark.createDataFrame(rows, "column_name string, mismatch_count long"),
        rows,
        failed,
    )


def build_raw_mismatch_join_for_evidence(
    b_raw: DataFrame,
    r_raw: DataFrame,
    jr_norm: DataFrame,
    failed_cols: List[str],
    pk_norm_cols: List[str],
    pk_norm_pairs: List[Tuple[str, str]],
    pii_set: Set[str],
    join_repartition: int,
) -> DataFrame:
    if not failed_cols:
        return jr_norm
    need = []
    for c in failed_cols:
        need += [f"b_n_{c}", f"r_n_{c}"]
    raw_cols = pk_norm_cols + [f"raw_{c}" for c in failed_cols]
    b = b_raw.select(*raw_cols).select(
        *pk_norm_cols, *[F.col(f"raw_{c}").alias(f"b_raw_{c}") for c in failed_cols]
    )
    r = r_raw.select(*raw_cols).select(
        *pk_norm_cols, *[F.col(f"raw_{c}").alias(f"r_raw_{c}") for c in failed_cols]
    )
    ev = (
        jr_norm.select(*pk_norm_cols, *need)
        .repartition(int(join_repartition), F.col(pk_norm_cols[0]))
        .join(
            b.repartition(int(join_repartition), F.col(pk_norm_cols[0])),
            on=pk_norm_cols,
            how="inner",
        )
        .join(
            r.repartition(int(join_repartition), F.col(pk_norm_cols[0])),
            on=pk_norm_cols,
            how="inner",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    _ = ev.count()
    return ev

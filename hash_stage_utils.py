from typing import Dict, List, Set, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mask_utils import build_pk_mask_cols
from normalization_utils import normalize_pk_auto, normalize_value_auto, raw_trim


def build_hash_stage_dataset(
    df: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
    scale_by_col: Dict[str, int],
    pii_set: Set[str],
) -> DataFrame:
    pk_norm_cols = [f"pk_n_{c}" for c in pk_cols]
    pk_norm_pairs = list(zip(pk_norm_cols, pk_cols))
    exprs = []

    for c in pk_cols:
        exprs.append(normalize_pk_auto(F.col(c)).alias(f"pk_n_{c}"))

    for c in compare_cols:
        n_expr = normalize_value_auto(F.col(c), scale_by_col.get(c))
        exprs.append(F.sha2(F.coalesce(n_expr, F.lit("∅")), 256).alias(f"h_{c}"))

    out = df.select(*exprs)
    pk_exprs = [F.coalesce(F.col(c), F.lit("")).cast("string") for c in pk_norm_cols]
    col_hash_exprs = [F.coalesce(F.col(f"h_{c}"), F.lit("∅")) for c in compare_cols]

    return (
        out.withColumn("pk_hash", F.sha2(F.concat_ws("||", *pk_exprs), 256))
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *col_hash_exprs), 256))
        .withColumn("pk_mask", F.concat_ws(",", *build_pk_mask_cols(pk_norm_pairs, pii_set)))
    )


def build_raw_stage_dataset(df: DataFrame, pk_cols: List[str], compare_cols: List[str]) -> DataFrame:
    exprs = []
    for c in pk_cols:
        exprs.append(normalize_pk_auto(F.col(c)).alias(f"pk_n_{c}"))
    for c in compare_cols:
        exprs.append(raw_trim(F.col(c)).alias(f"raw_{c}"))
    return df.select(*exprs)

from __future__ import annotations

from typing import List, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .mask_utils import build_pk_mask_cols, get_mask_udf
from .s3_io import spark_write_csv
from .spark_utils import chunks


def write_empty_csv(spark: SparkSession, path: str, schema: str) -> None:
    spark_write_csv(spark.createDataFrame([], schema), path, 1)


def write_dup_evidence(
    df: DataFrame,
    path: str,
    pk_norm_pairs: List[Tuple[str, str]],
    pii_set: Set[str],
    limit: int,
) -> None:
    ev = (
        df.withColumn(
            "pk_mask", F.concat_ws(",", *build_pk_mask_cols(pk_norm_pairs, pii_set))
        )
        .select("pk_mask", "count")
        .orderBy(F.col("count").desc())
    )
    spark_write_csv(ev, path, limit)


def write_column_counts(df: DataFrame, path: str) -> None:
    spark_write_csv(df, path, 1000000)


def build_value_samples_for_failed_columns(
    jr: DataFrame,
    failed_columns: List[str],
    pk_norm_pairs: List[Tuple[str, str]],
    pii_set: Set[str],
    per_column_limit: int,
    batch_size: int,
) -> DataFrame:
    spark = jr.sparkSession
    pk_mask_expr = F.concat_ws(",", *build_pk_mask_cols(pk_norm_pairs, pii_set))
    mask_udf = get_mask_udf()
    dfs = []
    for batch_cols in chunks(failed_columns, batch_size):
        structs = []
        for c in batch_cols:
            diff = ~F.col(f"b_n_{c}").eqNullSafe(F.col(f"r_n_{c}"))
            structs.append(
                F.when(
                    diff,
                    F.struct(
                        F.lit(c).alias("column_name"),
                        F.col(f"b_raw_{c}").alias("s3_value_raw"),
                        F.col(f"r_raw_{c}").alias("redshift_value_raw"),
                    ),
                )
            )
        arr = F.filter(F.array(*structs), lambda x: x.isNotNull())
        dfs.append(
            jr.select(pk_mask_expr.alias("pk_mask"), F.explode(arr).alias("d"))
            .select(
                "pk_mask",
                F.col("d.column_name").alias("column_name"),
                F.col("d.s3_value_raw").alias("s3_value_raw"),
                F.col("d.redshift_value_raw").alias("redshift_value_raw"),
            )
            .limit(int(per_column_limit) * max(len(batch_cols), 1))
        )
    if not dfs:
        return spark.createDataFrame(
            [],
            "pk_mask string, column_name string, s3_value string, redshift_value string",
        )
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df)
    pii_list = sorted(list(pii_set))
    is_pii = (
        F.array_contains(F.array(*[F.lit(x) for x in pii_list]), F.col("column_name"))
        if pii_list
        else F.lit(False)
    )
    s3v = F.coalesce(F.col("s3_value_raw").cast("string"), F.lit(""))
    rsv = F.coalesce(F.col("redshift_value_raw").cast("string"), F.lit(""))
    return out.select(
        "pk_mask",
        "column_name",
        F.when(is_pii, mask_udf(s3v)).otherwise(s3v).alias("s3_value"),
        F.when(is_pii, mask_udf(rsv)).otherwise(rsv).alias("redshift_value"),
    )

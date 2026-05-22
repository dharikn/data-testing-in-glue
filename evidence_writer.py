from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_value_samples_for_failed_columns(
    jr: DataFrame,
    failed_columns: List[str],
    pk_norm_pairs,
    pii_set,
    per_column_limit: int,
    batch_size: int,
) -> DataFrame:
    # Kept for compatibility with earlier imports. The two-job reconcile script builds evidence directly.
    spark = jr.sparkSession
    return spark.createDataFrame([], "pk_mask string, column_name string, s3_value string, redshift_value string")

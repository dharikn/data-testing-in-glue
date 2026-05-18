from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_hashes(df: DataFrame, pk_norm_cols: List[str], norm_cols: List[str]) -> DataFrame:
    pk_exprs=[F.coalesce(F.col(c),F.lit('')).cast('string') for c in pk_norm_cols]
    row_exprs=[F.coalesce(F.col(c),F.lit('∅')).cast('string') for c in norm_cols]
    return df.withColumn('pk_hash',F.sha2(F.concat_ws('||',*pk_exprs),256)).withColumn('row_hash',F.sha2(F.concat_ws('||',*row_exprs),256))

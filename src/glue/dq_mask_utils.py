"""PII-safe masking utilities for evidence output."""

from __future__ import annotations

from typing import List, Optional, Set, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _progressive_mask_py(value: Optional[str]) -> str:
    """Mask the first half of a string value."""
    if value is None:
        return ""
    text = str(value)
    if not text:
        return ""
    cut = (len(text) + 1) // 2
    return ("*" * cut) + text[cut:]


def get_mask_udf():
    """Return Spark UDF for evidence-only masking."""
    return F.udf(_progressive_mask_py, StringType())


def build_pk_mask_cols(
    pk_norm_pairs: List[Tuple[str, str]],
    pii_set: Set[str],
) -> List[Column]:
    """Build masked/unmasked PK columns for evidence keys."""
    mask_udf = get_mask_udf()
    cols: List[Column] = []

    for norm_col, base_col in pk_norm_pairs:
        value = F.coalesce(F.col(norm_col).cast("string"), F.lit(""))
        if base_col in pii_set:
            cols.append(mask_udf(value))
        else:
            cols.append(value)

    return cols

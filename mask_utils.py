from __future__ import annotations
from typing import List, Optional, Set, Tuple
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def _progressive_mask_py(val: Optional[str]) -> str:
    if val is None: return ''
    s=str(val)
    if s=='': return ''
    k=(len(s)+1)//2
    return ('*'*k)+s[k:]

def get_mask_udf(): return F.udf(_progressive_mask_py, StringType())

def build_pk_mask_cols(pk_norm_pairs: List[Tuple[str,str]], pii_set: Set[str]) -> List[Column]:
    mask_udf=get_mask_udf(); out=[]
    for norm_col, base_col in pk_norm_pairs:
        v=F.coalesce(F.col(norm_col).cast('string'), F.lit(''))
        out.append(mask_udf(v) if base_col in pii_set else v)
    return out

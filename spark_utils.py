from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any, Generator, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def now_utc_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def run_ts_folder_utc() -> str:
    return datetime.now(timezone.utc).strftime("%d%m%Y_%H%M%S")


def chunks(items: List[Any], size: int) -> Generator[List[Any], None, None]:
    size = max(int(size or 1), 1)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def configure_spark(
    spark: SparkSession, join_repartition: int, checkpoint_dir: Optional[str] = None
) -> None:
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(int(join_repartition or 150)))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    if checkpoint_dir:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)


def safe_unpersist(*dfs: DataFrame) -> None:
    for df in dfs:
        try:
            if df is not None:
                df.unpersist()
        except Exception:
            pass


def checkpoint_if_enabled(
    df: DataFrame, enabled: bool, eager: bool = True
) -> DataFrame:
    return df.checkpoint(eager=eager) if enabled else df

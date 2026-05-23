"""Common Spark and logging utilities for Glue DQ jobs."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any, Generator, List

from pyspark.sql import DataFrame, SparkSession


def get_logger(name: str) -> logging.Logger:
    """Create a stdout logger suitable for Glue CloudWatch logs."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def run_ts_folder_utc() -> str:
    """Return timestamp string used in S3 run folders."""
    return datetime.now(timezone.utc).strftime("%d%m%Y_%H%M%S")


def chunks(
    items: List[Any],
    size: int,
) -> Generator[List[Any], None, None]:
    """Yield fixed-size chunks from a list."""
    size = max(int(size or 1), 1)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def configure_spark(
    spark: SparkSession,
    join_repartition: int,
) -> None:
    """Apply Spark settings used by reconciliation workloads."""
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(int(join_repartition or 300)),
    )
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true",
    )
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def safe_unpersist(*dfs: DataFrame) -> None:
    """Best-effort unpersist for cached DataFrames."""
    for df in dfs:
        try:
            if df is not None:
                df.unpersist(blocking=False)
        except Exception:
            # Cleanup must never mask the real job result.
            pass

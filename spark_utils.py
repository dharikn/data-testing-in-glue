import logging
import sys
from datetime import datetime, timezone
from typing import Any, Generator, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_ts_folder_utc() -> str:
    return datetime.now(timezone.utc).strftime("%d%m%Y_%H%M%S")


def chunks(items: List[Any], size: int) -> Generator[List[Any], None, None]:
    size = max(int(size or 1), 1)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def configure_spark(spark: SparkSession, join_repartition: int) -> None:
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(int(join_repartition or 300)))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def safe_unpersist(*dfs: DataFrame) -> None:
    for df in dfs:
        try:
            if df is not None:
                df.unpersist(blocking=False)
        except Exception:
            pass


def persist_count(df: DataFrame, name: str, logger: logging.Logger) -> DataFrame:
    logger.info(f"Persist/materialise: {name}")
    out = df.persist(StorageLevel.MEMORY_AND_DISK)
    _ = out.count()
    logger.info(f"Done materialising: {name}")
    return out

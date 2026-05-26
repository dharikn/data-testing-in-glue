import re

from dq_spark_utils import (
    chunks,
    configure_spark,
    get_logger,
    now_utc_iso,
    run_ts_folder_utc,
    safe_unpersist,
)


class FakeConf:
    def __init__(self):
        self.values = {}

    def set(self, key, value):
        self.values[key] = value


class FakeSparkContext:
    def __init__(self):
        self.level = None

    def setLogLevel(self, level):
        self.level = level


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()
        self.conf = FakeConf()


class FakeDataFrame:
    def __init__(self, fail=False):
        self.fail = fail
        self.unpersisted = False

    def unpersist(self, blocking=False):
        if self.fail:
            raise RuntimeError("ignore")
        self.unpersisted = True
        self.blocking = blocking


def test_get_logger_reuses_named_logger():
    logger1 = get_logger("unit-test-logger")
    logger2 = get_logger("unit-test-logger")
    assert logger1 is logger2
    assert logger1.handlers


def test_now_utc_iso_format():
    value = now_utc_iso()
    assert value.endswith("Z")
    assert "T" in value


def test_run_ts_folder_utc_format():
    value = run_ts_folder_utc()
    assert re.match(r"^\d{8}_\d{6}$", value)


def test_chunks_splits_items():
    assert list(chunks([1, 2, 3, 4, 5], 2)) == [
        [1, 2], [3, 4], [5]
    ]


def test_chunks_handles_zero_size():
    assert list(chunks([1, 2], 0)) == [[1], [2]]


def test_configure_spark_sets_expected_options():
    spark = FakeSpark()
    configure_spark(spark, 300)
    assert spark.sparkContext.level == "WARN"
    assert spark.conf.values["spark.sql.shuffle.partitions"] == "300"
    assert spark.conf.values["spark.sql.adaptive.enabled"] == "true"
    assert spark.conf.values["spark.sql.autoBroadcastJoinThreshold"] == "-1"


def test_safe_unpersist_ignores_none_and_errors():
    df_ok = FakeDataFrame()
    df_fail = FakeDataFrame(fail=True)
    safe_unpersist(None, df_ok, df_fail)
    assert df_ok.unpersisted is True
    assert df_ok.blocking is False

import sys
import types
from pathlib import Path

import pytest

TEST_DIR = Path(__file__).resolve().parent
SRC_DIR = TEST_DIR.parent
GLUE_SRC = SRC_DIR / "glue"

print(f"DEBUG conftest GLUE_SRC={GLUE_SRC}")

if str(GLUE_SRC) not in sys.path:
    sys.path.insert(0, str(GLUE_SRC))

def _install_awsglue_stubs() -> None:
    """Install minimal AWS Glue stubs for local unit tests."""
    awsglue = types.ModuleType("awsglue")
    context = types.ModuleType("awsglue.context")
    job = types.ModuleType("awsglue.job")
    utils = types.ModuleType("awsglue.utils")

    class FakeGlueContext:
        def __init__(self, spark_context):
            self.spark_context = spark_context
            self.spark_session = None

    class FakeJob:
        def __init__(self, glue_context):
            self.glue_context = glue_context
            self.initialised = False
            self.committed = False

        def init(self, name, args):
            self.initialised = True
            self.name = name
            self.args = args

        def commit(self):
            self.committed = True

    def get_resolved_options(argv, names):
        return {name: f"value_for_{name}" for name in names}

    context.GlueContext = FakeGlueContext
    job.Job = FakeJob
    utils.getResolvedOptions = get_resolved_options
    sys.modules.setdefault("awsglue", awsglue)
    sys.modules.setdefault("awsglue.context", context)
    sys.modules.setdefault("awsglue.job", job)
    sys.modules.setdefault("awsglue.utils", utils)


_install_awsglue_stubs()


@pytest.fixture(scope="session")
def spark():
    """Return local Spark session for dataframe unit tests."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("glue-dq-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class FakeS3Client:
    """Small in-memory S3 client used by unit tests."""

    def __init__(self):
        self.objects = {}
        self.pages = []

    def get_object(self, Bucket, Key):
        body = self.objects[(Bucket, Key)]

        class Body:
            def read(self_inner):
                return body

        return {"Body": Body()}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body
        self.last_content_type = ContentType

    def list_objects_v2(self, **kwargs):
        if self.pages:
            return self.pages.pop(0)
        return {"Contents": [], "IsTruncated": False}


@pytest.fixture()
def fake_s3_client():
    return FakeS3Client()

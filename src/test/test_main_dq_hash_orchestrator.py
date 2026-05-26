import main_dq_hash_orchestrator as main_mod


class FakeSparkContext:
    @staticmethod
    def getOrCreate():
        return object()


class FakeGlueContext:
    def __init__(self, sc):
        self.sc = sc
        self.spark_session = object()


class FakeJob:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.committed = False

    def init(self, name, args):
        self.name = name
        self.args = args

    def commit(self):
        self.committed = True


def test_get_args(monkeypatch):
    monkeypatch.setattr(
        main_mod,
        "getResolvedOptions",
        lambda argv, names: {name: name for name in names},
    )
    args = main_mod._get_args()
    assert args["JOB_NAME"] == "JOB_NAME"


def test_main_orchestrates_services(monkeypatch):
    calls = []
    monkeypatch.setattr(
        main_mod,
        "_get_args",
        lambda: {
            "JOB_NAME": "job",
            "BOOTSTRAP_CONFIG_BUCKET": "bucket",
            "BOOTSTRAP_CONFIG_PREFIX": "prefix",
        },
    )
    monkeypatch.setattr(main_mod.boto3, "client", lambda name: object())
    monkeypatch.setattr(main_mod, "SparkContext", FakeSparkContext)
    monkeypatch.setattr(main_mod, "GlueContext", FakeGlueContext)
    monkeypatch.setattr(main_mod, "Job", FakeJob)
    monkeypatch.setattr(
        main_mod,
        "load_runtime_config_from_s3",
        lambda **kwargs: {"JOIN_REPARTITION": 300, "FAIL_JOB_ON_DQ": False},
    )
    monkeypatch.setattr(
        main_mod,
        "configure_spark",
        lambda spark, join_repartition: calls.append(("spark", join_repartition)),
    )
    monkeypatch.setattr(main_mod, "build_hash_stage", lambda **kwargs: {})
    monkeypatch.setattr(main_mod, "reconcile_hash_stage", lambda **kwargs: "FAIL")
    main_mod.main()
    assert calls == [("spark", 300)]

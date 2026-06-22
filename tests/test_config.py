from common.config import Settings


def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "host:1234")
    monkeypatch.setenv("MODEL_PATH", "models/m.joblib")
    s = Settings.from_env()
    assert s.kafka_bootstrap == "host:1234"
    assert s.model_path == "models/m.joblib"


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    s = Settings.from_env()
    assert s.kafka_bootstrap == "localhost:9092"
    assert s.iceberg_rest_uri.startswith("http")

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    iceberg_rest_uri: str
    postgres_dsn: str
    trino_host: str
    trino_port: int
    model_path: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            iceberg_rest_uri=os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
            postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://card:card@localhost:5432/card"),
            trino_host=os.getenv("TRINO_HOST", "localhost"),
            trino_port=int(os.getenv("TRINO_PORT", "8080")),
            model_path=os.getenv("MODEL_PATH", "models/fraud_model.joblib"),
        )

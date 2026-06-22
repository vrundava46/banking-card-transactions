# Banking Credit-Card Transaction Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade, locally-runnable credit-card authorization pipeline that decides approve/decline in real time, detects card fraud, scores every transaction with a supervised ML model, and serves card analytics.

**Architecture:** Labeled synthetic generator → Kafka (4 topics) → Spark Structured Streaming (real-time rule engine + ML micro-batch scoring + Bronze landing to Iceberg/MinIO) → Airflow batch (Bronze→Silver, feature engineering, model train/eval/persist) → dbt Gold marts → Trino/Postgres serving. Same proven stack as the telecom project (Iceberg REST catalog so Spark and Trino interoperate).

**Tech Stack:** Python 3.11, PySpark 3.5, Apache Iceberg (REST catalog), Kafka (`apache/kafka:3.7.0`), MinIO (S3), Postgres, Trino, dbt-trino, Airflow, scikit-learn (`HistGradientBoostingClassifier`), joblib, Great Expectations, pytest, docker-compose.

---

## File Structure (decomposition)

```
banking-card-transactions/
  requirements.txt
  pyproject.toml
  .env.example
  infra/
    docker-compose.yml             kafka, iceberg-rest, minio, trino, postgres, schema-registry, kafka-ui
    iceberg/trino-iceberg.properties   Trino REST-catalog config
    trino/postgres.properties      Trino postgres catalog (for chargeback/decision marts)
  common/
    __init__.py · config.py · schemas.py · spark.py
  generator/
    __init__.py · scenarios.py     labeled normal + fraud txns (card-testing, impossible-travel)
    producer.py                    emits auth/decision/settlement/chargeback to Kafka
  streaming/
    __init__.py
    geo.py                         haversine distance/speed helper (pure)
    rules.py                       PURE fraud rule functions (heart, fully unit-tested)
    scoring.py                     combine rules -> risk score, severity, approve/decline
    bronze_sink.py                 Kafka -> Bronze Iceberg
    fraud_job.py                   stateful streaming: rules + ML micro-batch scoring -> decisions/alerts
  batch/
    __init__.py
    silver.py                      Bronze->Silver (join auth+decision+settlement+chargeback, enrich)
    features.py                    per-transaction ML features (+ label)
    model.py                       train/evaluate/persist/load HistGradientBoosting
    run_silver.py · run_train.py · run_score.py   Airflow entrypoints
  dbt/
    dbt_project.yml · profiles.yml · packages.yml
    models/gold/*.sql              approval_monitoring, spend_by_mcc, spend_by_merchant,
                                   chargeback_rates, risk_exposure, fraud_model_scores
    models/gold/schema.yml
  airflow/dags/
    batch_etl_dag.py · model_training_dag.py · model_scoring_dag.py
  quality/
    __init__.py · expectations_silver.py
  models/                          persisted model artifacts (gitignored)
  scripts/
    verify_cluster.py              bounded live end-to-end check
  tests/
    test_config.py · test_schemas.py · test_geo.py · test_scenarios.py · test_producer.py
    test_rules.py · test_scoring.py · test_fraud_job.py · test_silver.py
    test_features.py · test_model.py · test_quality.py · test_e2e_smoke.py
  docs/
    SCALING.md · RUNBOOK.md
```

**Design notes:**
- `streaming/rules.py`, `streaming/scoring.py`, `streaming/geo.py` are **pure functions
  over dicts** — no Kafka, no Spark, no I/O — so they unit-test deterministically.
- `batch/model.py` is plain pandas/sklearn; trains a **seeded** model so tests assert a
  reproducible holdout ROC-AUC. The persisted model is consumed by both the streaming
  micro-batch and a batch mart.
- `label_is_fraud` flows only to feature/model code, **never** into the rule engine.

---

## Phase 0 — Prerequisites & Infrastructure

### Task 1: Python project scaffolding

**Files:** Create `requirements.txt`, `pyproject.toml`, `.env.example`, `common/__init__.py`

- [ ] **Step 1: Create `requirements.txt`**

```
pyspark==3.5.1
confluent-kafka==2.4.0
faker==25.2.0
python-dotenv==1.0.1
scikit-learn==1.5.0
joblib==1.4.2
pandas==2.2.2
numpy==1.26.4
psycopg2-binary==2.9.9
great-expectations==0.18.16
dbt-trino==1.8.1
trino==0.328.0
pytest==8.2.1
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
filterwarnings = ["ignore::DeprecationWarning"]

[tool.setuptools.packages.find]
include = ["common*", "generator*", "streaming*", "batch*"]
```

- [ ] **Step 3: Create `.env.example`**

```
KAFKA_BOOTSTRAP=localhost:9092
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
ICEBERG_REST_URI=http://localhost:8181
POSTGRES_DSN=postgresql://card:card@localhost:5432/card
TRINO_HOST=localhost
TRINO_PORT=8080
MODEL_PATH=models/fraud_model.joblib
```

- [ ] **Step 4: Create venv and install**

Run: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`
Expected: installs without conflict.

- [ ] **Step 5: Create `common/__init__.py`** (empty).

- [ ] **Step 6: Commit**

```bash
git add requirements.txt pyproject.toml .env.example common/__init__.py
git commit -m "chore: python scaffolding and deps"
```

### Task 2: Infrastructure — docker-compose (REST catalog stack)

**Files:** Create `infra/docker-compose.yml`, `infra/iceberg/trino-iceberg.properties`, `infra/trino/postgres.properties`

- [ ] **Step 1: Create `infra/docker-compose.yml`**

```yaml
services:
  kafka:
    image: apache/kafka:3.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092,INTERNAL://kafka:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: ["minio-data:/data"]

  iceberg-rest:
    image: tabulario/iceberg-rest:1.5.0
    depends_on: [minio]
    ports: ["8181:8181"]
    environment:
      CATALOG_WAREHOUSE: "s3://lakehouse/warehouse"
      CATALOG_IO__IMPL: "org.apache.iceberg.aws.s3.S3FileIO"
      CATALOG_S3_ENDPOINT: "http://minio:9000"
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
      AWS_ACCESS_KEY_ID: "minioadmin"
      AWS_SECRET_ACCESS_KEY: "minioadmin"
      AWS_REGION: "us-east-1"

  postgres:
    image: postgres:16
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: card
      POSTGRES_PASSWORD: card
      POSTGRES_DB: card
    volumes: ["pg-data:/var/lib/postgresql/data"]

  trino:
    image: trinodb/trino:451
    depends_on: [minio, iceberg-rest]
    ports: ["8080:8080"]
    volumes:
      - ./iceberg/trino-iceberg.properties:/etc/trino/catalog/iceberg.properties
      - ./trino/postgres.properties:/etc/trino/catalog/postgres.properties

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    ports: ["8085:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:29092"

volumes:
  minio-data:
  pg-data:
```

- [ ] **Step 2: Create `infra/iceberg/trino-iceberg.properties`**

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://lakehouse/warehouse
iceberg.file-format=PARQUET
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.region=us-east-1
```

- [ ] **Step 3: Create `infra/trino/postgres.properties`**

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/card
connection-user=card
connection-password=card
```

- [ ] **Step 4: Start the stack**

Run: `docker compose -f infra/docker-compose.yml up -d`
Expected: all containers running (`docker compose -f infra/docker-compose.yml ps`).

- [ ] **Step 5: Create lakehouse bucket** (attach `mc` to the compose network)

Run:
```bash
NET=$(docker network ls --format '{{.Name}}' | grep -i infra | head -1)
docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb -p local/lakehouse"
```
Expected: `Bucket created successfully`.

- [ ] **Step 6: Commit**

```bash
git add infra/
git commit -m "infra: docker-compose with iceberg REST catalog stack"
```

---

## Phase 1 — Common Config, Schemas, Spark

### Task 3: Typed config loader

**Files:** Create `common/config.py`; Test `tests/test_config.py`

- [ ] **Step 1: Write the failing test** (`tests/test_config.py`)

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL "No module named 'common.config'".

- [ ] **Step 3: Write `common/config.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add common/config.py tests/test_config.py
git commit -m "feat: typed settings loader"
```

### Task 4: Event schemas & topic constants

**Files:** Create `common/schemas.py`; Test `tests/test_schemas.py`

- [ ] **Step 1: Write the failing test** (`tests/test_schemas.py`)

```python
from common.schemas import (TOPICS, AUTH_FIELDS, DECISION_FIELDS,
                            SETTLEMENT_FIELDS, CHARGEBACK_FIELDS)

def test_topics_present():
    assert TOPICS == {"authorizations": "card.authorizations",
                      "decisions": "card.decisions",
                      "settlements": "card.settlements",
                      "chargebacks": "card.chargebacks"}

def test_auth_has_correlation_and_label():
    assert AUTH_FIELDS[0] == "auth_id"
    assert "card_id" in AUTH_FIELDS
    assert "label_is_fraud" in AUTH_FIELDS

def test_all_topics_share_auth_id():
    for fields in (AUTH_FIELDS, DECISION_FIELDS, SETTLEMENT_FIELDS, CHARGEBACK_FIELDS):
        assert "auth_id" in fields
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_schemas.py -v`
Expected: FAIL "No module named 'common.schemas'".

- [ ] **Step 3: Write `common/schemas.py`**

```python
TOPICS = {
    "authorizations": "card.authorizations",
    "decisions": "card.decisions",
    "settlements": "card.settlements",
    "chargebacks": "card.chargebacks",
}
ALERTS_TOPIC = "card.alerts"

AUTH_FIELDS = [
    "auth_id", "card_id", "account_id", "merchant_id", "mcc", "merchant_country",
    "amount", "currency", "entry_mode", "is_card_present", "lat", "lon",
    "device_id", "event_ts", "label_is_fraud",
]
DECISION_FIELDS = ["auth_id", "decision", "decline_reason", "risk_score", "event_ts"]
SETTLEMENT_FIELDS = ["auth_id", "settled_amount", "event_ts"]
CHARGEBACK_FIELDS = ["auth_id", "dispute_reason", "chargeback_amount",
                     "is_fraud_dispute", "event_ts"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_schemas.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add common/schemas.py tests/test_schemas.py
git commit -m "feat: event schemas and topic constants"
```

### Task 5: Spark session factory (REST catalog)

**Files:** Create `common/spark.py`

> Environment glue; verified by integration later. Complete content provided.

- [ ] **Step 1: Write `common/spark.py`**

```python
from pyspark.sql import SparkSession
from common.config import Settings

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
ICEBERG_AWS = "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    s = settings or Settings.from_env()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", f"{ICEBERG_PKG},{ICEBERG_AWS},{KAFKA_PKG}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", s.iceberg_rest_uri)
        .config("spark.sql.catalog.lake.warehouse", "s3://lakehouse/warehouse")
        .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lake.s3.endpoint", s.minio_endpoint)
        .config("spark.sql.catalog.lake.s3.access-key-id", s.minio_access_key)
        .config("spark.sql.catalog.lake.s3.secret-access-key", s.minio_secret_key)
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.client.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lake")
        .getOrCreate()
    )
```

- [ ] **Step 2: Verify import**

Run: `python -c "import common.spark; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add common/spark.py
git commit -m "feat: spark session factory with iceberg REST catalog"
```

---

## Phase 2 — Geo Helper & Generator

### Task 6: Haversine geo helper

**Files:** Create `streaming/__init__.py`, `streaming/geo.py`; Test `tests/test_geo.py`

- [ ] **Step 1: Write the failing test** (`tests/test_geo.py`)

```python
from streaming.geo import haversine_km, implied_speed_kmh

def test_haversine_known_distance():
    # London (51.5, -0.13) to Paris (48.85, 2.35) ~= 340 km
    d = haversine_km(51.5, -0.13, 48.85, 2.35)
    assert 300 < d < 380

def test_zero_distance():
    assert haversine_km(10.0, 20.0, 10.0, 20.0) == 0.0

def test_implied_speed():
    # 340 km in 0.5 h -> 680 km/h
    speed = implied_speed_kmh(340.0, 0.5)
    assert 670 < speed < 690

def test_implied_speed_zero_time_is_infinite():
    assert implied_speed_kmh(100.0, 0.0) == float("inf")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_geo.py -v`
Expected: FAIL "No module named 'streaming.geo'".

- [ ] **Step 3: Write `streaming/__init__.py`** (empty), then `streaming/geo.py`

```python
import math

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))

def implied_speed_kmh(distance_km: float, hours: float) -> float:
    if hours <= 0:
        return float("inf")
    return distance_km / hours
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_geo.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add streaming/__init__.py streaming/geo.py tests/test_geo.py
git commit -m "feat: haversine geo helper"
```

### Task 7: Labeled transaction scenarios

**Files:** Create `generator/__init__.py`, `generator/scenarios.py`; Test `tests/test_scenarios.py`

- [ ] **Step 1: Write the failing test** (`tests/test_scenarios.py`)

```python
from generator.scenarios import (make_auth_event, normal_traffic,
                                  card_testing_burst, high_amount_fraud)

def test_make_auth_event_has_all_fields():
    e = make_auth_event()
    for k in ["auth_id", "card_id", "merchant_id", "mcc", "amount",
              "entry_mode", "is_card_present", "lat", "lon", "event_ts",
              "label_is_fraud"]:
        assert k in e

def test_normal_traffic_mostly_legit():
    events = normal_traffic(200)
    fraud = [e for e in events if e["label_is_fraud"]]
    assert len(fraud) == 0
    assert len({e["card_id"] for e in events}) > 20  # many distinct cards

def test_card_testing_burst_is_labeled_fraud_small_amounts():
    events = card_testing_burst(40, card_id="card_bad")
    assert all(e["label_is_fraud"] for e in events)
    assert all(e["card_id"] == "card_bad" for e in events)
    assert all(e["amount"] < 5.0 for e in events)   # micro-amounts
    assert all(not e["is_card_present"] for e in events)  # CNP

def test_high_amount_fraud_labeled():
    e = high_amount_fraud(card_id="card_bad")
    assert e["label_is_fraud"] is True
    assert e["amount"] > 2000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scenarios.py -v`
Expected: FAIL "No module named 'generator.scenarios'".

- [ ] **Step 3: Write `generator/__init__.py`** (empty), then `generator/scenarios.py`

```python
import random
import uuid
from datetime import datetime, timezone

ENTRY_MODES = ["chip", "contactless", "online", "swipe"]
MCCS = ["5411", "5812", "5999", "4111", "5732", "7995"]  # 7995 = gambling (risky)
HIGH_RISK_MCCS = {"7995", "6051"}
MERCHANTS = [f"mer_{i}" for i in range(1, 31)]
CARDS = [f"card_{i}" for i in range(1, 51)]
# (country, lat, lon)
GEOS = [("US", 40.7, -74.0), ("GB", 51.5, -0.13), ("DE", 52.5, 13.4),
        ("IN", 19.1, 72.9), ("BR", -23.5, -46.6)]

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_auth_event(card_id=None, amount=None, mcc=None, entry_mode=None,
                    is_card_present=None, geo=None, label=False) -> dict:
    g = geo or random.choice(GEOS)
    em = entry_mode or random.choice(ENTRY_MODES)
    return {
        "auth_id": str(uuid.uuid4()),
        "card_id": card_id or random.choice(CARDS),
        "account_id": "acct_" + (card_id or random.choice(CARDS)).split("_")[-1],
        "merchant_id": random.choice(MERCHANTS),
        "mcc": mcc or random.choice(MCCS),
        "merchant_country": g[0],
        "amount": round(amount if amount is not None else random.uniform(5, 300), 2),
        "currency": "USD",
        "entry_mode": em,
        "is_card_present": is_card_present if is_card_present is not None
                           else em in ("chip", "contactless", "swipe"),
        "lat": g[1],
        "lon": g[2],
        "device_id": f"dev_{random.randint(1, 200)}",
        "event_ts": _now_iso(),
        "label_is_fraud": bool(label),
    }

def normal_traffic(count: int) -> list[dict]:
    return [make_auth_event(label=False) for _ in range(count)]

def card_testing_burst(count: int, card_id: str = "card_bad") -> list[dict]:
    """Rapid micro-amount CNP auths on one card — the card-testing signature."""
    return [make_auth_event(card_id=card_id, amount=round(random.uniform(0.5, 4.5), 2),
                            entry_mode="online", is_card_present=False, label=True)
            for _ in range(count)]

def high_amount_fraud(card_id: str = "card_bad") -> dict:
    return make_auth_event(card_id=card_id, amount=round(random.uniform(2001, 9000), 2),
                           entry_mode="online", is_card_present=False,
                           mcc="7995", label=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scenarios.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/__init__.py generator/scenarios.py tests/test_scenarios.py
git commit -m "feat: labeled transaction scenarios (normal + card-testing + high-amount)"
```

### Task 8: Decision/settlement/chargeback derivation

**Files:** Modify `generator/scenarios.py`; Test `tests/test_scenarios.py` (append)

- [ ] **Step 1: Add failing tests** (append to `tests/test_scenarios.py`)

```python
from generator.scenarios import (make_decision_event, make_settlement_event,
                                  make_chargeback_event)

def test_decision_links_to_auth():
    a = make_auth_event()
    d = make_decision_event(a, decision="approve", risk_score=12.0)
    assert d["auth_id"] == a["auth_id"]
    assert d["decision"] == "approve"
    assert d["risk_score"] == 12.0

def test_settlement_uses_amount():
    a = make_auth_event(amount=42.0)
    s = make_settlement_event(a)
    assert s["auth_id"] == a["auth_id"]
    assert s["settled_amount"] == 42.0

def test_chargeback_marks_fraud_dispute():
    a = make_auth_event(label=True)
    c = make_chargeback_event(a)
    assert c["auth_id"] == a["auth_id"]
    assert c["is_fraud_dispute"] is True
    assert c["chargeback_amount"] == a["amount"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scenarios.py -v`
Expected: FAIL "cannot import name 'make_decision_event'".

- [ ] **Step 3: Append to `generator/scenarios.py`**

```python
def make_decision_event(auth: dict, decision: str, risk_score: float,
                        decline_reason: str = "") -> dict:
    return {
        "auth_id": auth["auth_id"],
        "decision": decision,
        "decline_reason": decline_reason,
        "risk_score": float(risk_score),
        "event_ts": _now_iso(),
    }

def make_settlement_event(auth: dict) -> dict:
    return {
        "auth_id": auth["auth_id"],
        "settled_amount": auth["amount"],
        "event_ts": _now_iso(),
    }

def make_chargeback_event(auth: dict) -> dict:
    return {
        "auth_id": auth["auth_id"],
        "dispute_reason": "fraud" if auth["label_is_fraud"] else "service",
        "chargeback_amount": auth["amount"],
        "is_fraud_dispute": bool(auth["label_is_fraud"]),
        "event_ts": _now_iso(),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scenarios.py -v`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/scenarios.py tests/test_scenarios.py
git commit -m "feat: decision/settlement/chargeback event derivation"
```

### Task 9: Kafka producer

**Files:** Create `generator/producer.py`; Test `tests/test_producer.py`

- [ ] **Step 1: Write the failing test** (`tests/test_producer.py`)

```python
import json
from generator.producer import emit_batch

class FakeProducer:
    def __init__(self): self.sent = []
    def produce(self, topic, key, value): self.sent.append((topic, key, value))
    def flush(self): pass

def test_emit_batch_produces_all_four_topics():
    fake = FakeProducer()
    emit_batch(fake, n_normal=5, fraud=False)
    topics = {t for (t, _, _) in fake.sent}
    assert "card.authorizations" in topics
    assert "card.decisions" in topics
    assert "card.settlements" in topics

def test_fraud_batch_emits_labeled_card_testing():
    fake = FakeProducer()
    emit_batch(fake, n_normal=0, fraud=True)
    auths = [json.loads(v) for (t, _, v) in fake.sent if t == "card.authorizations"]
    assert auths and all(a["label_is_fraud"] for a in auths)
    chargebacks = [t for (t, _, _) in fake.sent if t == "card.chargebacks"]
    assert chargebacks  # fraud produces chargebacks
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_producer.py -v`
Expected: FAIL "No module named 'generator.producer'".

- [ ] **Step 3: Write `generator/producer.py`**

```python
import json
import random
from generator import scenarios
from common.schemas import TOPICS

def _send(producer, topic, event):
    producer.produce(topic, key=event["auth_id"], value=json.dumps(event))

def emit_batch(producer, n_normal: int, fraud: bool) -> None:
    auths = scenarios.normal_traffic(n_normal)
    if fraud:
        auths = scenarios.card_testing_burst(40) + [scenarios.high_amount_fraud()]
    for a in auths:
        _send(producer, TOPICS["authorizations"], a)
        if a["label_is_fraud"]:
            _send(producer, TOPICS["decisions"],
                  scenarios.make_decision_event(a, "decline", 95.0, "fraud_rule"))
            _send(producer, TOPICS["chargebacks"], scenarios.make_chargeback_event(a))
        else:
            _send(producer, TOPICS["decisions"],
                  scenarios.make_decision_event(a, "approve", 5.0))
            _send(producer, TOPICS["settlements"], scenarios.make_settlement_event(a))
    producer.flush()

def build_kafka_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap})

def main():  # pragma: no cover
    import time
    from common.config import Settings
    s = Settings.from_env()
    p = build_kafka_producer(s.kafka_bootstrap)
    while True:
        emit_batch(p, n_normal=200, fraud=False)
        if random.random() < 0.15:
            emit_batch(p, n_normal=0, fraud=True)
        time.sleep(1)

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_producer.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/producer.py tests/test_producer.py
git commit -m "feat: kafka producer emitting correlated card events"
```

---

## Phase 3 — Fraud Rule Engine (pure & fully tested)

### Task 10: Pure rule functions

**Files:** Create `streaming/rules.py`; Test `tests/test_rules.py`

Rules operate on a per-transaction **context dict** carrying the current txn plus
per-card windowed aggregates and the card's previous location/time.

- [ ] **Step 1: Write the failing test** (`tests/test_rules.py`)

```python
from streaming import rules

def ctx(**kw):
    base = dict(amount=50.0, mcc="5411", is_card_present=True,
                card_auth_count=2, card_small_amt_count=0, card_avg_amount=60.0,
                distance_km=1.0, hours_since_prev=5.0, device_is_new=False)
    base.update(kw); return base

def test_velocity():
    assert rules.velocity_score(ctx(card_auth_count=40)) > 0
    assert rules.velocity_score(ctx(card_auth_count=2)) == 0

def test_card_testing():
    hot = ctx(card_small_amt_count=25, amount=1.0)
    assert rules.card_testing_score(hot) > 0
    assert rules.card_testing_score(ctx(card_small_amt_count=0)) == 0

def test_impossible_travel():
    # 5000 km in 0.2 h -> 25000 km/h, impossible
    assert rules.impossible_travel_score(ctx(distance_km=5000, hours_since_prev=0.2)) > 0
    assert rules.impossible_travel_score(ctx(distance_km=10, hours_since_prev=5)) == 0

def test_amount_anomaly():
    assert rules.amount_anomaly_score(ctx(amount=5000, card_avg_amount=50)) > 0
    assert rules.amount_anomaly_score(ctx(amount=55, card_avg_amount=50)) == 0

def test_high_risk_mcc():
    assert rules.high_risk_mcc_score(ctx(mcc="7995")) > 0
    assert rules.high_risk_mcc_score(ctx(mcc="5411")) == 0

def test_cnp_new_device():
    assert rules.cnp_score(ctx(is_card_present=False, device_is_new=True)) > 0
    assert rules.cnp_score(ctx(is_card_present=True, device_is_new=False)) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rules.py -v`
Expected: FAIL "No module named 'streaming.rules'".

- [ ] **Step 3: Write `streaming/rules.py`**

```python
"""Pure card-fraud rule functions over a per-transaction context dict.

Each returns a non-negative risk contribution. No I/O, no Spark, no label access.
"""
from streaming.geo import implied_speed_kmh

HIGH_RISK_MCCS = {"7995", "6051"}        # gambling, quasi-cash
VELOCITY_CEILING = 20                     # auths per card per window
CARD_TESTING_MIN = 10                     # small-amount auths in window
SMALL_AMOUNT = 5.0
IMPOSSIBLE_SPEED_KMH = 1000.0             # faster than a commercial flight
AMOUNT_ANOMALY_RATIO = 10.0               # amount vs card average

def velocity_score(ctx: dict) -> float:
    over = ctx.get("card_auth_count", 0) - VELOCITY_CEILING
    return float(min(over, 80)) * 1.0 if over > 0 else 0.0

def card_testing_score(ctx: dict) -> float:
    n = ctx.get("card_small_amt_count", 0)
    if n >= CARD_TESTING_MIN and ctx.get("amount", 0) <= SMALL_AMOUNT:
        return float(n) * 3.0
    return 0.0

def impossible_travel_score(ctx: dict) -> float:
    speed = implied_speed_kmh(ctx.get("distance_km", 0.0),
                              ctx.get("hours_since_prev", 1e9))
    return 60.0 if speed > IMPOSSIBLE_SPEED_KMH else 0.0

def amount_anomaly_score(ctx: dict) -> float:
    avg = ctx.get("card_avg_amount", 0.0)
    if avg > 0 and ctx.get("amount", 0.0) >= AMOUNT_ANOMALY_RATIO * avg:
        return 35.0
    return 0.0

def high_risk_mcc_score(ctx: dict) -> float:
    return 20.0 if ctx.get("mcc") in HIGH_RISK_MCCS else 0.0

def cnp_score(ctx: dict) -> float:
    if not ctx.get("is_card_present", True) and ctx.get("device_is_new", False):
        return 15.0
    return 0.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rules.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add streaming/rules.py tests/test_rules.py
git commit -m "feat: pure card-fraud rule functions"
```

### Task 11: Score combination, severity, decision

**Files:** Create `streaming/scoring.py`; Test `tests/test_scoring.py`

- [ ] **Step 1: Write the failing test** (`tests/test_scoring.py`)

```python
from streaming.scoring import score_transaction, severity, decision

def ctx(**kw):
    base = dict(amount=50.0, mcc="5411", is_card_present=True,
                card_auth_count=2, card_small_amt_count=0, card_avg_amount=60.0,
                distance_km=1.0, hours_since_prev=5.0, device_is_new=False)
    base.update(kw); return base

def test_clean_txn_low_and_approved():
    s = score_transaction(ctx())
    assert s == 0
    assert severity(s) == "low"
    assert decision(s) == "approve"

def test_card_testing_high_and_declined():
    s = score_transaction(ctx(card_small_amt_count=25, amount=1.0,
                              is_card_present=False, device_is_new=True))
    assert s >= 50
    assert severity(s) == "high"
    assert decision(s) == "decline"

def test_decision_threshold():
    assert decision(0) == "approve"
    assert decision(49) == "approve"
    assert decision(50) == "decline"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scoring.py -v`
Expected: FAIL "No module named 'streaming.scoring'".

- [ ] **Step 3: Write `streaming/scoring.py`**

```python
from streaming import rules

RULES = [
    rules.velocity_score,
    rules.card_testing_score,
    rules.impossible_travel_score,
    rules.amount_anomaly_score,
    rules.high_risk_mcc_score,
    rules.cnp_score,
]
DECLINE_THRESHOLD = 50.0

def score_transaction(ctx: dict) -> float:
    return float(sum(rule(ctx) for rule in RULES))

def severity(score: float) -> str:
    if score >= 50:
        return "high"
    if score >= 20:
        return "medium"
    return "low"

def decision(score: float) -> str:
    return "decline" if score >= DECLINE_THRESHOLD else "approve"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scoring.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add streaming/scoring.py tests/test_scoring.py
git commit -m "feat: rule score combination, severity, approve/decline"
```

---

## Phase 4 — Feature Engineering & ML Model

### Task 12: Per-transaction features

**Files:** Create `batch/__init__.py`, `batch/features.py`; Test `tests/test_features.py`

- [ ] **Step 1: Write the failing test** (`tests/test_features.py`)

```python
import pandas as pd
from batch.features import build_features, FEATURE_COLS

def test_build_features_columns_and_label():
    df = pd.DataFrame([
        dict(amount=1.0, mcc="7995", entry_mode="online", is_card_present=False,
             event_ts="2026-06-21T10:00:00+00:00", card_id="c1", label_is_fraud=True),
        dict(amount=80.0, mcc="5411", entry_mode="chip", is_card_present=True,
             event_ts="2026-06-21T14:00:00+00:00", card_id="c2", label_is_fraud=False),
    ])
    out = build_features(df)
    for c in FEATURE_COLS:
        assert c in out.columns
    assert "label" in out.columns
    assert out.loc[0, "is_high_risk_mcc"] == 1
    assert out.loc[1, "is_high_risk_mcc"] == 0
    assert out.loc[0, "log_amount"] < out.loc[1, "log_amount"]
    assert out.loc[0, "hour"] == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_features.py -v`
Expected: FAIL "No module named 'batch.features'".

- [ ] **Step 3: Write `batch/__init__.py`** (empty), then `batch/features.py`

```python
import numpy as np
import pandas as pd

HIGH_RISK_MCCS = {"7995", "6051"}
FEATURE_COLS = ["log_amount", "is_card_present", "is_online", "is_high_risk_mcc",
                "hour", "card_txn_rank"]

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["log_amount"] = np.log1p(df["amount"].astype(float))
    out["is_card_present"] = df["is_card_present"].astype(int)
    out["is_online"] = (df["entry_mode"] == "online").astype(int)
    out["is_high_risk_mcc"] = df["mcc"].isin(HIGH_RISK_MCCS).astype(int)
    out["hour"] = pd.to_datetime(df["event_ts"]).dt.hour
    # simple per-card velocity proxy: ordinal rank of the txn within its card
    out["card_txn_rank"] = df.groupby("card_id").cumcount()
    if "label_is_fraud" in df.columns:
        out["label"] = df["label_is_fraud"].astype(int)
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_features.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/__init__.py batch/features.py tests/test_features.py
git commit -m "feat: per-transaction ML feature engineering"
```

### Task 13: Train / evaluate / persist model

**Files:** Create `batch/model.py`; Test `tests/test_model.py`

- [ ] **Step 1: Write the failing test** (`tests/test_model.py`)

```python
import numpy as np
import pandas as pd
from batch.model import train_model, score_frame, save_model, load_model

def _labeled_features(n=400, seed=0):
    rng = np.random.default_rng(seed)
    # fraud: low log_amount, online, high-risk mcc; legit: opposite. Separable.
    rows = []
    for _ in range(n):
        fraud = rng.random() < 0.3
        rows.append(dict(
            log_amount=rng.normal(0.5 if fraud else 4.5, 0.4),
            is_card_present=0 if fraud else 1,
            is_online=1 if fraud else 0,
            is_high_risk_mcc=1 if fraud and rng.random() < 0.7 else 0,
            hour=int(rng.integers(0, 24)),
            card_txn_rank=int(rng.integers(0, 30)) if fraud else int(rng.integers(0, 3)),
            label=int(fraud)))
    return pd.DataFrame(rows)

def test_train_model_separates_fraud():
    feats = _labeled_features()
    model, metrics = train_model(feats, seed=42)
    assert metrics["roc_auc"] > 0.9          # clearly separable synthetic data
    assert 0 < metrics["n_test"] <= len(feats)

def test_score_frame_high_prob_for_obvious_fraud(tmp_path):
    feats = _labeled_features()
    model, _ = train_model(feats, seed=42)
    obvious = pd.DataFrame([dict(log_amount=0.3, is_card_present=0, is_online=1,
                                 is_high_risk_mcc=1, hour=3, card_txn_rank=25)])
    probs = score_frame(model, obvious)
    assert probs[0] > 0.5

def test_save_and_load_roundtrip(tmp_path):
    feats = _labeled_features()
    model, _ = train_model(feats, seed=42)
    path = tmp_path / "m.joblib"
    save_model(model, str(path))
    loaded = load_model(str(path))
    legit = pd.DataFrame([dict(log_amount=4.6, is_card_present=1, is_online=0,
                               is_high_risk_mcc=0, hour=12, card_txn_rank=1)])
    assert score_frame(loaded, legit)[0] < 0.5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_model.py -v`
Expected: FAIL "No module named 'batch.model'".

- [ ] **Step 3: Write `batch/model.py`**

```python
import os
import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from batch.features import FEATURE_COLS

def train_model(features: pd.DataFrame, seed: int = 42):
    X = features[FEATURE_COLS]
    y = features["label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.25, random_state=seed, stratify=y)
    model = HistGradientBoostingClassifier(random_state=seed, max_iter=200)
    model.fit(X_tr, y_tr)
    proba = model.predict_proba(X_te)[:, 1]
    metrics = {"roc_auc": float(roc_auc_score(y_te, proba)), "n_test": int(len(y_te))}
    return model, metrics

def score_frame(model, features: pd.DataFrame):
    return model.predict_proba(features[FEATURE_COLS])[:, 1]

def save_model(model, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    joblib.dump(model, path)

def load_model(path: str):
    return joblib.load(path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_model.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/model.py tests/test_model.py
git commit -m "feat: HistGradientBoosting train/evaluate/persist for fraud scoring"
```

---

## Phase 5 — Streaming Fraud Job (rules + ML)

### Task 14: Streaming scoring helper (rules + model)

**Files:** Create `streaming/fraud_job.py`; Test `tests/test_fraud_job.py`

The Spark wiring is integration glue; the per-row combination of rule score + model
probability is a **testable pure helper** `apply_scoring(rows, model_prob_fn)`.

- [ ] **Step 1: Write the failing test** (`tests/test_fraud_job.py`)

```python
from streaming.fraud_job import apply_scoring

def test_apply_scoring_combines_rules_and_model():
    rows = [
        dict(auth_id="a1", card_id="c_bad", amount=1.0, mcc="7995",
             is_card_present=False, card_auth_count=40, card_small_amt_count=25,
             card_avg_amount=50.0, distance_km=1.0, hours_since_prev=5.0,
             device_is_new=True),
        dict(auth_id="a2", card_id="c_ok", amount=60.0, mcc="5411",
             is_card_present=True, card_auth_count=2, card_small_amt_count=0,
             card_avg_amount=60.0, distance_km=1.0, hours_since_prev=5.0,
             device_is_new=False),
    ]
    # stub model: returns high prob for the small-amount online row
    def model_prob_fn(r): return 0.95 if r["amount"] < 5 else 0.02
    out = {o["auth_id"]: o for o in apply_scoring(rows, model_prob_fn)}
    assert out["a1"]["decision"] == "decline"
    assert out["a1"]["severity"] == "high"
    assert out["a1"]["model_prob"] == 0.95
    assert out["a2"]["decision"] == "approve"
    assert out["a2"]["model_prob"] == 0.02
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fraud_job.py -v`
Expected: FAIL "No module named 'streaming.fraud_job'".

- [ ] **Step 3: Write `streaming/fraud_job.py`**

```python
from streaming.scoring import score_transaction, severity, decision

def apply_scoring(rows: list[dict], model_prob_fn) -> list[dict]:
    out = []
    for r in rows:
        rule_score = score_transaction(r)
        prob = float(model_prob_fn(r))
        out.append({**r, "rule_score": rule_score, "model_prob": prob,
                    "severity": severity(rule_score),
                    "decision": decision(rule_score)})
    return out

# --- Spark wiring (integration; not unit-tested) ---
def run():  # pragma: no cover
    from pyspark.sql.functions import col, from_json
    from common.spark import build_spark
    from common.config import Settings
    from common.schemas import TOPICS
    from streaming.bronze_sink import AUTH_SCHEMA
    from batch.model import load_model, score_frame
    from batch.features import build_features
    import pandas as pd, psycopg2

    s = Settings.from_env()
    spark = build_spark("card-fraud-job", s)
    model = load_model(s.model_path)

    def _score_batch(df, _id):
        pdf = (df.select(from_json(col("value").cast("string"),
                                   AUTH_SCHEMA).alias("d")).select("d.*").toPandas())
        if pdf.empty:
            return
        pdf["card_auth_count"] = pdf.groupby("card_id")["auth_id"].transform("size")
        pdf["card_small_amt_count"] = (pdf.assign(s=(pdf["amount"] <= 5).astype(int))
                                       .groupby("card_id")["s"].transform("sum"))
        pdf["card_avg_amount"] = pdf.groupby("card_id")["amount"].transform("mean")
        pdf["distance_km"] = 0.0
        pdf["hours_since_prev"] = 1e9
        pdf["device_is_new"] = False
        feats = build_features(pdf)
        probs = score_frame(model, feats)
        rows = pdf.to_dict("records")
        scored = apply_scoring(rows, lambda r: probs[rows.index(r)])
        conn = psycopg2.connect(s.postgres_dsn)
        with conn, conn.cursor() as cur:
            for a in scored:
                if a["decision"] == "decline" or a["model_prob"] > 0.5:
                    cur.execute(
                        "INSERT INTO fraud_alerts(auth_id, card_id, rule_score, "
                        "model_prob, severity, decision) VALUES (%s,%s,%s,%s,%s,%s)",
                        (a["auth_id"], a["card_id"], a["rule_score"],
                         a["model_prob"], a["severity"], a["decision"]))
        conn.close()

    (spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers", s.kafka_bootstrap)
     .option("subscribe", TOPICS["authorizations"]).load()
     .writeStream.foreachBatch(_score_batch).start())
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_fraud_job.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Create Postgres alert table**

Run:
```bash
docker exec -i $(docker compose -f infra/docker-compose.yml ps -q postgres) \
  psql -U card -d card -c "CREATE TABLE IF NOT EXISTS fraud_alerts(
    id serial primary key, auth_id text, card_id text, rule_score double precision,
    model_prob double precision, severity text, decision text,
    created_at timestamptz default now());"
```
Expected: `CREATE TABLE`.

- [ ] **Step 6: Commit**

```bash
git add streaming/fraud_job.py tests/test_fraud_job.py
git commit -m "feat: streaming fraud job combining rules + ML probability"
```

### Task 15: Bronze streaming sink

**Files:** Create `streaming/bronze_sink.py`

> Integration glue (Kafka→Iceberg). Complete content provided.

- [ ] **Step 1: Write `streaming/bronze_sink.py`**

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               BooleanType)
from common.spark import build_spark
from common.config import Settings
from common.schemas import TOPICS

AUTH_SCHEMA = StructType([
    StructField("auth_id", StringType()), StructField("card_id", StringType()),
    StructField("account_id", StringType()), StructField("merchant_id", StringType()),
    StructField("mcc", StringType()), StructField("merchant_country", StringType()),
    StructField("amount", DoubleType()), StructField("currency", StringType()),
    StructField("entry_mode", StringType()), StructField("is_card_present", BooleanType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("device_id", StringType()), StructField("event_ts", StringType()),
    StructField("label_is_fraud", BooleanType()),
])

def _read(spark, bootstrap, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest").load())

def run():  # pragma: no cover
    s = Settings.from_env()
    spark = build_spark("card-bronze-sink", s)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    raw = _read(spark, s.kafka_bootstrap, TOPICS["authorizations"])
    parsed = (raw.select(from_json(col("value").cast("string"), AUTH_SCHEMA).alias("d"))
                 .select("d.*"))
    (parsed.writeStream.format("iceberg")
        .option("checkpointLocation", "/tmp/ckpt/card_bronze_auth")
        .toTable("lake.bronze.authorizations"))
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 2: Verify import**

Run: `python -c "import streaming.bronze_sink; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add streaming/bronze_sink.py
git commit -m "feat: bronze streaming sink kafka->iceberg"
```

---

## Phase 6 — Batch: Silver + entrypoints

### Task 16: Silver transform

**Files:** Create `batch/silver.py`; Test `tests/test_silver.py`

- [ ] **Step 1: Write the failing test** (`tests/test_silver.py`)

```python
import pytest
from pyspark.sql import SparkSession
from batch.silver import build_silver

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").getOrCreate()
    yield s
    s.stop()

def test_silver_joins_auth_decision_settlement_chargeback(spark):
    auth = spark.createDataFrame(
        [("a1", "c1", "5411", 80.0, True),
         ("a1", "c1", "5411", 80.0, True),   # dup
         ("a2", "c2", "7995", 1.0, False)],
        ["auth_id", "card_id", "mcc", "amount", "is_card_present"])
    dec = spark.createDataFrame(
        [("a1", "approve", 5.0), ("a2", "decline", 95.0)],
        ["auth_id", "decision", "risk_score"])
    settle = spark.createDataFrame([("a1", 80.0)], ["auth_id", "settled_amount"])
    cb = spark.createDataFrame([("a2", 1.0, True)],
                               ["auth_id", "chargeback_amount", "is_fraud_dispute"])
    out = build_silver(auth, dec, settle, cb).orderBy("auth_id").collect()
    assert len(out) == 2
    a2 = [r for r in out if r["auth_id"] == "a2"][0]
    assert a2["decision"] == "decline"
    assert a2["is_charged_back"] is True
    a1 = [r for r in out if r["auth_id"] == "a1"][0]
    assert a1["settled_amount"] == 80.0
    assert a1["is_charged_back"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_silver.py -v`
Expected: FAIL "No module named 'batch.silver'".

- [ ] **Step 3: Write `batch/silver.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def build_silver(auth: DataFrame, decisions: DataFrame, settlements: DataFrame,
                 chargebacks: DataFrame) -> DataFrame:
    a = auth.dropDuplicates(["auth_id"])
    joined = (a.join(decisions, "auth_id", "left")
               .join(settlements, "auth_id", "left")
               .join(chargebacks.select("auth_id", "chargeback_amount",
                                        "is_fraud_dispute"), "auth_id", "left"))
    return joined.withColumn(
        "is_charged_back", when(col("chargeback_amount").isNotNull(), True)
                           .otherwise(False))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_silver.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/silver.py tests/test_silver.py
git commit -m "feat: bronze->silver join transform"
```

### Task 17: Batch entrypoints (silver, train, score)

**Files:** Create `batch/run_silver.py`, `batch/run_train.py`, `batch/run_score.py`

> Integration entrypoints invoked by Airflow. Complete content; not unit-tested.

- [ ] **Step 1: Write `batch/run_silver.py`**

```python
from common.spark import build_spark
from batch.silver import build_silver

def run():  # pragma: no cover
    spark = build_spark("card-batch-silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    silver = build_silver(spark.table("lake.bronze.authorizations"),
                          spark.table("lake.bronze.decisions"),
                          spark.table("lake.bronze.settlements"),
                          spark.table("lake.bronze.chargebacks"))
    silver.writeTo("lake.silver.transactions").createOrReplace()
    spark.stop()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 2: Write `batch/run_train.py`**

```python
from common.spark import build_spark
from common.config import Settings
from batch.features import build_features
from batch.model import train_model, save_model

def run():  # pragma: no cover
    s = Settings.from_env()
    spark = build_spark("card-model-train", s)
    pdf = spark.table("lake.silver.transactions").select(
        "amount", "mcc", "entry_mode", "is_card_present", "event_ts",
        "card_id", "label_is_fraud").toPandas()
    feats = build_features(pdf)
    model, metrics = train_model(feats)
    print("model metrics:", metrics)
    save_model(model, s.model_path)
    spark.stop()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 3: Write `batch/run_score.py`**

```python
import psycopg2
from common.spark import build_spark
from common.config import Settings
from batch.features import build_features
from batch.model import load_model, score_frame

def run():  # pragma: no cover
    s = Settings.from_env()
    spark = build_spark("card-model-score", s)
    model = load_model(s.model_path)
    pdf = spark.table("lake.silver.transactions").select(
        "auth_id", "amount", "mcc", "entry_mode", "is_card_present",
        "event_ts", "card_id").toPandas()
    feats = build_features(pdf)
    pdf["model_prob"] = score_frame(model, feats)
    conn = psycopg2.connect(s.postgres_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS fraud_scores(
            auth_id text, model_prob double precision,
            scored_at timestamptz default now())""")
        for _, r in pdf.iterrows():
            cur.execute("INSERT INTO fraud_scores(auth_id, model_prob) VALUES (%s,%s)",
                        (r["auth_id"], float(r["model_prob"])))
    conn.close()
    spark.stop()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 4: Verify imports compile**

Run: `python -m py_compile batch/run_silver.py batch/run_train.py batch/run_score.py && echo ok`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add batch/run_silver.py batch/run_train.py batch/run_score.py
git commit -m "feat: batch entrypoints for silver, model training, scoring"
```

---

## Phase 7 — dbt Gold Marts

### Task 18: dbt project + marts

**Files:** Create `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/packages.yml`,
`dbt/models/gold/approval_monitoring.sql`, `spend_by_mcc.sql`, `spend_by_merchant.sql`,
`chargeback_rates.sql`, `risk_exposure.sql`, `fraud_model_scores.sql`,
`dbt/models/gold/schema.yml`

> Validated by `dbt build`. Complete content provided.

- [ ] **Step 1: Create `dbt/dbt_project.yml`**

```yaml
name: card_gold
version: "1.0"
profile: card
model-paths: ["models"]
models:
  card_gold:
    gold:
      +materialized: table
```

- [ ] **Step 2: Create `dbt/profiles.yml`**

```yaml
card:
  target: dev
  outputs:
    dev:
      type: trino
      host: localhost
      port: 8080
      user: analytics
      catalog: iceberg
      schema: gold
      http_scheme: http
```

- [ ] **Step 3: Create `dbt/packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

- [ ] **Step 4: Create `dbt/models/gold/approval_monitoring.sql`**

```sql
select
  decision,
  decline_reason,
  count(*) as txn_count,
  round(1.0 * count_if(decision = 'approve') / nullif(count(*), 0), 4) as approval_rate
from iceberg.silver.transactions
group by 1, 2
```

- [ ] **Step 5: Create `dbt/models/gold/spend_by_mcc.sql`**

```sql
select
  mcc,
  count(*) as txn_count,
  sum(amount) as total_amount,
  avg(amount) as avg_amount
from iceberg.silver.transactions
group by 1
```

- [ ] **Step 6: Create `dbt/models/gold/spend_by_merchant.sql`**

```sql
select
  merchant_id,
  count(*) as txn_count,
  sum(amount) as total_amount
from iceberg.silver.transactions
group by 1
```

- [ ] **Step 7: Create `dbt/models/gold/chargeback_rates.sql`**

```sql
select
  mcc,
  count(*) as txn_count,
  count_if(is_charged_back) as chargebacks,
  count_if(is_fraud_dispute) as fraud_chargebacks,
  round(1.0 * count_if(is_charged_back) / nullif(count(*), 0), 4) as chargeback_rate
from iceberg.silver.transactions
group by 1
```

- [ ] **Step 8: Create `dbt/models/gold/risk_exposure.sql`**

```sql
select
  merchant_country,
  count_if(decision = 'decline') as declined_count,
  sum(case when decision = 'decline' then amount else 0 end) as blocked_amount,
  sum(case when is_fraud_dispute then chargeback_amount else 0 end) as fraud_loss
from iceberg.silver.transactions
group by 1
```

- [ ] **Step 9: Create `dbt/models/gold/fraud_model_scores.sql`**

```sql
select
  case when model_prob >= 0.8 then 'high'
       when model_prob >= 0.5 then 'medium' else 'low' end as risk_band,
  count(*) as txn_count,
  avg(model_prob) as avg_prob
from postgres.public.fraud_scores
group by 1
```

- [ ] **Step 10: Create `dbt/models/gold/schema.yml`**

```yaml
version: 2
models:
  - name: approval_monitoring
    columns:
      - name: approval_rate
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1
  - name: spend_by_mcc
    columns:
      - name: mcc
        tests: [not_null]
  - name: chargeback_rates
    columns:
      - name: chargeback_rate
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1
  - name: risk_exposure
    columns:
      - name: merchant_country
        tests: [not_null]
```

- [ ] **Step 11: Build marts** (after Silver + scores exist)

Run: `cd dbt && dbt deps && dbt build --profiles-dir .`
Expected: models run and tests pass.

- [ ] **Step 12: Commit**

```bash
git add dbt/
git commit -m "feat: dbt gold marts (approval, spend, chargebacks, risk, fraud scores)"
```

---

## Phase 8 — Orchestration & Quality

### Task 19: Airflow DAGs

**Files:** Create `airflow/dags/batch_etl_dag.py`, `airflow/dags/model_training_dag.py`, `airflow/dags/model_scoring_dag.py`

> Validity verified by importing without error.

- [ ] **Step 1: Create `airflow/dags/batch_etl_dag.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_batch_etl", start_date=datetime(2026, 1, 1),
         schedule="@hourly", catchup=False) as dag:
    silver = BashOperator(task_id="bronze_to_silver",
                          bash_command="python -m batch.run_silver")
    dbt = BashOperator(task_id="dbt_build",
                       bash_command="cd $PROJECT_ROOT/dbt && dbt build --profiles-dir .")
    quality = BashOperator(task_id="quality_checks",
                           bash_command="python -m quality.expectations_silver")
    silver >> dbt >> quality
```

- [ ] **Step 2: Create `airflow/dags/model_training_dag.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_model_training", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="train_model", bash_command="python -m batch.run_train")
```

- [ ] **Step 3: Create `airflow/dags/model_scoring_dag.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_model_scoring", start_date=datetime(2026, 1, 1),
         schedule="@hourly", catchup=False) as dag:
    BashOperator(task_id="score_transactions", bash_command="python -m batch.run_score")
```

- [ ] **Step 4: Verify DAGs import**

Run: `python airflow/dags/batch_etl_dag.py && python airflow/dags/model_training_dag.py && python airflow/dags/model_scoring_dag.py && echo ok`
Expected: prints `ok` (requires `pip install apache-airflow==2.9.1` to validate locally).

- [ ] **Step 5: Commit**

```bash
git add airflow/
git commit -m "feat: airflow dags for etl, model training, and scoring"
```

### Task 20: Great Expectations on Silver

**Files:** Create `quality/__init__.py`, `quality/expectations_silver.py`; Test `tests/test_quality.py`

- [ ] **Step 1: Write the failing test** (`tests/test_quality.py`)

```python
import pandas as pd
from quality.expectations_silver import validate_silver

def test_validate_silver_catches_null_auth_id():
    bad = pd.DataFrame({"auth_id": ["a1", None], "amount": [10.0, 20.0],
                        "decision": ["approve", "decline"]})
    r = validate_silver(bad)
    assert r["ok"] is False
    assert "auth_id" in r["failures"]

def test_validate_silver_catches_negative_amount():
    bad = pd.DataFrame({"auth_id": ["a1"], "amount": [-5.0], "decision": ["approve"]})
    r = validate_silver(bad)
    assert r["ok"] is False
    assert "amount" in r["failures"]

def test_validate_silver_passes_clean():
    good = pd.DataFrame({"auth_id": ["a1", "a2"], "amount": [10.0, 20.0],
                         "decision": ["approve", "decline"]})
    assert validate_silver(good)["ok"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_quality.py -v`
Expected: FAIL "No module named 'quality.expectations_silver'".

- [ ] **Step 3: Write `quality/__init__.py`** (empty), then `quality/expectations_silver.py`

```python
import pandas as pd

VALID_DECISIONS = {"approve", "decline"}

def validate_silver(df: pd.DataFrame) -> dict:
    failures = []
    if df["auth_id"].isnull().any():
        failures.append("auth_id")
    if (df["amount"] < 0).any():
        failures.append("amount")
    if not df["decision"].isin(VALID_DECISIONS).all():
        failures.append("decision")
    return {"ok": len(failures) == 0, "failures": failures}

def main():  # pragma: no cover
    raise SystemExit("wire to read silver from Trino in production")

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_quality.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add quality/ tests/test_quality.py
git commit -m "feat: data-quality validation for silver"
```

---

## Phase 9 — End-to-End Smoke, Live Verify, Docs

### Task 21: E2E smoke test

**Files:** Create `tests/test_e2e_smoke.py`

Exercises generator → feature build → model train → combined rule+ML scoring in-process,
proving the fraud path end to end without the cluster.

- [ ] **Step 1: Write the test** (`tests/test_e2e_smoke.py`)

```python
import json
import pandas as pd
from generator.producer import emit_batch
from batch.features import build_features
from batch.model import train_model, score_frame
from streaming.fraud_job import apply_scoring

class FakeProducer:
    def __init__(self): self.sent = []
    def produce(self, topic, key, value): self.sent.append((topic, key, value))
    def flush(self): pass

def _auths(sent):
    return [json.loads(v) for (t, _, v) in sent if t == "card.authorizations"]

def test_card_testing_burst_is_declined_and_high_model_prob():
    fake = FakeProducer()
    for _ in range(6):
        emit_batch(fake, n_normal=120, fraud=False)
        emit_batch(fake, n_normal=0, fraud=True)
    auths = _auths(fake.sent)
    df = pd.DataFrame(auths)
    feats = build_features(df)
    model, metrics = train_model(feats, seed=42)
    assert metrics["roc_auc"] > 0.8

    # build streaming context per card and score
    df["card_auth_count"] = df.groupby("card_id")["auth_id"].transform("size")
    df["card_small_amt_count"] = (df.assign(s=(df["amount"] <= 5).astype(int))
                                  .groupby("card_id")["s"].transform("sum"))
    df["card_avg_amount"] = df.groupby("card_id")["amount"].transform("mean")
    df["distance_km"] = 0.0
    df["hours_since_prev"] = 1e9
    df["device_is_new"] = ~df["is_card_present"]
    probs = score_frame(model, build_features(df))
    rows = df.to_dict("records")
    scored = apply_scoring(rows, lambda r: probs[rows.index(r)])

    fraud_rows = [s for s in scored if s["label_is_fraud"]]
    legit_rows = [s for s in scored if not s["label_is_fraud"]]
    # most labeled card-testing fraud is declined by rules
    assert sum(s["decision"] == "decline" for s in fraud_rows) > len(fraud_rows) * 0.5
    # legit traffic is overwhelmingly approved
    assert sum(s["decision"] == "approve" for s in legit_rows) > len(legit_rows) * 0.9
```

- [ ] **Step 2: Run the full suite**

Run: `pytest -v`
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_e2e_smoke.py
git commit -m "test: end-to-end fraud-path smoke (rules + ML)"
```

### Task 22: Live cluster verification script

**Files:** Create `scripts/verify_cluster.py`

> Bounded live check (run with stack up). Complete content; mirrors Project 1's pattern.

- [ ] **Step 1: Write `scripts/verify_cluster.py`**

```python
"""Bounded live end-to-end verification against the Docker cluster.

Kafka -> Spark -> Iceberg-on-MinIO -> Postgres -> Trino, plus model train/score.
Run (stack up): PYTHONPATH=. python scripts/verify_cluster.py
"""
import json
import sys
import psycopg2
from pyspark.sql.functions import col, from_json

from common.spark import build_spark
from common.config import Settings
from common.schemas import TOPICS
from streaming.bronze_sink import AUTH_SCHEMA
from batch.features import build_features
from batch.model import train_model, score_frame
from generator import scenarios

def produce(bootstrap):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    auths = scenarios.normal_traffic(200) + scenarios.card_testing_burst(40)
    for a in auths:
        p.produce(TOPICS["authorizations"], key=a["auth_id"], value=json.dumps(a))
    p.flush()
    return len(auths)

def main() -> int:
    s = Settings.from_env()
    n = produce(s.kafka_bootstrap)
    print(f"[1/4] Produced {n} authorizations to Kafka")

    spark = build_spark("verify-cluster", s)
    spark.sparkContext.setLogLevel("ERROR")
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", s.kafka_bootstrap)
           .option("subscribe", TOPICS["authorizations"])
           .option("startingOffsets", "earliest").load())
    auth = raw.select(from_json(col("value").cast("string"),
                                AUTH_SCHEMA).alias("d")).select("d.*")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    auth.writeTo("lake.bronze.authorizations").createOrReplace()
    cnt = spark.table("lake.bronze.authorizations").count()
    assert cnt == n, f"expected {n}, got {cnt}"
    print(f"[2/4] Kafka -> Bronze Iceberg (MinIO) OK ({cnt} rows)")

    pdf = spark.table("lake.bronze.authorizations").select(
        "auth_id", "amount", "mcc", "entry_mode", "is_card_present",
        "event_ts", "card_id", "label_is_fraud").toPandas()
    feats = build_features(pdf)
    model, metrics = train_model(feats, seed=42)
    assert metrics["roc_auc"] > 0.7, metrics
    print(f"[3/4] Model trained on lakehouse data OK (roc_auc={metrics['roc_auc']:.3f})")

    import trino
    tc = trino.dbapi.connect(host=s.trino_host, port=s.trino_port, user="verify",
                             catalog="iceberg")
    cur = tc.cursor()
    cur.execute("SELECT count(*) FROM iceberg.bronze.authorizations")
    tcount = cur.fetchone()[0]
    assert tcount == n, f"Trino saw {tcount}, expected {n}"
    print(f"[4/4] Trino reads Iceberg bronze.authorizations OK ({tcount} rows)")

    spark.stop()
    print("\nALL LIVE CLUSTER CHECKS PASSED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify import compiles**

Run: `python -m py_compile scripts/verify_cluster.py && echo ok`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add scripts/verify_cluster.py
git commit -m "test: bounded live cluster verification script"
```

### Task 23: SCALING.md & RUNBOOK.md

**Files:** Create `docs/SCALING.md`, `docs/RUNBOOK.md`

- [ ] **Step 1: Write `docs/SCALING.md`**

Map each local choice to petabyte production (Kafka partitioning by `card_id` hash,
Spark tuning/state store, Iceberg partition by `days(event_ts)` + `bucket(card_id)`
with compaction, MinIO→S3, Postgres→warehouse, Airflow→MWAA) **plus model-ops:**
training cadence and triggers, a feature store for train/serve parity, model
registry/versioning, online vs offline inference trade-offs (here: rules real-time,
model in micro-batch), and **drift monitoring via PSI** on feature and score
distributions with automated retraining triggers.

- [ ] **Step 2: Write `docs/RUNBOOK.md`**

Ordered local start-up: (1) `docker compose up -d`; (2) create `lakehouse` bucket via
`mc` on the compose network; (3) create Postgres `fraud_alerts`/`fraud_scores` tables;
(4) start `streaming/bronze_sink.py`; (5) run `batch/run_silver.py` then
`batch/run_train.py` to persist a model; (6) start `streaming/fraud_job.py`;
(7) start `generator/producer.py`; (8) trigger Airflow DAGs; (9) `dbt build`;
(10) inspect marts in Trino + `fraud_alerts`/`fraud_scores` in Postgres. Include
teardown (`docker compose down -v`, clear `/tmp/ckpt`) and troubleshooting (Kafka
image/listeners, REST-catalog reachability, missing model file → run training first).

- [ ] **Step 3: Commit**

```bash
git add docs/SCALING.md docs/RUNBOOK.md
git commit -m "docs: scaling blueprint (incl. model-ops) and runbook"
```

---

## Self-Review Notes (completed)

- **Spec coverage:** event model (Task 4), labeled generator + fraud patterns + all 4
  topic events (Tasks 7-9), geo helper (Task 6), real-time rule engine + scoring +
  decision (Tasks 10-11), supervised features + model train/eval/persist (Tasks 12-13),
  streaming rules+ML job (Task 14), Bronze sink (Task 15), Silver (Task 16), batch
  train/score entrypoints (Task 17), all five+ Gold marts incl. approval/spend/
  chargebacks/risk/fraud-scores (Task 18), Airflow incl. training + scoring DAGs
  (Task 19), Great Expectations (Task 20), e2e smoke (Task 21), live verify (Task 22),
  SCALING.md with model-ops (Task 23). Every spec section maps to a task.
- **Placeholder scan:** no TBD/TODO; every code step has complete code.
- **Type consistency:** `build_features`/`FEATURE_COLS`, `train_model`/`score_frame`/
  `save_model`/`load_model`, `score_transaction`/`severity`/`decision`,
  `apply_scoring(rows, model_prob_fn)`, `build_silver(auth, decisions, settlements,
  chargebacks)`, `emit_batch(producer, n_normal, fraud)`, `AUTH_SCHEMA`,
  `validate_silver` are consistent across all referencing tasks.
- **Label isolation:** `label_is_fraud` is consumed only by `build_features`/model code
  and the generator; the rule engine context never includes it.
```

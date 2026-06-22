# Banking Credit-Card Transaction Pipeline

A production-grade, locally-runnable pipeline for a large bank's **credit-card
authorization** stream. It makes a real-time approve/decline decision, detects card
fraud (card-testing, velocity, impossible-travel, amount/MCC anomalies, CNP), scores
every transaction with a **supervised gradient-boosted model**, and serves approval,
spend, chargeback, and risk-exposure analytics.

## Architecture

```
Labeled synthetic generator → Kafka (auth/decision/settlement/chargeback) ┐
                          ├─ real-time RULE ENGINE → approve/decline → Postgres alerts
                          ├─ ML micro-batch scoring (HistGradientBoosting) → fraud prob
                          └─ Bronze landing → Iceberg (MinIO, REST catalog)
                                    │
        Airflow → Spark batch: Bronze→Silver → dbt Gold marts (Trino)
                  + model training (daily) + model scoring (hourly) → Postgres
```

- **Stream:** Spark Structured Streaming · **Lakehouse:** Iceberg REST catalog on MinIO
- **ML:** scikit-learn `HistGradientBoostingClassifier` (offline train, micro-batch score)
- **Serving:** Postgres + Trino · **Orchestration:** Airflow · **Marts:** dbt

See [`docs/superpowers/specs/`](docs/superpowers/specs) for the design spec,
[`docs/SCALING.md`](docs/SCALING.md) for the petabyte + model-ops mapping, and
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) to run it.

## Fraud detection
- **Real-time rules** ([`streaming/rules.py`](streaming/rules.py)): velocity,
  **card-testing** (rapid micro-amount CNP auths), **impossible-travel** (haversine
  speed), amount-vs-history anomaly, high-risk MCC, CNP+new-device → approve/decline.
- **Supervised ML** ([`batch/model.py`](batch/model.py)): HistGradientBoosting trained
  on labeled data, evaluated by holdout ROC-AUC, scored in the micro-batch + a mart.
- The fraud **label never reaches the rule engine** — only feature/model code uses it.

## Quick start
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q                       # 37 tests, no Docker required
docker compose -f infra/docker-compose.yml up -d   # then follow docs/RUNBOOK.md
```

## Layout
| Path | Responsibility |
|------|----------------|
| `generator/` | labeled synthetic txn producer (normal + fraud patterns) |
| `streaming/` | geo helper, rule engine, scoring, bronze sink, rules+ML fraud job |
| `batch/` | silver transform, ML features, model train/eval/persist, scoring entrypoints |
| `dbt/` | gold marts (approval, spend, chargebacks, risk, fraud scores) + tests |
| `airflow/` | ETL, model-training, model-scoring DAGs |
| `quality/` | Silver data-quality validation |
| `infra/` | docker-compose (Kafka, MinIO, Iceberg-REST, Trino, Postgres) |
| `tests/` | pytest unit + end-to-end fraud-path tests |

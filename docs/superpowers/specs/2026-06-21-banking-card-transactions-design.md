# Banking Credit-Card Transaction Pipeline — Design Spec

**Date:** 2026-06-21
**Status:** Approved (design), pending implementation plan
**Goal:** Production-grade reference pipeline (local open-source, docker-compose,
fully working) for a large bank's credit-card authorization stream — real-time
fraud detection + supervised ML scoring + card analytics.

---

## 1. Domain & Narrative

A large bank's card-processing platform ingests **credit-card authorization** events
at very high volume. From one event stream it must:

1. Make a real-time **approve/decline** decision per authorization.
2. Detect fraud — **card-testing** (rapid micro-amount auths), **velocity**,
   **impossible-travel**, amount-vs-history anomalies, high-risk MCC, card-not-present
   (CNP) risk.
3. Score every transaction with a **supervised ML model** (gradient-boosted).
4. Serve approval, spend, chargeback, and risk-exposure analytics from one lakehouse.

## 2. Architecture Choices (locked)

Same stack family as the telecom project (proven end-to-end):

| Concern          | Choice |
|------------------|--------|
| Stream engine    | Spark Structured Streaming |
| Lakehouse        | Apache Iceberg via **REST catalog** (`tabulario/iceberg-rest`) on MinIO |
| ML library       | **scikit-learn `HistGradientBoostingClassifier`** (no native deps, seed-deterministic, joblib-serializable) |
| ML scoring       | Train **offline** (batch/Airflow); score in the **streaming micro-batch** + a batch mart; retrain on schedule |
| Messaging        | Apache Kafka (`apache/kafka:3.7.0`, dual listeners) |
| Orchestration    | Apache Airflow |
| Transform/marts  | dbt (dbt-trino) |
| Serving          | Postgres (alerts/marts) + Trino |

## 3. Event Model (Kafka topics)

### `card.authorizations`
| field | notes |
|-------|-------|
| auth_id | UUID, correlation key |
| card_id | tokenized PAN |
| account_id | cardholder account |
| merchant_id | merchant |
| mcc | merchant category code |
| merchant_country | ISO country |
| amount | transaction amount |
| currency | ISO currency |
| entry_mode | chip / contactless / online / swipe |
| is_card_present | bool |
| lat, lon | merchant geo (for impossible-travel) |
| device_id | device fingerprint (CNP) |
| event_ts | event time |
| label_is_fraud | **ground-truth label for ML training only; never read by real-time rules** |

### `card.decisions`
`auth_id`, `decision` (approve/decline), `decline_reason`, `risk_score`, `event_ts`

### `card.settlements`
`auth_id`, `settled_amount`, `event_ts`

### `card.chargebacks`
`auth_id`, `dispute_reason`, `chargeback_amount`, `is_fraud_dispute`, `event_ts`

## 4. Data Flow (medallion lakehouse)

```
Synthetic generator (labeled normal + fraud patterns) -> Kafka (4 topics)
        |
   Spark Structured Streaming
     |- real-time RULE ENGINE -> approve/decline -> card.decisions + Postgres alerts
     |- ML micro-batch scoring (load persisted model) -> fraud probability
     |- Bronze landing -> Iceberg (MinIO, REST catalog)
        |
   Airflow batch: Bronze->Silver (join auth+decision+settlement+chargeback, enrich)
        |- FEATURE engineering -> train HistGradientBoosting (labeled) -> evaluate -> persist
        |- dbt Gold marts
        |
   Trino + Postgres serving
```

## 5. Fraud Detection

### 5.1 Real-time rule engine (`streaming/rules.py`, pure & unit-tested)
Operates on a per-card windowed aggregate / single-transaction context. Each rule
returns a non-negative risk contribution; the sum is thresholded to approve/decline
and a `low/medium/high` severity, written to `card.decisions` + Postgres `fraud_alerts`.

Rules:
- **Velocity** — too many authorizations per `card_id` within a sliding window.
- **Card-testing** — many rapid **small-amount** auths on a card/merchant (the
  card-testing signature: low amounts, high count, often declines).
- **Impossible-travel** — same `card_id` seen in two locations whose distance/time
  implies an impossible travel speed.
- **Amount anomaly** — amount far above the card's typical spend.
- **High-risk MCC** — known high-risk merchant categories.
- **CNP weighting** — card-not-present + new device raises risk.

### 5.2 Supervised ML
- The generator emits `label_is_fraud` ground truth across fraud patterns.
- `batch/features.py` builds per-transaction features (amount, log-amount, mcc,
  entry mode, is_card_present, hour-of-day, per-card velocity/aggregates, geo).
- `batch/model.py` trains/evaluates/persists a `HistGradientBoostingClassifier`
  (seeded). Evaluation asserts a **holdout ROC-AUC threshold** and that obvious fraud
  is caught. The model (joblib) is applied in the streaming micro-batch and a batch
  `fraud_model_scores` mart.

## 6. Analytics — Gold marts (dbt)
- **approval_monitoring** — approval rate, decline reasons, by issuer/network/time.
- **spend_by_mcc** / **spend_by_merchant** — transaction volume & value by category/merchant.
- **chargeback_rates** — dispute & fraud-chargeback rates and loss by segment.
- **risk_exposure** — declined/blocked value and high-risk corridor exposure.
- **fraud_model_scores** — model probability distribution and top-risk transactions.

## 7. Repo Layout (own repo, each unit independently testable)
```
banking-card-transactions/
  generator/      labeled synthetic txn producer (normal + fraud patterns)
  streaming/      rule engine + scoring + bronze sink + fraud job (rules + ML micro-batch)
  batch/          silver transform, feature engineering, model train/eval, model scoring
  dbt/            gold marts + tests
  airflow/        DAGs: batch ETL, model training, model scoring
  quality/        Great Expectations / data-quality checks
  infra/          docker-compose + Iceberg REST catalog + Trino catalogs
  models/         persisted model artifacts (gitignored)
  tests/          pytest for rules, features, model, silver, e2e
  docs/           spec, SCALING.md (incl. model-ops), RUNBOOK.md
```

## 8. Testing & Quality
- **pytest** for every fraud rule, feature builder, the Silver join, and **model
  training/evaluation** on a small labeled set (deterministic seed; assert holdout
  ROC-AUC threshold and that an obvious card-testing burst is caught).
- **dbt tests** on marts; **Great Expectations** on Silver.
- **End-to-end smoke**: labeled card-testing burst → declines + high fraud score;
  normal traffic → approvals.
- **Live verification** mirrors Project 1: Kafka → Spark → Iceberg/MinIO → Postgres →
  Trino + dbt marts, via a bounded `scripts/verify_cluster.py`.

## 9. "Production Blueprint" Mapping (`docs/SCALING.md`)
Local→petabyte mapping (Kafka partitioning, Spark tuning, Iceberg partition/compaction,
MinIO→S3, Postgres→warehouse, Airflow→MWAA) **plus model-ops**: training cadence,
feature store, model registry/versioning, online vs offline inference trade-offs, and
**drift monitoring (PSI)**.

## 10. Prerequisites
Docker Desktop (installed), Python 3.11, Java 21 — all available. Reuses the same
stack family as the telecom project.

## 11. Out of Scope (YAGNI)
- Real card-network/issuer integration (synthetic data only).
- PCI-DSS controls / real PAN handling (cards are tokenized synthetic ids).
- BI dashboard tooling (marts are queryable; visualization left to the consumer).
- A standalone real-time model server (model scores inside the Spark micro-batch).
- Deep hyperparameter tuning / AutoML (a single seeded, evaluated model).

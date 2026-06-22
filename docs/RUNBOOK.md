# Runbook — Banking Credit-Card Transaction Pipeline

## Prerequisites
- Docker Desktop running (`docker info` succeeds).
- venv: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`

## Start-up (ordered)
1. **Infrastructure**
   ```bash
   docker compose -f infra/docker-compose.yml up -d
   docker compose -f infra/docker-compose.yml ps
   ```
2. **Create lakehouse bucket** (attach `mc` to the compose network)
   ```bash
   NET=$(docker network ls --format '{{.Name}}' | grep -i infra | head -1)
   docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
     "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb -p local/lakehouse"
   ```
3. **Create Postgres tables**
   ```bash
   docker exec -i "$(docker compose -f infra/docker-compose.yml ps -q postgres)" \
     psql -U card -d card -c "CREATE TABLE IF NOT EXISTS fraud_alerts(
       id serial primary key, auth_id text, card_id text, rule_score double precision,
       model_prob double precision, severity text, decision text,
       created_at timestamptz default now());
     CREATE TABLE IF NOT EXISTS fraud_scores(
       auth_id text, model_prob double precision, scored_at timestamptz default now());"
   ```
4. **Bronze landing stream** (terminal 1): `python -m streaming.bronze_sink`
5. **Generator** (terminal 2): `python -m generator.producer`
6. **Build Silver + train a model** (so the fraud job can load one):
   ```bash
   python -m batch.run_silver
   python -m batch.run_train     # writes models/fraud_model.joblib
   ```
7. **Real-time fraud job** (terminal 3): `python -m streaming.fraud_job`
8. **Batch marts + scoring**: trigger Airflow DAGs `card_batch_etl`,
   `card_model_training`, `card_model_scoring`, or run directly:
   ```bash
   python -m batch.run_score
   (cd dbt && dbt deps && dbt build --profiles-dir .)
   ```
9. **Inspect**
   - Alerts: `psql -U card -d card -c "SELECT decision, count(*) FROM fraud_alerts GROUP BY 1;"`
   - Marts (Trino): `SELECT * FROM iceberg.gold.risk_exposure ORDER BY fraud_loss DESC;`
   - Kafka UI http://localhost:8085 · MinIO http://localhost:9001 · Trino http://localhost:8080

## Verification scripts
- `PYTHONPATH=. python scripts/verify_cluster.py` — bounded live check:
  Kafka → Spark → Iceberg-on-MinIO → model training → Trino (needs stack up + bucket).

## Teardown
```bash
docker compose -f infra/docker-compose.yml down -v
rm -rf /tmp/ckpt
```

## Troubleshooting
- **Kafka image/listeners:** uses `apache/kafka:3.7.0` with dual listeners (host
  `localhost:9092`, in-cluster `kafka:29092`).
- **Trino can't see Iceberg:** confirm the `iceberg-rest` service is up and the REST URI
  in `trino-iceberg.properties` / `common/spark.py` matches.
- **`fraud_job` fails to load model:** run `python -m batch.run_train` first.
- **Stream replays / stuck:** delete the checkpoint under `/tmp/ckpt` and restart.

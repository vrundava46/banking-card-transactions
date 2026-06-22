# Scaling Blueprint — Local → Petabyte Production (incl. Model-Ops)

This pipeline runs locally on docker-compose, but every component maps onto
petabyte-scale production. This document is the bridge.

## Kafka (event backbone)
- **Local:** single broker, default partitions.
- **Production:** partition each topic by `hash(card_id)` so a card's traffic stays
  ordered within a partition (important for velocity/impossible-travel state). Size
  partitions for peak TPS (≤ ~10 MB/s/partition); replication factor 3,
  `min.insync.replicas=2`; tiered storage for cold segments.

## Spark (stream + batch)
- **Local:** `local[*]`, micro-batch.
- **Production:** YARN/Kubernetes; tune `spark.sql.shuffle.partitions`, enable AQE.
  For streaming, use the **RocksDB state store** for per-card velocity/geo state and
  checkpoint to S3. Right-size executors; dynamic allocation.

## Iceberg (lakehouse tables)
- **Local:** Iceberg **REST catalog** (`tabulario/iceberg-rest`) over MinIO, so Spark
  and Trino share the same tables.
- **Production:** managed REST / Glue / Nessie catalog. Partition Bronze/Silver by
  `days(event_ts)` + `bucket(card_id)`. Schedule `rewrite_data_files` (compaction),
  `expire_snapshots`, `remove_orphan_files`.

## Storage & Serving
- MinIO → S3 (lifecycle policies, tiering). Postgres → Redshift/Snowflake/BigQuery for
  the serving warehouse. Trino as a multi-worker autoscaling cluster.

## Orchestration
- Airflow → MWAA / Cloud Composer. SLAs, retries with backoff, freshness sensors.

## Model-Ops (this pipeline's ML layer)
- **Training cadence & triggers:** retrain on schedule (`card_model_training` DAG) and
  on drift breach. Train offline on Silver; never on the hot path.
- **Train/serve parity:** promote `batch/features.py` into a **feature store** so the
  streaming micro-batch and training compute identical features.
- **Model registry/versioning:** persist each model with metrics + a version id; the
  scoring job pins a version; roll back on regression.
- **Inference placement:** rules score in real time (constant per-event cost); the model
  scores in the Spark micro-batch (near-real-time). Move to an online model server only
  if sub-100ms model latency is required.
- **Drift monitoring (PSI):** compute Population Stability Index on feature and score
  distributions vs the training baseline; alert and auto-trigger retraining on breach.
- **Imbalance & metrics:** fraud is rare — monitor PR-AUC/recall at fixed precision, not
  just ROC-AUC; use class weights / threshold tuning per cost trade-off.

## Cost controls
- Spot executors for batch; autoscaling Trino; partition pruning; compaction; tiered
  Kafka + S3 storage.

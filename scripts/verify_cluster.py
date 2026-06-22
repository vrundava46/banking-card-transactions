"""Bounded live end-to-end verification against the Docker cluster.

Kafka -> Spark -> Iceberg-on-MinIO -> Postgres -> Trino, plus model train/score.
Run (stack up): PYTHONPATH=. python scripts/verify_cluster.py
"""
import json
import sys
import time
from pyspark.sql.functions import col, from_json

from common.spark import build_spark
from common.config import Settings
from streaming.bronze_sink import AUTH_SCHEMA
from batch.features import build_features
from batch.model import train_model
from generator import scenarios

# Unique topic + table per run so verification is idempotent across invocations
# and never picks up events left in Kafka from a previous run.
RUN_ID = str(int(time.time()))
TOPIC = f"card.authorizations.verify_{RUN_ID}"
TABLE = f"lake.bronze.authorizations_verify_{RUN_ID}"


def produce(bootstrap, topic):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    auths = scenarios.normal_traffic(200) + scenarios.card_testing_burst(40)
    for a in auths:
        p.produce(topic, key=a["auth_id"], value=json.dumps(a))
    p.flush()
    return len(auths)


def main() -> int:
    s = Settings.from_env()
    n = produce(s.kafka_bootstrap, TOPIC)
    print(f"[1/4] Produced {n} authorizations to Kafka topic {TOPIC}")

    spark = build_spark("verify-cluster", s)
    spark.sparkContext.setLogLevel("ERROR")
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", s.kafka_bootstrap)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest").load())
    auth = raw.select(from_json(col("value").cast("string"),
                                AUTH_SCHEMA).alias("d")).select("d.*")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    auth.writeTo(TABLE).createOrReplace()
    cnt = spark.table(TABLE).count()
    assert cnt == n, f"expected {n}, got {cnt}"
    print(f"[2/4] Kafka -> Bronze Iceberg (MinIO) OK ({cnt} rows)")

    pdf = spark.table(TABLE).select(
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
    cur.execute(f"SELECT count(*) FROM {TABLE.replace('lake.', 'iceberg.')}")
    tcount = cur.fetchone()[0]
    assert tcount == n, f"Trino saw {tcount}, expected {n}"
    print(f"[4/4] Trino reads Iceberg {TABLE} OK ({tcount} rows)")

    # Cleanup so repeated verification runs don't accumulate tables
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.stop()
    print("\nALL LIVE CLUSTER CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

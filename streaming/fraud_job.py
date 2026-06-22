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
    import psycopg2

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

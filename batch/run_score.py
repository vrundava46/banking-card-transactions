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

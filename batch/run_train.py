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

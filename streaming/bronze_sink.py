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

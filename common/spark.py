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

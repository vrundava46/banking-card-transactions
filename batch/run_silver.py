from common.spark import build_spark
from batch.silver import build_silver


def run():  # pragma: no cover
    spark = build_spark("card-batch-silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    silver = build_silver(spark.table("lake.bronze.authorizations"),
                          spark.table("lake.bronze.decisions"),
                          spark.table("lake.bronze.settlements"),
                          spark.table("lake.bronze.chargebacks"))
    silver.writeTo("lake.silver.transactions").createOrReplace()
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    run()

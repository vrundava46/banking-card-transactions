from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def build_silver(auth: DataFrame, decisions: DataFrame, settlements: DataFrame,
                 chargebacks: DataFrame) -> DataFrame:
    a = auth.dropDuplicates(["auth_id"])
    joined = (a.join(decisions, "auth_id", "left")
               .join(settlements, "auth_id", "left")
               .join(chargebacks.select("auth_id", "chargeback_amount",
                                        "is_fraud_dispute"), "auth_id", "left"))
    return joined.withColumn(
        "is_charged_back", when(col("chargeback_amount").isNotNull(), True)
                           .otherwise(False))

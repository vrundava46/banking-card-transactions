import pytest
from pyspark.sql import SparkSession
from batch.silver import build_silver


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").getOrCreate()
    yield s
    s.stop()


def test_silver_joins_auth_decision_settlement_chargeback(spark):
    auth = spark.createDataFrame(
        [("a1", "c1", "5411", 80.0, True),
         ("a1", "c1", "5411", 80.0, True),   # dup
         ("a2", "c2", "7995", 1.0, False)],
        ["auth_id", "card_id", "mcc", "amount", "is_card_present"])
    dec = spark.createDataFrame(
        [("a1", "approve", 5.0), ("a2", "decline", 95.0)],
        ["auth_id", "decision", "risk_score"])
    settle = spark.createDataFrame([("a1", 80.0)], ["auth_id", "settled_amount"])
    cb = spark.createDataFrame([("a2", 1.0, True)],
                               ["auth_id", "chargeback_amount", "is_fraud_dispute"])
    out = build_silver(auth, dec, settle, cb).orderBy("auth_id").collect()
    assert len(out) == 2
    a2 = [r for r in out if r["auth_id"] == "a2"][0]
    assert a2["decision"] == "decline"
    assert a2["is_charged_back"] is True
    a1 = [r for r in out if r["auth_id"] == "a1"][0]
    assert a1["settled_amount"] == 80.0
    assert a1["is_charged_back"] is False

import json
import pandas as pd
from generator.producer import emit_batch
from batch.features import build_features
from batch.model import train_model, score_frame
from streaming.fraud_job import apply_scoring


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def _auths(sent):
    return [json.loads(v) for (t, _, v) in sent if t == "card.authorizations"]


def test_card_testing_burst_is_declined_and_high_model_prob():
    fake = FakeProducer()
    for _ in range(6):
        emit_batch(fake, n_normal=120, fraud=False)
        emit_batch(fake, n_normal=0, fraud=True)
    auths = _auths(fake.sent)
    df = pd.DataFrame(auths)
    feats = build_features(df)
    model, metrics = train_model(feats, seed=42)
    assert metrics["roc_auc"] > 0.8

    # build streaming context per card and score
    df["card_auth_count"] = df.groupby("card_id")["auth_id"].transform("size")
    df["card_small_amt_count"] = (df.assign(s=(df["amount"] <= 5).astype(int))
                                  .groupby("card_id")["s"].transform("sum"))
    df["card_avg_amount"] = df.groupby("card_id")["amount"].transform("mean")
    df["distance_km"] = 0.0
    df["hours_since_prev"] = 1e9
    df["device_is_new"] = ~df["is_card_present"]
    probs = score_frame(model, build_features(df))
    rows = df.to_dict("records")
    scored = apply_scoring(rows, lambda r: probs[rows.index(r)])

    fraud_rows = [s for s in scored if s["label_is_fraud"]]
    legit_rows = [s for s in scored if not s["label_is_fraud"]]
    # most labeled card-testing fraud is declined by rules
    assert sum(s["decision"] == "decline" for s in fraud_rows) > len(fraud_rows) * 0.5
    # legit traffic is overwhelmingly approved
    assert sum(s["decision"] == "approve" for s in legit_rows) > len(legit_rows) * 0.9
